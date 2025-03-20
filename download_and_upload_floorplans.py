import pandas as pd
import requests
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
import os
import pickle
import time
import aiohttp
import asyncio
from io import BytesIO
import aiofiles

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def get_google_drive_service():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)

async def download_image_async(session, url, filename):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            async with aiofiles.open(filename, 'wb') as f:
                await f.write(await response.read())
        return True
    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")
        return False

async def download_all_images(urls_and_filenames):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url, filename in urls_and_filenames:
            task = asyncio.create_task(download_image_async(session, url, filename))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        return sum(1 for result in results if result)

async def upload_to_drive_async(service, file_path, folder_id):
    try:
        async with aiofiles.open(file_path, 'rb') as f:
            file_data = await f.read()
        
        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [folder_id]
        }
        
        media = MediaIoBaseUpload(
            BytesIO(file_data),
            mimetype='image/jpeg',
            resumable=True
        )
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        return file.get('id')
    except Exception as e:
        print(f"Error uploading {file_path}: {str(e)}")
        return None

async def upload_all_files(service, folder_id, temp_dir):
    tasks = []
    for filename in os.listdir(temp_dir):
        file_path = os.path.join(temp_dir, filename)
        task = asyncio.create_task(upload_to_drive_async(service, file_path, folder_id))
        tasks.append((file_path, task))
    
    successful_uploads = 0
    for file_path, task in tasks:
        file_id = await task
        if file_id:
            successful_uploads += 1
        else:
            print(f"Failed to upload {file_path}")
    
    return successful_uploads

def main():
    start_time = time.time()
    BATCH_SIZE = 100
    
    # Track failed operations
    failed_downloads = []
    failed_uploads = []
    
    # Create a temporary directory for downloads
    if not os.path.exists('temp_images'):
        os.makedirs('temp_images')

    # Initialize Google Drive service
    service = get_google_drive_service()

    # Read the CSV file
    df = pd.read_csv('bayut_floor_plans_sample.csv')

    # Replace this with your Google Drive folder ID
    FOLDER_ID = '1irEISHn_jzSgu8AdCRwl8oGHpcNgRZIs'

    # Prepare all URLs and filenames
    all_files = []
    for _, row in df.iterrows():
        url_list = eval(row['image2D_url'])
        # Clean the title to make it filesystem-friendly
        title = row['title'].replace(' - ', '_').replace(' ', '_')
        # Replace all invalid filename characters
        title = title.replace('/', '_').replace('\\', '_').replace(':', '_').replace('?', '_').replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_')
        external_id = row['url'].split('details-')[-1].split('.')[0]
        
        for i, url in enumerate(url_list):
            filename = f"temp_images/{title}_{external_id}_{i+1}{os.path.splitext(url)[1]}"
            all_files.append((url, filename))

    total_images = len(all_files)
    successful_count = 0
    
    print(f"Starting processing of {total_images} images in batches of {BATCH_SIZE}...")
    
    # Process in batches
    for i in range(0, total_images, BATCH_SIZE):
        batch_start_time = time.time()
        batch = all_files[i:i + BATCH_SIZE]
        print(f"\nProcessing batch {(i//BATCH_SIZE) + 1}/{(total_images + BATCH_SIZE - 1)//BATCH_SIZE}")
        
        # Download batch
        download_start = time.time()
        print(f"Downloading {len(batch)} images...")
        downloaded = asyncio.run(download_all_images(batch))
        download_time = time.time() - download_start
        
        # Track failed downloads
        for url, filename in batch:
            if not os.path.exists(filename):
                failed_downloads.append((url, filename))
        
        if downloaded > 0:
            # Upload batch
            upload_start = time.time()
            print(f"Uploading {downloaded} images...")
            uploaded = asyncio.run(upload_all_files(service, FOLDER_ID, 'temp_images'))
            upload_time = time.time() - upload_start
            successful_count += uploaded
            
            # Track failed uploads
            for _, filename in batch:
                if os.path.exists(filename):
                    failed_uploads.append((filename, FOLDER_ID))
            
            # Delete uploaded files
            print("Cleaning up temporary files...")
            for _, filename in batch:
                try:
                    if os.path.exists(filename):
                        os.remove(filename)
                except Exception as e:
                    print(f"Error deleting {filename}: {str(e)}")
        
        batch_time = time.time() - batch_start_time
        print(f"Batch completed in {batch_time:.2f}s (Download: {download_time:.2f}s, Upload: {upload_time:.2f}s)")
        print(f"Progress: {successful_count}/{total_images} images processed")

    # Clean up the temporary directory
    try:
        os.rmdir('temp_images')
    except Exception as e:
        print(f"Error removing temporary directory: {str(e)}")

    # Print failed operations summary
    print("\n=== Failed Operations Summary ===")
    if failed_downloads:
        print(f"\nFailed Downloads ({len(failed_downloads)}):")
        for url, filename in failed_downloads:
            print(f"URL: {url}")
            print(f"Intended filename: {filename}")
            print("-" * 50)
    
    if failed_uploads:
        print(f"\nFailed Uploads ({len(failed_uploads)}):")
        for filename, folder_id in failed_uploads:
            print(f"File: {filename}")
            print(f"Target folder: {folder_id}")
            print("-" * 50)

    # Calculate and print the total execution time
    total_time = time.time() - start_time
    print(f"\nTotal time for processing: {total_time:.2f} seconds")
    print(f"Successfully processed {successful_count} out of {total_images} images")

if __name__ == '__main__':
    main()