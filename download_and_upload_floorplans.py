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
import zipfile
import shutil

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

def create_zip_file(temp_dir, zip_filename='floorplans_images.zip'):
    """Create a zip file from all images in the temporary directory."""
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, temp_dir)
                zipf.write(file_path, arcname)
    return zip_filename

def upload_zip_to_drive(service, zip_file, folder_id):
    """Upload the zip file to Google Drive."""
    try:
        file_metadata = {
            'name': os.path.basename(zip_file),
            'parents': [folder_id],
            'mimeType': 'application/zip'
        }
        
        media = MediaFileUpload(
            zip_file,
            mimetype='application/zip',
            resumable=True
        )
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        return file.get('id')
    except Exception as e:
        print(f"Error uploading zip file: {str(e)}")
        return None

def main():
    start_time = time.time()
    BATCH_SIZE = 1000
    
    # Create a temporary directory for downloads
    temp_dir = 'temp_images'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # Initialize Google Drive service
    service = get_google_drive_service()

    # Read the CSV file
    df = pd.read_csv('bayut_floor_plans.csv')

    # Replace this with your Google Drive folder ID
    FOLDER_ID = '1R-KkCK51NhDRzCLwaTsm8en6_2N2gOlt'

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
    print(f"Starting download of {total_images} images...")
    
    # Download all images
    downloaded = asyncio.run(download_all_images(all_files))
    print(f"Successfully downloaded {downloaded} images")

    if downloaded > 0:
        # Create zip file
        print("Creating zip file...")
        zip_filename = create_zip_file(temp_dir)
        
        # Upload zip file
        print("Uploading zip file to Google Drive...")
        zip_file_id = upload_zip_to_drive(service, zip_filename, FOLDER_ID)
        
        if zip_file_id:
            print(f"Successfully uploaded zip file. File ID: {zip_file_id}")
        else:
            print("Failed to upload zip file")

    # Clean up
    print("Cleaning up temporary files...")
    if os.path.exists(zip_filename):
        os.remove(zip_filename)
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    # Calculate and print the total execution time
    total_time = time.time() - start_time
    print(f"\nTotal time for processing: {total_time:.2f} seconds")
    print(f"Successfully downloaded {downloaded} out of {total_images} images")

if __name__ == '__main__':
    main()