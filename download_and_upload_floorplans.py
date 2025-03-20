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
            print(f"Successfully uploaded {file_path} to Drive")
            successful_uploads += 1
        else:
            print(f"Failed to upload {file_path}")
    
    return successful_uploads

def main():
    start_time = time.time()
    
    # Create a temporary directory for downloads
    if not os.path.exists('temp_images'):
        os.makedirs('temp_images')

    # Initialize Google Drive service
    service = get_google_drive_service()

    # Read the CSV file
    df = pd.read_csv('bayut_floor_plans_sample.csv')

    # Replace this with your Google Drive folder ID
    FOLDER_ID = '1VYAd9zGRs13zWAIPcvboF8GXl1d7tVHY'

    # Prepare URLs and filenames and process one at a time
    successful_count = 0
    total_images = sum(len(eval(row['image2D_url'])) for _, row in df.iterrows())
    
    print(f"Starting processing of {total_images} images...")
    
    for _, row in df.iterrows():
        url_list = eval(row['image2D_url'])
        title = row['title'].replace(' - ', '_').replace(' ', '_')
        external_id = row['url'].split('details-')[-1].split('.')[0]
        
        for i, url in enumerate(url_list):
            filename = f"temp_images/{title}_{external_id}_{i+1}{os.path.splitext(url)[1]}"
            
            # Download single image
            if asyncio.run(download_all_images([(url, filename)])):
                # Upload the image
                if asyncio.run(upload_all_files(service, FOLDER_ID, 'temp_images')):
                    successful_count += 1
                
                # Delete the temporary file
                try:
                    os.remove(filename)
                except Exception as e:
                    print(f"Error deleting {filename}: {str(e)}")
            
            print(f"Processed {successful_count}/{total_images} images")

    # Clean up the temporary directory
    try:
        os.rmdir('temp_images')
    except Exception as e:
        print(f"Error removing temporary directory: {str(e)}")

    # Calculate and print the total execution time
    total_time = time.time() - start_time
    print(f"Total time for processing: {total_time:.2f} seconds")
    print(f"Successfully processed {successful_count} out of {total_images} images")

if __name__ == '__main__':
    main()
