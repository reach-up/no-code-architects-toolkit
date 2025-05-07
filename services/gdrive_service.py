# Copyright (c) 2025 Stephen G. Pope, modifications by [Your Name/AI]
# Based on NCA Toolkit structure
# Licensed under GPL-2.0

import os
import io
import logging
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
import config # Assuming config.py loads GCP_SA_CREDENTIALS

# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Define necessary Drive API scopes
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.file'] # Scope for creating/managing files created by the app

def _get_drive_service():
    """Creates and returns an authenticated Google Drive service client."""
    try:
        # Load credentials from the environment variable
        # The variable might contain the JSON directly or a path to the file.
        # The library handles both cases if configured correctly.
        try:
            creds_json = json.loads(config.GCP_SA_CREDENTIALS)
            credentials = service_account.Credentials.from_service_account_info(
                creds_json, scopes=DRIVE_SCOPES
            )
        except json.JSONDecodeError:
             # Assume it's a file path if JSON parsing fails
             if not os.path.exists(config.GCP_SA_CREDENTIALS):
                 logger.error(f"GCP Service Account file not found at: {config.GCP_SA_CREDENTIALS}")
                 raise FileNotFoundError("GCP Service Account file not found.")
             credentials = service_account.Credentials.from_service_account_file(
                config.GCP_SA_CREDENTIALS, scopes=DRIVE_SCOPES
             )
        except Exception as e:
             logger.error(f"Error loading GCP Service Account credentials: {e}")
             raise ValueError(f"Could not load GCP credentials: {e}")


        if not credentials or not credentials.valid:
            logger.warning("Google Drive service account credentials are not valid.")

        service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
        logger.info("Google Drive service client created successfully.")
        return service
    except Exception as e:
        logger.error(f"Failed to initialize Google Drive service: {e}", exc_info=True)
        raise ConnectionError(f"Could not initialize Google Drive service: {e}")

def download_gdrive_file(file_id, local_save_path):
    """Downloads a file from Google Drive by ID to a local path."""
    service = _get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(local_save_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    try:
        logger.info(f"Attempting to download GDrive file ID: {file_id} to {local_save_path}")
        while done is False:
            status, done = downloader.next_chunk()
            if status:
                 logger.debug(f"GDrive Download {file_id}: {int(status.progress() * 100)}%.")
        logger.info(f"Successfully downloaded GDrive file ID: {file_id} to {local_save_path}")
        return True
    except HttpError as error:
        logger.error(f"Google Drive API error downloading file ID {file_id}: {error}", exc_info=True)
        if os.path.exists(local_save_path):
            os.remove(local_save_path)
        raise ConnectionError(f"Failed to download file ID {file_id}: {error}")
    except Exception as e:
        logger.error(f"An unexpected error occurred downloading file ID {file_id}: {e}", exc_info=True)
        if os.path.exists(local_save_path):
            os.remove(local_save_path)
        raise
    finally:
        fh.close()

def upload_file_to_gdrive(local_file_path, target_folder_id, output_filename):
    """Uploads a local file to a specified Google Drive folder."""
    service = _get_drive_service()
    file_metadata = {
        'name': output_filename,
        'parents': [target_folder_id]
    }
    media = MediaFileUpload(local_file_path, mimetype='video/mp4', resumable=True)
    try:
        logger.info(f"Attempting to upload {local_file_path} to GDrive folder ID: {target_folder_id} as {output_filename}")
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink' # Request fields needed in the response
        ).execute()
        logger.info(f"Successfully uploaded file: ID {file.get('id')}, Name: {file.get('name')}")
        return file
    except HttpError as error:
        logger.error(f"Google Drive API error uploading file {output_filename}: {error}", exc_info=True)
        raise ConnectionError(f"Failed to upload file {output_filename}: {error}")
    except Exception as e:
        logger.error(f"An unexpected error occurred uploading file {output_filename}: {e}", exc_info=True)
        raise
