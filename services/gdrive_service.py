# Copyright (c) 2025 Stephen G. Pope / AI Assistant
# Based on NCA Toolkit structure
# Licensed under GPL-2.0
# MODIFIED TO DECODE GCP_SA_CREDENTIALS FROM BASE64

import os
import io
import logging
import json
import base64 # <-- Added for decoding
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
import config

# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO) # Assuming basic config ok here

# Define necessary Drive API scopes
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.file']

def _get_drive_service():
    """Creates and returns an authenticated Google Drive service client."""
    try:
        # --- Decode Base64 Credentials ---
        encoded_creds = getattr(config, 'GCP_SA_CREDENTIALS', None)
        if not encoded_creds:
             logger.error("GCP_SA_CREDENTIALS environment variable is not set.")
             raise ValueError("GCP_SA_CREDENTIALS environment variable is not set.")

        logger.info("Decoding Base64 credentials from GCP_SA_CREDENTIALS for Drive...")
        try:
            decoded_creds_bytes = base64.b64decode(encoded_creds)
            decoded_creds_str = decoded_creds_bytes.decode('utf-8')
            creds_json = json.loads(decoded_creds_str) # Parse the decoded JSON string

            credentials = service_account.Credentials.from_service_account_info(
                creds_json, scopes=DRIVE_SCOPES
            )
            logger.info("Successfully loaded Drive credentials from Base64 encoded JSON.")

        except (base64.binascii.Error, json.JSONDecodeError, ValueError, TypeError) as e:
             logger.error(f"Failed to decode/parse Base64 credentials for Drive: {e}", exc_info=True)
             raise ValueError(f"Could not load GCP credentials from Base64 for Drive: {e}")
        # --- End Decode Base64 ---

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
    # Use io.BytesIO for in-memory handling if needed, or FileIO for direct saving
    # Using FileIO as before for direct saving:
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
            try: os.remove(local_save_path)
            except OSError: pass
        raise ConnectionError(f"Failed to download file ID {file_id}: {error}")
    except Exception as e:
        logger.error(f"An unexpected error occurred downloading file ID {file_id}: {e}", exc_info=True)
        if os.path.exists(local_save_path):
            try: os.remove(local_save_path)
            except OSError: pass
        raise
    finally:
        # Ensure file handle is closed even on error, ignore errors during close
        try: fh.close()
        except OSError: pass

def upload_file_to_gdrive(local_file_path, target_folder_id, output_filename):
    """Uploads a local file to a specified Google Drive folder."""
    service = _get_drive_service()
    file_metadata = {
        'name': output_filename,
        'parents': [target_folder_id]
    }
    media = MediaFileUpload(local_file_path, mimetype='video/mp4', resumable=True) # Adjust mimetype if needed
    try:
        logger.info(f"Attempting to upload {local_file_path} to GDrive folder ID: {target_folder_id} as {output_filename}")
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink' # Request fields needed in the response
        ).execute()
        logger.info(f"Successfully uploaded file: ID {file.get('id')}, Name: {file.get('name')}")
        return file # Return the GDrive file object (contains id, name, webViewLink etc.)
    except HttpError as error:
        logger.error(f"Google Drive API error uploading file {output_filename}: {error}", exc_info=True)
        raise ConnectionError(f"Failed to upload file {output_filename}: {error}")
    except Exception as e:
        logger.error(f"An unexpected error occurred uploading file {output_filename}: {e}", exc_info=True)
        raise
