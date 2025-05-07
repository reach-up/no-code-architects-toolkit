# Copyright (c) 2025 Stephen G. Pope / AI Assistant
# Based on NCA Toolkit structure
# Licensed under GPL-2.0
# MODIFIED TO DECODE GCP_SA_CREDENTIALS FROM BASE64

import os
import logging
import json
import base64 # <-- Added for decoding
from google.cloud import storage
from google.oauth2 import service_account
import config # Assuming config.py loads GCP_SA_CREDENTIALS and GCP_BUCKET_NAME

# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Cache the client
_gcs_client = None

def _get_gcs_client():
    """Initializes and returns the GCS client using credentials."""
    global _gcs_client
    if _gcs_client:
        return _gcs_client

    try:
        # --- Decode Base64 Credentials ---
        encoded_creds = getattr(config, 'GCP_SA_CREDENTIALS', None)
        gcp_bucket_name = getattr(config, 'GCP_BUCKET_NAME', None)

        if not encoded_creds:
            # If no GCP creds, maybe we don't need GCS? Log a warning.
            logger.warning("GCP_SA_CREDENTIALS environment variable is not set. GCS client cannot be initialized.")
            return None # Return None if GCS is optional based on env vars
            # OR raise ValueError("GCP_SA_CREDENTIALS environment variable is not set.") # if GCS is mandatory

        if not gcp_bucket_name:
             logger.warning("GCP_BUCKET_NAME environment variable is not set. GCS operations will likely fail.")
             # Depending on usage, might want to return None or raise error

        logger.info("Decoding Base64 credentials from GCP_SA_CREDENTIALS for GCS...")
        try:
            decoded_creds_bytes = base64.b64decode(encoded_creds)
            decoded_creds_str = decoded_creds_bytes.decode('utf-8')
            creds_json = json.loads(decoded_creds_str)

            credentials = service_account.Credentials.from_service_account_info(creds_json)
            logger.info("Successfully loaded GCS credentials from Base64 encoded JSON.")

        except (base64.binascii.Error, json.JSONDecodeError, ValueError, TypeError) as e:
             logger.error(f"Failed to decode/parse Base64 credentials for GCS: {e}", exc_info=True)
             raise ValueError(f"Could not load GCP credentials from Base64 for GCS: {e}")
        # --- End Decode Base64 ---

        _gcs_client = storage.Client(credentials=credentials)
        logger.info("Google Cloud Storage client initialized successfully.")
        return _gcs_client

    except Exception as e:
        logger.error(f"Failed to initialize GCS client: {e}", exc_info=True)
        # Depending on requirements, either return None or raise the error
        # raise ConnectionError(f"Could not initialize GCS client: {e}")
        return None


def upload_to_gcs(source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    client = _get_gcs_client()
    if not client:
        raise ConnectionError("GCS client not available. Check credentials configuration.")

    bucket_name = getattr(config, 'GCP_BUCKET_NAME', None)
    if not bucket_name:
         raise ValueError("GCP_BUCKET_NAME is not configured.")

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        logger.info(f"Uploading {source_file_name} to gs://{bucket_name}/{destination_blob_name}...")
        blob.upload_from_filename(source_file_name)
        logger.info(f"File {source_file_name} uploaded to {destination_blob_name}.")
        # Return public URL (consider security implications) or just blob name/path
        # return blob.public_url
        return f"gs://{bucket_name}/{destination_blob_name}" # Return GCS URI

    except Exception as e:
        logger.error(f"Failed to upload {source_file_name} to GCS: {e}", exc_info=True)
        raise ConnectionError(f"Failed to upload to GCS: {e}")


def download_from_gcs(source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    client = _get_gcs_client()
    if not client:
        raise ConnectionError("GCS client not available. Check credentials configuration.")

    bucket_name = getattr(config, 'GCP_BUCKET_NAME', None)
    if not bucket_name:
         raise ValueError("GCP_BUCKET_NAME is not configured.")

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        logger.info(f"Downloading gs://{bucket_name}/{source_blob_name} to {destination_file_name}...")
        blob.download_to_filename(destination_file_name)
        logger.info(f"Blob {source_blob_name} downloaded to {destination_file_name}.")
        return True

    except Exception as e:
        logger.error(f"Failed to download {source_blob_name} from GCS: {e}", exc_info=True)
        raise ConnectionError(f"Failed to download from GCS: {e}")

# Add other GCS utility functions as needed (e.g., delete_blob, list_blobs)

