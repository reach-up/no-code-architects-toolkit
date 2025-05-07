import logging
import boto3
from botocore.exceptions import ClientError
import config
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

_s3_client = None

def _get_s3_client():
    global _s3_client
    if _s3_client:
        return _s3_client

    access_key = getattr(config, 'S3_ACCESS_KEY', None)
    secret_key = getattr(config, 'S3_SECRET_KEY', None)
    region = getattr(config, 'S3_REGION', None)
    endpoint_url = getattr(config, 'S3_ENDPOINT_URL', None) # Required for non-AWS S3, optional for AWS

    if not access_key or not secret_key:
        logger.warning("S3 Access Key or Secret Key not configured. S3 client cannot be initialized.")
        return None

    try:
        session = boto3.session.Session()
        _s3_client = session.client(
            's3',
            region_name=region if region else None,
            endpoint_url=endpoint_url if endpoint_url else None,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        logger.info("Boto3 S3 client initialized.")
        return _s3_client
    except Exception as e:
        logger.error(f"Failed to initialize Boto3 S3 client: {e}", exc_info=True)
        return None

def download_file_from_s3(bucket_name, object_key, local_file_path):
    s3_client = _get_s3_client()
    if not s3_client:
        raise ConnectionError("S3 client not available. Check credentials.")
    if not bucket_name:
         raise ValueError("S3 bucket name is required for download.")

    logger.info(f"Attempting to download s3://{bucket_name}/{object_key} to {local_file_path}")
    try:
        s3_client.download_file(bucket_name, object_key, local_file_path)
        logger.info(f"Successfully downloaded {object_key} from bucket {bucket_name}.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.error(f"S3 object not found: s3://{bucket_name}/{object_key}")
            raise FileNotFoundError(f"S3 object not found: s3://{bucket_name}/{object_key}")
        elif e.response['Error']['Code'] == '403':
             logger.error(f"S3 permission denied for: s3://{bucket_name}/{object_key}")
             raise PermissionError(f"S3 permission denied for: s3://{bucket_name}/{object_key}")
        else:
            logger.error(f"S3 ClientError downloading {object_key}: {e}", exc_info=True)
            raise ConnectionError(f"S3 ClientError downloading {object_key}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error downloading {object_key} from S3: {e}", exc_info=True)
        raise ConnectionError(f"Unexpected error downloading {object_key} from S3: {e}")


def upload_file_to_s3(local_file_path, bucket_name, object_key, content_type='video/mp4'):
    s3_client = _get_s3_client()
    if not s3_client:
        raise ConnectionError("S3 client not available. Check credentials.")
    if not bucket_name:
         raise ValueError("S3 bucket name is required for upload.")

    extra_args = {'ContentType': content_type}
    # You can add other args like ACL here if needed, e.g., 'ACL': 'public-read'
    # extra_args['ACL'] = 'private' # Default is private

    logger.info(f"Attempting to upload {local_file_path} to s3://{bucket_name}/{object_key}")
    try:
        s3_client.upload_file(local_file_path, bucket_name, object_key, ExtraArgs=extra_args)
        logger.info(f"Successfully uploaded {local_file_path} to s3://{bucket_name}/{object_key}")
        # Construct URL (might vary based on region/endpoint)
        # Standard AWS S3 URL format:
        region = getattr(config, 'S3_REGION', 'us-east-1') # Default if not set
        endpoint_url = getattr(config, 'S3_ENDPOINT_URL', f"https://{bucket_name}.s3.{region}.amazonaws.com")
        if f"{bucket_name}.s3" in endpoint_url: # Handle virtual hosted-style endpoints
             file_url = f"{endpoint_url}/{object_key}"
        else: # Handle path-style or regional endpoints (adjust if needed)
             file_url = f"{endpoint_url}/{bucket_name}/{object_key}"

        return {
            "bucket": bucket_name,
            "object_key": object_key,
            "url": file_url # Provide a best-guess URL
        }
    except ClientError as e:
        logger.error(f"S3 ClientError uploading {object_key}: {e}", exc_info=True)
        raise ConnectionError(f"S3 ClientError uploading {object_key}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error uploading {object_key} to S3: {e}", exc_info=True)
        raise ConnectionError(f"Unexpected error uploading {object_key} to S3: {e}")
