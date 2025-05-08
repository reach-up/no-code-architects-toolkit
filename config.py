# Copyright (c) 2025 Stephen G. Pope
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.


import os
import logging

# Setup simple logging for config loading issues if needed
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__) # Uncomment if you want specific logging

# --- Environment Variable Loading ---

print("--- Loading Configuration from Environment Variables ---")

# Mandatory API Key
API_KEY = os.environ.get('API_KEY')
if not API_KEY:
    print("❌ FATAL ENV ERROR: API_KEY environment variable is not set.")
    # Consider raising an error immediately if it's absolutely essential for startup
    raise ValueError("API_KEY environment variable is not set")
else:
    # Avoid logging the key itself unless debugging needed locally
    print(f"✅ ENV OK: API_KEY is present.")
    # print(f"API_KEY = {API_KEY}") # Example if you need to see it during debug

# --- S3 Configuration ---
# Assign S3 variables so they can be imported by other modules
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
S3_REGION = os.environ.get('S3_REGION') # Optional for some S3-compatible services
S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL') # Optional for AWS S3, needed for others

# Optional: Check if essential S3 vars are set if S3 functionality might be used
# This helps catch configuration errors early. Adjust checks based on requirements.
# For example, if S3 is optional, these might just be warnings.
if S3_ACCESS_KEY and S3_SECRET_KEY and S3_BUCKET_NAME:
     print(f"✅ ENV OK: S3 configuration variables (Key, Secret, Bucket) detected.")
     # print(f"S3_BUCKET_NAME = {S3_BUCKET_NAME}")
     # print(f"S3_REGION = {S3_REGION}")
     # print(f"S3_ENDPOINT_URL = {S3_ENDPOINT_URL}")
     # print(f"S3_ACCESS_KEY = {'*' * (len(S3_ACCESS_KEY)-4) + S3_ACCESS_KEY[-4:] if S3_ACCESS_KEY else None}") # Mask key
else:
     # Decide if this should be a warning or prevent startup if S3 is mandatory
     print(f"⚠️ ENV WARNING: One or more core S3 variables (S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET_NAME) are missing. S3 features may fail.")

# --- GCP Configuration ---
GCP_SA_CREDENTIALS = os.environ.get('GCP_SA_CREDENTIALS', '')
GCP_BUCKET_NAME = os.environ.get('GCP_BUCKET_NAME', '') # For Google Cloud Storage

if GCP_SA_CREDENTIALS:
     print(f"✅ ENV OK: GCP_SA_CREDENTIALS detected.")
     # print(f"GCP_SA_CREDENTIALS = {GCP_SA_CREDENTIALS[:20]}...") # Log snippet, careful with secrets
else:
      print(f"⚠️ ENV INFO: GCP_SA_CREDENTIALS not set. GCP/GDrive features will not work.")
if GCP_BUCKET_NAME:
    print(f"✅ ENV OK: GCP_BUCKET_NAME = {GCP_BUCKET_NAME}")
else:
    print(f"⚠️ ENV INFO: GCP_BUCKET_NAME not set. Google Cloud Storage features may not work.")


# --- Other Configuration ---
# Storage path setting
LOCAL_STORAGE_PATH = os.environ.get('LOCAL_STORAGE_PATH', '/tmp')
print(f"✅ ENV OK: LOCAL_STORAGE_PATH = {LOCAL_STORAGE_PATH}")

print("--- Configuration Loading Complete ---")


# Optional function to validate storage provider-specific vars if needed later
# (Keeping the function definition but it's not actively used by the S3/GCP loading above)
def validate_env_vars(provider):
    """ Validate the necessary environment variables for the selected storage provider """
    required_vars = {
        'GCP': ['GCP_BUCKET_NAME', 'GCP_SA_CREDENTIALS'],
        'S3': ['S3_ACCESS_KEY', 'S3_SECRET_KEY', 'S3_BUCKET_NAME'], # Removed endpoint/region as they can be optional
        # 'S3_DO': ['S3_ENDPOINT_URL', 'S3_ACCESS_KEY', 'S3_SECRET_KEY'] # Example for DigitalOcean Spaces
    }
    vars_to_check = required_vars.get(provider, [])
    if not vars_to_check:
        print(f"No specific variable validation defined for provider: {provider}")
        return True

    missing_vars = [var for var in vars_to_check if not globals().get(var)] # Check module globals

    if missing_vars:
        # Log or raise error - depends on how strictly you want to enforce this
        error_msg = f"Missing environment variables for {provider} storage: {', '.join(missing_vars)}"
        print(f"❌ CONFIG ERROR: {error_msg}")
        # raise ValueError(error_msg) # Uncomment to make validation mandatory
        return False
    else:
         print(f"✅ CONFIG OK: Required variables for provider '{provider}' seem present.")
         return True