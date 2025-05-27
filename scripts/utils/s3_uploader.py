import boto3
import os
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv
import glob

load_dotenv() # Load environment variables from .env file

# Environment variables for R2 configuration
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL") # e.g., https://<accountid>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME") # Default bucket if not passed to function

def _get_s3_client():
    """Helper function to create and return an S3 client for R2."""
    if not all([R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        print("Error: R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, and R2_SECRET_ACCESS_KEY environment variables must be set.")
        return None
    
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto' # Cloudflare R2 specific
    )

def upload_file_to_r2(local_file_path: str, object_name: str, bucket_name: str = None) -> bool:
    """
    Uploads a single file to Cloudflare R2 or an S3-compatible storage.
    (Retains original single file upload logic for flexibility)
    """
    s3_client = _get_s3_client()
    if not s3_client:
        return False

    target_bucket = bucket_name or R2_BUCKET_NAME
    if not target_bucket:
        print("Error: R2 bucket name not provided and R2_BUCKET_NAME environment variable is not set.")
        return False

    if not os.path.exists(local_file_path):
        print(f"Error: Local file {local_file_path} not found.")
        return False

    print(f"Uploading {local_file_path} to R2 bucket '{target_bucket}' as '{object_name}'...")
    try:
        s3_client.upload_file(local_file_path, target_bucket, object_name)
        print(f"Successfully uploaded {local_file_path} to {target_bucket}/{object_name}")
        return True
    except FileNotFoundError:
        print(f"Error: The file {local_file_path} was not found for upload.")
    except NoCredentialsError:
        print("Error: Credentials not available for R2 upload.")
    except PartialCredentialsError:
        print("Error: Incomplete credentials for R2 upload.")
    except ClientError as e:
        print(f"ClientError during R2 upload: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during R2 upload: {e}")
    
    return False

def sync_directory_to_r2(local_directory: str, r2_prefix: str, bucket_name: str = None) -> bool:
    """
    Uploads all files from a local directory to a specified prefix in Cloudflare R2.
    This is not a differential sync; it uploads all local files found.
    It will overwrite files in R2 if they have the same object name.

    Args:
        local_directory (str): Path to the local directory to sync from.
        r2_prefix (str): The prefix in the R2 bucket where files will be uploaded.
                         e.g., "raw/isrctn/". Must end with a '/'.
        bucket_name (str): R2 bucket name. If None, uses R2_BUCKET_NAME from env vars.

    Returns:
        bool: True if all attempted uploads were successful (or no files to upload), False if any upload failed.
    """
    s3_client = _get_s3_client()
    if not s3_client:
        return False

    target_bucket = bucket_name or R2_BUCKET_NAME
    if not target_bucket:
        print("Error: R2 bucket name for sync not provided and R2_BUCKET_NAME environment variable is not set.")
        return False

    if not os.path.isdir(local_directory):
        print(f"Error: Local directory {local_directory} not found or is not a directory.")
        return False

    if not r2_prefix.endswith('/'):
        r2_prefix += '/'
        print(f"Warning: r2_prefix did not end with '/'. Appended '/': {r2_prefix}")

    all_successful = True
    files_uploaded_count = 0

    print(f"Starting sync of local directory '{local_directory}' to R2 bucket '{target_bucket}' prefix '{r2_prefix}'...")

    for root, _, files in os.walk(local_directory):
        for filename in files:
            local_file_path = os.path.join(root, filename)
            # Create the relative path from the local_directory base to maintain structure in R2
            relative_path = os.path.relpath(local_file_path, local_directory)
            object_name = os.path.join(r2_prefix, relative_path).replace("\\", "/") # Ensure forward slashes for S3 keys
            
            # Re-using the single file upload logic
            success = upload_file_to_r2(local_file_path, object_name, target_bucket)
            if success:
                files_uploaded_count += 1
            else:
                all_successful = False
                print(f"Failed to upload {local_file_path}. Sync will be marked as partially failed.")
                # Optionally, one could decide to stop the whole sync on first error

    if files_uploaded_count == 0 and all_successful:
        print(f"No files found in {local_directory} to sync, or all uploads failed before starting.")
    elif all_successful:
        print(f"Successfully synced {files_uploaded_count} files from {local_directory} to {target_bucket}/{r2_prefix}")
    else:
        print(f"Sync from {local_directory} completed with one or more errors. Uploaded {files_uploaded_count} files successfully.")
        
    return all_successful


if __name__ == "__main__":
    # Example Usage for single file upload (as before)
    # print("--- Testing single file upload ---")
    # dummy_file = "test_upload.txt"
    # with open(dummy_file, "w") as f:
    #     f.write("This is a test file for R2 upload.")
    # if R2_BUCKET_NAME:
    #     upload_file_to_r2(dummy_file, f"test_uploads/single/{dummy_file}")
    # else:
    #     print("Skipping single file upload test: R2_BUCKET_NAME environment variable not set.")
    # if os.path.exists(dummy_file):
    #     os.remove(dummy_file)
    # print("----------------------------------\n")

    # Example Usage for directory sync
    print("--- Syncing directory to R2 ---")
    sync_dir = "data/raw"
    # os.makedirs(os.path.join(test_sync_dir, "subdir"), exist_ok=True)
    # with open(os.path.join(test_sync_dir, "file1.txt"), "w") as f:
    #     f.write("Test file 1 for sync.")
    # with open(os.path.join(test_sync_dir, "subdir", "file2.txt"), "w") as f:
    #     f.write("Test file 2 for sync in subdir.")

    if R2_BUCKET_NAME:
        print(f"Attempting to sync directory '{sync_dir}' to R2 bucket '{R2_BUCKET_NAME}' at prefix 'test_sync_uploads/'...")
        sync_success = sync_directory_to_r2(sync_dir, "test_sync_uploads/", R2_BUCKET_NAME)
        if sync_success:
            print("Directory sync test successful (or no files to upload).")
        else:
            print("Directory sync test failed or partially failed.")
    else:
        print("Skipping directory sync test: R2_BUCKET_NAME environment variable not set.")
        print("Please set R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, and R2_BUCKET_NAME.")

    # Clean up test sync directory and files
    # if os.path.exists(test_sync_dir):
    #     for root, dirs, files_in_root in os.walk(test_sync_dir, topdown=False):
    #         for name in files_in_root:
    #             os.remove(os.path.join(root, name))
    #         for name in dirs:
    #             os.rmdir(os.path.join(root, name))
    #     os.rmdir(test_sync_dir)
    print("-----------------------------") 