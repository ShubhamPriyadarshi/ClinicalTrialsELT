import os
from dotenv import load_dotenv
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
# from office365.runtime.auth.user_credential import UserCredential # For user auth (not recommended)
import datetime

load_dotenv()

# --- Environment Variables for SharePoint --- 
SHAREPOINT_SITE_URL = os.getenv("SHAREPOINT_SITE_URL")
SHAREPOINT_CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
SHAREPOINT_CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")
SHAREPOINT_TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID") # Not always directly used by ClientCredential, but good to have
SHAREPOINT_ICTRP_FOLDER_PATH = os.getenv("SHAREPOINT_ICTRP_FOLDER_PATH")

# User credentials (alternative, not recommended for unattended scripts)
# SHAREPOINT_USER = os.getenv("SHAREPOINT_USER_PRINCIPAL_NAME")
# SHAREPOINT_PASSWORD = os.getenv("SHAREPOINT_USER_PASSWORD")

# Output directory for raw ICTRP data
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
ICTRP_RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "ictrp")

def get_sharepoint_context():
    """Authenticates to SharePoint and returns a ClientContext object."""
    if not SHAREPOINT_SITE_URL:
        print("Error: SHAREPOINT_SITE_URL environment variable not set.")
        return None

    # App-Only Authentication (Preferred)
    if SHAREPOINT_CLIENT_ID and SHAREPOINT_CLIENT_SECRET:
        print(f"Attempting App-Only authentication to SharePoint site: {SHAREPOINT_SITE_URL}")
        credentials = ClientCredential(SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET)
        ctx = ClientContext(SHAREPOINT_SITE_URL).with_credentials(credentials)
        return ctx
    
    # User Authentication (Example - NOT RECOMMENDED for production/unattended scripts)
    # elif SHAREPOINT_USER and SHAREPOINT_PASSWORD:
    #     print(f"Attempting User authentication to SharePoint site: {SHAREPOINT_SITE_URL} (NOT RECOMMENDED FOR AUTOMATION)")
    #     credentials = UserCredential(SHAREPOINT_USER, SHAREPOINT_PASSWORD)
    #     ctx = ClientContext(SHAREPOINT_SITE_URL).with_credentials(credentials)
    #     return ctx
    else:
        print("Error: SharePoint authentication details not sufficiently provided.")
        print("Please set SHAREPOINT_CLIENT_ID and SHAREPOINT_CLIENT_SECRET (for App-Only auth), ")
        # print("or SHAREPOINT_USER_PRINCIPAL_NAME and SHAREPOINT_USER_PASSWORD (for User auth - not recommended).")
        return None

def extract_latest_ictrp_file_from_sharepoint() -> str | None:
    """
    Connects to SharePoint, finds the latest file in the ICTRP folder, 
    downloads it, and saves it locally.

    Returns:
        str: Path to the downloaded file if successful, None otherwise.
    """
    if not SHAREPOINT_ICTRP_FOLDER_PATH:
        print("Error: SHAREPOINT_ICTRP_FOLDER_PATH environment variable not set.")
        return None

    ctx = get_sharepoint_context()
    if not ctx:
        return None

    try:
        print(f"Accessing SharePoint folder: {SHAREPOINT_ICTRP_FOLDER_PATH}")
        # The library constructs the full URL based on SHAREPOINT_SITE_URL and folder path
        # Ensure SHAREPOINT_ICTRP_FOLDER_PATH is relative to the site or drive root
        # e.g., "Shared Documents/ICTRP Weekly Updates" or for personal site "Documents/ICTRP weekly updates"
        folder = ctx.web.get_folder_by_server_relative_url(SHAREPOINT_ICTRP_FOLDER_PATH)
        files = folder.files.get().execute_query() # Get all files in the folder

        if not files:
            print(f"No files found in SharePoint folder: {SHAREPOINT_ICTRP_FOLDER_PATH}")
            return None

        # Find the latest file based on TimeLastModified
        latest_file = None
        latest_mod_time = None

        for f in files:
            # Ensure TimeLastModified is a valid datetime string and parse it
            try:
                mod_time_str = f.properties.get("TimeLastModified", "")
                # Example format: "2024-01-15T10:30:00Z"
                mod_time = datetime.datetime.fromisoformat(mod_to_strip('Z')) # Make timezone naive for comparison if needed
                
                if latest_file is None or mod_time > latest_mod_time:
                    latest_file = f
                    latest_mod_time = mod_time
            except ValueError as ve:
                print(f"Warning: Could not parse TimeLastModified for file '{f.name}': {mod_time_str}. Error: {ve}")
                continue # Skip this file if date is unparseable

        if not latest_file:
            print(f"Could not determine the latest file in {SHAREPOINT_ICTRP_FOLDER_PATH} (possibly due to date parsing issues).")
            return None

        print(f"Latest file identified: {latest_file.name} (Modified: {latest_mod_time})")

        # Create local directory if it doesn't exist
        os.makedirs(ICTRP_RAW_DATA_DIR, exist_ok=True)
        local_file_path = os.path.join(ICTRP_RAW_DATA_DIR, latest_file.name)

        print(f"Downloading {latest_file.name} to {local_file_path}...")
        with open(local_file_path, "wb") as local_file:
            latest_file.download(local_file).execute_query()
        
        print(f"Successfully downloaded {latest_file.name} to {local_file_path}")
        return local_file_path

    except Exception as e:
        print(f"An error occurred during SharePoint operation: {e}")
        # More specific error handling can be added for common SharePoint exceptions
        return None

# Placeholder for the original extractor logic if it exists or for future use
# def extract_ictrp_data():
# print("ICTRP data extraction complete (placeholder).")
# return "path/to/raw/ictrp_data_folder"

if __name__ == "__main__":
    print("Attempting to extract the latest ICTRP file from SharePoint...")
    print("Ensure SharePoint environment variables are set in your .env file:")
    print("- SHAREPOINT_SITE_URL")
    print("- SHAREPOINT_CLIENT_ID")
    print("- SHAREPOINT_CLIENT_SECRET")
    # print("- SHAREPOINT_TENANT_ID (if applicable)")
    print("- SHAREPOINT_ICTRP_FOLDER_PATH")
    
    downloaded_file = extract_latest_ictrp_file_from_sharepoint()
    if downloaded_file:
        print(f"\nExtraction successful. File saved at: {downloaded_file}")
    else:
        print("\nExtraction failed. Check logs and SharePoint configuration.")