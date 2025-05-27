# ISRCTN Data Extractor

import requests
import os
import json # Keep for potential future use with other ISRCTN endpoints
import xml.etree.ElementTree as ET
from typing import List, Tuple
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import json
import xmltodict

# Assuming s3_uploader is in scripts.utils
# from scripts.utils.s3_uploader import upload_file_to_r2

load_dotenv() # Load environment variables from .env file at the beginning

# Base URL for ISRCTN API
ISRCTN_API_BASE_URL = "https://www.isrctn.com/api"

# Output directory for raw data
# PROJECT_ROOT should ideally be dynamically determined or passed as config
# Assuming script is run from project root or scripts/extraction for this relative path
# For Airflow, this path will be more carefully managed.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "isrctn")

# R2 Configuration (consider moving to environment variables or a config file)
R2_BUCKET_FOR_ISRCTN = os.getenv("R2_ISRCTN_BUCKET", "your-isrctn-bucket-name") # Example, get from env
R2_OBJECT_PREFIX_ISRCTN = "raw/isrctn/"

# API Key - not needed for this specific public endpoint
# ISRCTN_API_KEY = os.getenv("ISRCTN_API_KEY")

def fetch_and_save_trial_who_xml(trial_id: str, output_dir: str) -> str | None:
    """Fetches a specific trial in WHO format from ISRCTN and saves it as XML."""
    api_url = f"{ISRCTN_API_BASE_URL}/trial/{trial_id}/format/default"
    print(f"Fetching data from: {api_url}")

    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        file_name = f"{trial_id}_default.xml"
        output_path = os.path.join(output_dir, file_name)
        
        # The API returns XML directly, so we save response.text
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(response.text)
            
        print(f"Successfully fetched and saved data to {output_path}")
        return output_path
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {trial_id}: {e}")
        return None
    except IOError as e:
        print(f"Error saving data for {trial_id} to {output_path}: {e}")
        return None
    
# def parse_xml_response(response_text: str) -> Tuple[int, List[str]]:
#     """
#     Parse XML response to get total count and trial IDs.
    
#     Args:
#         response_text (str): XML response text
        
#     Returns:
#         Tuple[int, List[str]]: (total_count, list of trial IDs)
#     """
#     root = ET.fromstring(response_text)
#     # Get total count from the allTrials element
#     total_count = int(root.get('totalCount', 0))
    
#     # Extract trial IDs
#     trial_ids = []
#     for trial in root.findall('.//{http://www.67bricks.com/isrctn}isrctn'):
#         if trial.text:
#             trial_ids.append(trial.text)
    
#     return total_count, trial_ids

def parse_xml_response(xml_text: str) -> Tuple[int, List[str]]:
    """
    Parses XML response to get total count and trial IDs.
    Handles namespaces.
    """
    try:
        root = ET.fromstring(xml_text)
        namespaces = {'ns': 'http://www.67bricks.com/isrctn'}
        
        total_count_str = root.get('totalCount')
        if total_count_str is None:
            print("Warning: 'totalCount' attribute not found in XML root.")
            return 0, []
            
        total_count = int(total_count_str)
        
        trial_ids = []
        for trial_elem in root.findall('ns:trial', namespaces):
            id_elem = trial_elem.find('ns:id', namespaces)
            if id_elem is not None and id_elem.text:
                trial_ids.append(id_elem.text.strip())
        return total_count, trial_ids
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return 0, []
    except Exception as e:
        print(f"Unexpected error in parse_xml_response: {e}")
        return 0, []

_api_call_counter = 0 # Global or class member to ensure unique IDs across calls if instance is reused

def _save_xml_response_and_upload(xml_text: str, start_dt: datetime, end_dt: datetime, api_call_id: int):
    """Saves the XML response to a file and uploads it to R2."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    fn_chunk_start = start_dt.strftime("%Y-%m-%dT%H-%M-%S")
    fn_chunk_end = end_dt.strftime("%Y-%m-%dT%H-%M-%S")
    base_file_name = f"isrctn_call_{api_call_id}_range_{fn_chunk_start}_to_{fn_chunk_end}.json"
    local_xml_path = os.path.join(RAW_DATA_DIR, base_file_name)

    with open(local_xml_path, "w", encoding="utf-8") as f:
        data_dict = xmltodict.parse(xml_text)
        json_data = json.dumps(data_dict)
        f.write(json_data)
    
    # Write the json data to output 
    # json file
    print(f"Saved raw XML response locally to: {local_xml_path}")

    # Upload to R2
    if R2_BUCKET_FOR_ISRCTN == "your-isrctn-bucket-name":
         print(f"Skipping R2 upload: R2_ISRCTN_BUCKET is set to placeholder. Please configure R2_ISRCTN_BUCKET environment variable.")
    elif os.getenv("R2_ENDPOINT_URL") and os.getenv("R2_ACCESS_KEY_ID") and os.getenv("R2_SECRET_ACCESS_KEY"):
        r2_object_name = f"{R2_OBJECT_PREFIX_ISRCTN}{base_file_name}"
    #     upload_success = upload_file_to_r2(
    #         local_file_path=local_xml_path,
    #         object_name=r2_object_name,
    #         bucket_name=R2_BUCKET_FOR_ISRCTN
    #     )
    #     if upload_success:
    #         print(f"Successfully uploaded {base_file_name} to R2.")
    #         # Optionally, delete local file after successful upload
    #         # os.remove(local_xml_path)
    #         # print(f"Removed local file: {local_xml_path}")
    #     else:
    #         print(f"Failed to upload {base_file_name} to R2. File remains locally at {local_xml_path}")
    # else:
    #     print(f"Skipping R2 upload: R2 connection environment variables (R2_ENDPOINT_URL, etc.) not fully set.")

def fetch_trials(
    query: str = "",
    start_date: str = "2005-01-01T00:00:00",
    end_date: str = None,
    initial_chunk_size_days: int = 365,
    min_chunk_size_days: int = 1
) -> List[str]:
    """
    Fetches trial IDs from ISRCTN API with dynamic pagination using lastEdited dates.
    Saves XML responses only for chunks that are not further subdivided or are at the minimum grain and have totalCount > 0.
    Uploads saved XML to R2.
    """
    global _api_call_counter
    _api_call_counter = 0 

    if end_date is None:
        end_date_dt = datetime.now()
    else:
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")

    current_start_dt = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
    final_end_dt = end_date_dt

    all_trial_ids_list: List[str] = []

    def _fetch_and_process_chunk(
        p_chunk_start_dt: datetime, 
        p_chunk_end_dt: datetime, 
        p_current_chunk_size_days: int
    ):
        nonlocal all_trial_ids_list # To append to the list in the outer scope
        global _api_call_counter
        _api_call_counter += 1
        current_api_call_id = _api_call_counter

        chunk_start_str_api = p_chunk_start_dt.strftime("%Y-%m-%dT%H:%M:%S")
        chunk_end_str_api = p_chunk_end_dt.strftime("%Y-%m-%dT%H:%M:%S")
        
        date_query = f"lastEdited GE {chunk_start_str_api} AND lastEdited LT {chunk_end_str_api}"
        if query:
            full_query = f"({date_query}) AND ({query})"
        else:
            full_query = date_query
            
        api_url = f"{ISRCTN_API_BASE_URL}/query/format/default?q={full_query}&limit=100"
        
        print(f"\nFetching (API Call ID: {current_api_call_id}):")
        print(f"Date range: {chunk_start_str_api} to {chunk_end_str_api}")
        print(f"Current chunk size parameter: {p_current_chunk_size_days} days")
        print(f"API URL: {api_url}")
        
        response_text = None
        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            response_text = response.text
        except requests.exceptions.Timeout:
            print(f"Timeout during API Call ID: {current_api_call_id} for URL: {api_url}")
            return
        except requests.exceptions.RequestException as e:
            print(f"Error fetching chunk (API Call ID: {current_api_call_id}): {e}")
            return
        except Exception as e:
            print(f"Unexpected error during API fetch (API Call ID: {current_api_call_id}): {e}")
            return

        total_count, trial_ids_from_chunk = parse_xml_response(response_text)
        print(f"API reported {total_count} total trials for this range. Retrieved {len(trial_ids_from_chunk)} IDs in this call.")
        # time.sleep(1) 

        actual_days_in_this_fetch = (p_chunk_end_dt - p_chunk_start_dt).days
        if actual_days_in_this_fetch == 0 and (p_chunk_end_dt - p_chunk_start_dt).total_seconds() > 0:
            actual_days_in_this_fetch = 1

        if total_count == 0:
            print(f"No trials found for this range (API Call ID: {current_api_call_id}). XML will not be saved or uploaded.")
        elif total_count <= 99:
            _save_xml_response_and_upload(response_text, p_chunk_start_dt, p_chunk_end_dt, current_api_call_id)
            all_trial_ids_list.extend(trial_ids_from_chunk)
        elif actual_days_in_this_fetch <= min_chunk_size_days:
            print(f"Warning: API limit hit (total: {total_count}) for date range {chunk_start_str_api} to {chunk_end_str_api}, "
                  f"but chunk's actual day span ({actual_days_in_this_fetch} day(s)) is at/below minimum ({min_chunk_size_days} days). "
                  f"Saving and attempting upload of the {len(trial_ids_from_chunk)} IDs fetched. Data loss may occur.")
            _save_xml_response_and_upload(response_text, p_chunk_start_dt, p_chunk_end_dt, current_api_call_id)
            all_trial_ids_list.extend(trial_ids_from_chunk)
        else:
            print(f"API limit hit (total: {total_count}) for range. Subdividing. XML for call {current_api_call_id} (range {chunk_start_str_api} to {chunk_end_str_api}) will NOT be saved or uploaded.")
            new_sub_chunk_size_days = max(actual_days_in_this_fetch // 2, min_chunk_size_days)
            sub_period_start_dt = p_chunk_start_dt
            while sub_period_start_dt < p_chunk_end_dt:
                sub_period_end_dt = min(
                    sub_period_start_dt + timedelta(days=new_sub_chunk_size_days),
                    p_chunk_end_dt 
                )
                if sub_period_start_dt == sub_period_end_dt and (sub_period_end_dt - sub_period_start_dt).total_seconds() <=0 :
                    break 
                _fetch_and_process_chunk(sub_period_start_dt, sub_period_end_dt, new_sub_chunk_size_days)
                sub_period_start_dt = sub_period_end_dt
    
    main_loop_current_start_dt = current_start_dt
    primary_segment_count = 0
    while main_loop_current_start_dt < final_end_dt:
        primary_segment_count += 1
        current_primary_chunk_end_dt = min(
            main_loop_current_start_dt + timedelta(days=initial_chunk_size_days),
            final_end_dt
        )
        
        print(f"\nProcessing primary date segment {primary_segment_count}: "
              f"{main_loop_current_start_dt.strftime('%Y-%m-%dT%H:%M:%S')} to "
              f"{current_primary_chunk_end_dt.strftime('%Y-%m-%dT%H:%M:%S')}")

        _fetch_and_process_chunk(
            main_loop_current_start_dt, 
            current_primary_chunk_end_dt, 
            initial_chunk_size_days
        )
        
        main_loop_current_start_dt = current_primary_chunk_end_dt
            
    print(f"\nCompleted processing {primary_segment_count} primary date segments.")
    print(f"Total API calls made: {_api_call_counter}")
    unique_trial_ids = list(set(all_trial_ids_list))
    print(f"Total unique trial IDs found: {len(unique_trial_ids)}")
    return unique_trial_ids

# Original function placeholder - can be adapted or removed if only specific fetching is needed now
def extract_isrctn_data():
    """Extracts data from the ISRCTN API (placeholder for broader extraction)."""
    print("Extracting data from ISRCTN...")
    # TODO: Implement broader API call logic, e.g., searching trials
    # Example:
    # headers = {"Authorization": f"Bearer {ISRCTN_API_KEY}"}
    # params = {"q": "cancer", "pageSize": 100} # example query
    # search_url = f"{ISRCTN_API_BASE_URL}/v0.6/trials" # Assuming a search endpoint
    # response = requests.get(search_url, headers=headers, params=params)
    # response.raise_for_status()
    # data = response.json() # if the search endpoint returns JSON
    # with open(os.path.join(RAW_DATA_DIR, "isrctn_search_results.json"), "w") as f:
    #     json.dump(data, f)
    print("ISRCTN data extraction complete (placeholder - use specific functions like fetch_and_save_trial_who_xml).")
    # This function would return a path to a manifest or a list of files in a real scenario
    return os.path.join(RAW_DATA_DIR, "general_isrctn_data_placeholder.xml_or_json") 

if __name__ == "__main__":
    start_date_str = "2005-01-01T00:00:00"
    # For testing, let's use a smaller range initially, e.g., 1 year or a few months
    # end_date_str = "2006-01-01T00:00:00" 
    end_date_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S") # Or current date

    print(f"Starting ISRCTN trial ID extraction for period: {start_date_str} to {end_date_str}")
    
    trial_ids_found = fetch_trials(
        query="",  # Example: "cancer" or leave empty for all
        start_date=start_date_str,
        end_date=end_date_str,
        initial_chunk_size_days=365, # Start with yearly chunks
        min_chunk_size_days=1        # Min chunk is 1 day
    )
    print(f"\n--- Extraction Summary ---")
    print(f"Total unique trial IDs collected: {len(trial_ids_found)}")
    # print(f"First 10 IDs: {trial_ids_found[:10]}") 