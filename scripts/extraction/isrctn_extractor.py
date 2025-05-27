# isrctn_extractor.py (Modified)
import requests
import os
import json 
import xml.etree.ElementTree as ET
from typing import List, Tuple, Dict, Any
from datetime import datetime, timedelta, timezone # Ensure timezone awareness
import time
from dotenv import load_dotenv
import xmltodict
import uuid # For unique API call IDs

# Import the snowflake controller
from utils.snowflake_controller import snowflake_controller

load_dotenv()

# Base URL for ISRCTN API
ISRCTN_API_BASE_URL = "https://www.isrctn.com/api"

# Output directory for raw data
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Adjust if needed
RAW_DATA_DIR_BASE = os.path.join(PROJECT_ROOT, "data", "raw", "isrctn_controlled") # New base

# # R2 Configuration
# R2_BUCKET_FOR_ISRCTN = os.getenv("R2_ISRCTN_BUCKET", "your-isrctn-bucket-name")
# R2_OBJECT_PREFIX_ISRCTN = "raw/isrctn_controlled/" # Match new structure

# (fetch_and_save_trial_who_xml remains the same, not directly used by main fetch_trials)

def parse_xml_response(xml_text: str) -> Tuple[int, List[str]]:
    """ Parses XML response to get total count and trial IDs. Handles namespaces. """
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

def _save_json_response_and_log_paths(
    json_text: str, 
    job_id: str, 
    api_call_id: str, 
    chunk_start_dt: datetime, 
    chunk_end_dt: datetime
) -> Tuple[str, str | None]:
    """Saves the JSON response locally and prepares R2 path."""
    # Create a job-specific subdirectory
    job_specific_raw_dir = os.path.join(RAW_DATA_DIR_BASE, job_id)
    os.makedirs(job_specific_raw_dir, exist_ok=True)
    
    fn_chunk_start = chunk_start_dt.strftime("%Y-%m-%dT%H-%M-%S")
    fn_chunk_end = chunk_end_dt.strftime("%Y-%m-%dT%H-%M-%S")
    base_file_name = f"apicall_{api_call_id}_range_{fn_chunk_start}_to_{fn_chunk_end}.json"
    local_json_path = os.path.join(job_specific_raw_dir, base_file_name)

    with open(local_json_path, "w", encoding="utf-8") as f:
        f.write(json_text)
    print(f"Saved raw JSON response locally to: {local_json_path}")

    # r2_object_name = None
    # if R2_BUCKET_FOR_ISRCTN != "your-isrctn-bucket-name" and \
    #    os.getenv("R2_ENDPOINT_URL") and os.getenv("R2_ACCESS_KEY_ID") and os.getenv("R2_SECRET_ACCESS_KEY"):
        
    #     r2_object_name = f"{R2_OBJECT_PREFIX_ISRCTN}{job_id}/{base_file_name}"
    #     # Re-enable actual upload if s3_uploader is available and configured
    #     # upload_success = upload_file_to_r2(...)
    #     # if upload_success: print(f"Successfully uploaded {base_file_name} to R2 as {r2_object_name}.")
    #     # else: print(f"Failed to upload {base_file_name} to R2. Will log R2 path anyway.")
    #     print(f"R2 Upload (simulated): Object name would be {r2_object_name}")
    # else:
    #     print(f"Skipping R2 upload: R2 not fully configured.")
    return local_json_path


def process_extraction_job(job_details: Dict[str, Any], processing_instance_id: str,
                           query: str = "",
                           initial_chunk_size_days: int = 365, # This is now the max size for subdivision
                           min_chunk_size_days: int = 1):
    """
    Processes a single extraction job defined by job_details.
    Uses snowflake_controller for logging and state updates.
    """
    job_id = job_details["job_id"]
    job_requested_start_dt = job_details["requested_start_datetime_utc"]
    job_requested_end_dt = job_details["requested_end_datetime_utc"]
    # Start from where the job left off, or the beginning of the job's requested range
    current_overall_start_dt = job_details.get("last_processed_api_end_datetime_utc") or job_requested_start_dt
    
    print(f"Instance {processing_instance_id} starting job {job_id}: {current_overall_start_dt} to {job_requested_end_dt}")

    all_unique_trial_ids_for_job = set() # Using a set for automatic deduplication within this job run
    total_api_calls_for_this_run = 0 # API calls made in this specific run of process_extraction_job

    # Inner recursive function for chunk processing
    def _fetch_and_process_api_chunk(
        p_api_chunk_start_dt: datetime, 
        p_api_chunk_end_dt: datetime, 
        p_current_chunk_size_days: int # Max size for this attempt
    ):
        nonlocal all_unique_trial_ids_for_job, total_api_calls_for_this_run
        
        api_call_id = str(uuid.uuid4())
        total_api_calls_for_this_run += 1
        
        chunk_start_str_api = p_api_chunk_start_dt.strftime("%Y-%m-%dT%H:%M:%S")
        chunk_end_str_api = p_api_chunk_end_dt.strftime("%Y-%m-%dT%H:%M:%S")
        
        date_query = f"lastEdited GE {chunk_start_str_api} AND lastEdited LT {chunk_end_str_api}"
        full_query = f"({date_query}) AND ({query})" if query else date_query
        api_url = f"{ISRCTN_API_BASE_URL}/query/format/default?q={full_query}&limit=100" # API limit is 100

        print(f"\nJob {job_id} - API Call {api_call_id}:")
        print(f"  Range: {chunk_start_str_api} to {chunk_end_str_api} ({p_current_chunk_size_days} day window)")
        print(f"  URL: {api_url}")

        response_text = None
        http_status_code = None
        error_message_api = None
        api_duration_start = time.monotonic()
        api_call_processing_status = "UNKNOWN"

        try:
            response = requests.get(api_url, timeout=60) # Increased timeout
            http_status_code = response.status_code
            response.raise_for_status()
            response_text = response.text
            api_call_processing_status = "SUCCESS_FETCH" # Intermediate status
        except requests.exceptions.Timeout:
            error_message_api = f"Timeout for URL: {api_url}"
            api_call_processing_status = "TIMEOUT"
            print(f"  Error: {error_message_api}")
        except requests.exceptions.RequestException as e:
            error_message_api = f"RequestException: {e}"
            api_call_processing_status = "API_ERROR"
            print(f"  Error: {error_message_api}")
        except Exception as e:
            error_message_api = f"Unexpected API fetch error: {e}"
            api_call_processing_status = "API_ERROR_UNEXPECTED"
            print(f"  Error: {error_message_api}")
        
        api_duration_ms = int((time.monotonic() - api_duration_start) * 1000)

        if not response_text:
            snowflake_controller.log_api_call(
                job_id, api_call_id, p_api_chunk_start_dt, p_api_chunk_end_dt, api_url,
                http_status_code, error_msg=error_message_api, proc_status=api_call_processing_status,
                duration_ms=api_duration_ms
            )
            return # Cannot proceed without response text

        api_reported_total_count, trial_ids_from_chunk = parse_xml_response(response_text)
        if not trial_ids_from_chunk and api_reported_total_count > 0 and api_call_processing_status == "SUCCESS_FETCH":
             # If parse_xml_response returns empty list but API reported count > 0, it's a parse issue
             api_call_processing_status = "PARSE_ERROR_EMPTY_IDS"
             error_message_api = error_message_api or "parse_xml_response returned empty list despite API totalCount > 0"

        print(f"  API reported {api_reported_total_count} trials. Retrieved {len(trial_ids_from_chunk)} IDs.")
        
        actual_days_in_this_fetch = (p_api_chunk_end_dt - p_api_chunk_start_dt).days
        if actual_days_in_this_fetch == 0 and (p_api_chunk_end_dt - p_api_chunk_start_dt).total_seconds() > 0:
            actual_days_in_this_fetch = 1 # Count it as 1 day if it's a sub-day range

        local_path, r2_path = None, None
        
        if api_reported_total_count == 0:
            print(f"  No trials found by API for this range. XML not saved.")
            api_call_processing_status = "NO_DATA"
        elif api_reported_total_count <= 99: # API limit is 100, so <=99 means we got all for the range
            json_text = json.dumps(xmltodict.parse(response_text))
            local_path = _save_json_response_and_log_paths(json_text, job_id, api_call_id, p_api_chunk_start_dt, p_api_chunk_end_dt)
            all_unique_trial_ids_for_job.update(trial_ids_from_chunk)
            snowflake_controller.store_trial_ids(job_id, api_call_id, trial_ids_from_chunk, p_api_chunk_start_dt)
            api_call_processing_status = "SUCCESS_PROCESSED" if api_call_processing_status == "SUCCESS_FETCH" else api_call_processing_status
        elif actual_days_in_this_fetch <= min_chunk_size_days:
            print(f"  Warning: API limit hit (total: {api_reported_total_count}) but at min chunk size ({actual_days_in_this_fetch} day(s)). Saving data, potential loss.")
            json_text = json.dumps(xmltodict.parse(response_text))
            local_path = _save_json_response_and_log_paths(json_text, job_id, api_call_id, p_api_chunk_start_dt, p_api_chunk_end_dt)
            all_unique_trial_ids_for_job.update(trial_ids_from_chunk)
            snowflake_controller.store_trial_ids(job_id, api_call_id, trial_ids_from_chunk, p_api_chunk_start_dt)
            api_call_processing_status = "SUCCESS_PROCESSED_MIN_CHUNK_LIMIT" if api_call_processing_status == "SUCCESS_FETCH" else api_call_processing_status
        else: # API limit hit, and we can subdivide further
            print(f"  API limit hit (total: {api_reported_total_count}). Subdividing. Raw response for this aggregate call not saved.")
            api_call_processing_status = "SUBDIVIDED" # This call itself didn't save data but triggered subdivision
            
            # Log this "parent" API call that is being subdivided
            snowflake_controller.log_api_call(
                job_id, api_call_id, p_api_chunk_start_dt, p_api_chunk_end_dt, api_url,
                http_status_code, api_reported_total_count, len(trial_ids_from_chunk), 
                proc_status=api_call_processing_status, duration_ms=api_duration_ms,
                error_msg=error_message_api # Log any fetch error even if subdividing
            )
            
            new_sub_chunk_size_days = max(actual_days_in_this_fetch // 2, min_chunk_size_days)
            sub_period_start_dt = p_api_chunk_start_dt
            while sub_period_start_dt < p_api_chunk_end_dt:
                sub_period_end_dt = min(
                    sub_period_start_dt + timedelta(days=new_sub_chunk_size_days),
                    p_api_chunk_end_dt 
                )
                if sub_period_start_dt >= sub_period_end_dt: # Ensure positive duration
                    break 
                _fetch_and_process_api_chunk(sub_period_start_dt, sub_period_end_dt, new_sub_chunk_size_days)
                sub_period_start_dt = sub_period_end_dt
            return # Return because this parent call is done, subdivisions handled recursively

        # Log this specific API call if it wasn't a "SUBDIVIDED" one (those are logged before recursion)
        snowflake_controller.log_api_call(
            job_id, api_call_id, p_api_chunk_start_dt, p_api_chunk_end_dt, api_url,
            http_status_code, api_reported_total_count, len(trial_ids_from_chunk), 
            proc_status=api_call_processing_status, 
            local_path=local_path, r2_path=r2_path, error_msg=error_message_api,
            duration_ms=api_duration_ms
        )
    # --- End of _fetch_and_process_api_chunk ---

    # Main loop for the current job, processing it in primary segments
    # The 'initial_chunk_size_days' now acts as the max size for a segment processed by _fetch_and_process_api_chunk
    # before it decides to subdivide.
    loop_current_start_dt = current_overall_start_dt
    
    try:
        while loop_current_start_dt < job_requested_end_dt:
            # Determine the end of the current primary segment for this job
            # This segment will be passed to _fetch_and_process_api_chunk
            # which will then handle its own subdivisions if necessary.
            current_primary_segment_end_dt = min(
                loop_current_start_dt + timedelta(days=initial_chunk_size_days),
                job_requested_end_dt
            )
            
            print(f"\nJob {job_id} - Processing primary segment: "
                  f"{loop_current_start_dt.strftime('%Y-%m-%dT%H:%M:%S')} to "
                  f"{current_primary_segment_end_dt.strftime('%Y-%m-%dT%H:%M:%S')}")

            _fetch_and_process_api_chunk(
                loop_current_start_dt, 
                current_primary_segment_end_dt, 
                initial_chunk_size_days # Max size for this segment attempt
            )
            
            # Update Snowflake with the progress *within* this job
            snowflake_controller.update_job_progress(job_id, current_primary_segment_end_dt, 0, 0) # API calls/IDs logged at end

            loop_current_start_dt = current_primary_segment_end_dt # Move to the next segment
            time.sleep(1) # Small delay between primary segments

        # Job completed its requested range for this run
        snowflake_controller.finalize_job_status(job_id, 'COMPLETED', 
                                                total_api_calls=total_api_calls_for_this_run,
                                                total_ids=len(all_unique_trial_ids_for_job))
        print(f"\nJob {job_id} completed successfully for instance {processing_instance_id}.")
        print(f"Total unique trial IDs for job {job_id}: {len(all_unique_trial_ids_for_job)}")
        print(f"Total API calls made in this run for job {job_id}: {total_api_calls_for_this_run}")

    except Exception as e:
        error_note = f"Instance {processing_instance_id} failed processing job {job_id}: {e}"
        print(error_note)
        import traceback
        traceback.print_exc()
        snowflake_controller.finalize_job_status(job_id, 'FAILED', notes=error_note,
                                                 total_api_calls=total_api_calls_for_this_run,
                                                 total_ids=len(all_unique_trial_ids_for_job))

def run_one_isrctn_extraction_job():
    """
    Fetches and processes one pending ISRCTN extraction job.
    This function is intended to be called by an Airflow DAG.
    """
    current_processing_instance_id = f"airflow_dag_run_{os.getenv('AIRFLOW_CTX_DAG_RUN_ID', 'manual_' + str(uuid.uuid4()))}"
    print(f"Attempting to run one ISRCTN extraction job. Instance ID: {current_processing_instance_id}")

    job = snowflake_controller.get_next_extraction_job(current_processing_instance_id)
    if job:
        print(f"Found job: {job['job_id']}. Starting processing.")
        process_extraction_job(
            job_details=job,
            processing_instance_id=current_processing_instance_id,
            query="", 
            initial_chunk_size_days=30, # Default for Airflow, can be configured via Airflow vars if needed
            min_chunk_size_days=1
        )
        print(f"Finished processing job {job['job_id']}.")
    else:
        print("No pending ISRCTN extraction jobs found.")

def trigger_isrctn_backfill_initialization(start_date_str: str, end_date_str: str, segment_days_int: int):
    """
    Initializes backfill jobs in Snowflake. Ensures schema exists first.
    This function is intended to be called by an Airflow DAG with parameters.
    """
    print(f"Received backfill parameters: Start='{start_date_str}', End='{end_date_str}', Segment Days={segment_days_int}")
    
    print("Ensuring Snowflake schema exists...")
    snowflake_controller.create_schema_if_not_exists()
    print("Schema check/creation complete.")
    
    print(f"Initializing backfill jobs in Snowflake from {start_date_str} to {end_date_str} with {segment_days_int}-day segments...")
    snowflake_controller.initialize_backfill_jobs(start_date_str, end_date_str, segment_days_int)
    print("Backfill job initialization complete via Airflow.")


if __name__ == "__main__":
    # --- ONE-TIME SETUP (Run manually if needed, not part of regular Airflow execution) ---
    # print("Running initial schema setup in Snowflake...")
    # snowflake_controller.create_schema_if_not_exists()
    # print("Schema setup complete.")
    
    # print("Initializing backfill jobs in Snowflake...")
    # start_date_param = "2005-01-01T00:00:00"
    # end_date_param = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    # snowflake_controller.initialize_backfill_jobs(start_date_param, end_date_param, segment_days=90)
    # print("Backfill job initialization complete.")
    # print("--- If this is the first run, or tables are empty, consider running the one-time setup above manually ---")
    # input("Pausing after setup. Press Enter to continue to extraction loop or Ctrl+C to exit if running standalone.")
    # --- END OF ONE-TIME SETUP ---

    print("\n--- Starting ISRCTN Extractor Main Loop (Standalone Mode) ---")
    current_processing_instance_id = f"isrctn_extractor_host_{os.uname().nodename}_{os.getpid()}"
    print(f"Processing Instance ID (Standalone): {current_processing_instance_id}")

    try:
        while True: 
            job = snowflake_controller.get_next_extraction_job(current_processing_instance_id)
            if job:
                process_extraction_job(
                    job_details=job,
                    processing_instance_id=current_processing_instance_id,
                    query="", 
                    initial_chunk_size_days=30,
                    min_chunk_size_days=1
                )
                print(f"\nFinished processing job {job['job_id']}. Looking for next job...")
                time.sleep(5) 
            else:
                print("No pending jobs found in standalone mode. Sleeping for 60 seconds...")
                time.sleep(60) 
    except KeyboardInterrupt:
        print("Standalone extractor stopped by user.")
    except Exception as e:
        print(f"Critical error in standalone extractor loop: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Standalone extractor shutdown.")