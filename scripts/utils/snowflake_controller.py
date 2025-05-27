# snowflake_controller.py
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import os
from datetime import datetime, timezone
import uuid

# --- Snowflake Connection ---
def get_snowflake_connection():
    """Establishes a connection to Snowflake using environment variables."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise

# --- DDL (Run this once manually in Snowflake to create tables) ---
# Store these DDL statements in a separate .sql file for proper management.
# For now, keeping them here for clarity.

# DDL_STATEMENTS = {
#     "isrctn_extraction_jobs": """
#     CREATE TABLE IF NOT EXISTS isrctn_extraction_jobs (
#         job_id VARCHAR PRIMARY KEY,
#         job_type VARCHAR DEFAULT 'INITIAL_LOAD',
#         requested_start_datetime_utc TIMESTAMP_NTZ,
#         requested_end_datetime_utc TIMESTAMP_NTZ,
#         status VARCHAR, -- PENDING, PROCESSING, COMPLETED, FAILED, PARTIAL_COMPLETED
#         last_processed_api_end_datetime_utc TIMESTAMP_NTZ, -- Tracks progress within a large job
#         attempt_count NUMBER DEFAULT 0,
#         last_attempt_timestamp_utc TIMESTAMP_NTZ,
#         processing_instance_id VARCHAR,
#         job_creation_timestamp_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
#         job_completion_timestamp_utc TIMESTAMP_NTZ,
#         job_notes VARCHAR,
#         total_api_calls_for_job NUMBER,
#         total_unique_ids_found_for_job NUMBER
#     );
#     """,
#     "isrctn_api_call_log": """
#     CREATE TABLE IF NOT EXISTS isrctn_api_call_log (
#         api_call_id VARCHAR PRIMARY KEY,
#         job_id VARCHAR, -- FK to isrctn_extraction_jobs.job_id (not enforced by FK for simplicity here)
#         api_call_requested_start_utc TIMESTAMP_NTZ,
#         api_call_requested_end_utc TIMESTAMP_NTZ,
#         api_url VARCHAR,
#         api_status_code NUMBER,
#         api_response_total_count NUMBER, -- totalCount from API
#         api_response_ids_retrieved NUMBER, -- actual IDs in this response
#         processing_status VARCHAR, -- SUCCESS, API_ERROR, PARSE_ERROR, TIMEOUT, NO_DATA
#         raw_response_local_path VARCHAR,
#         raw_response_r2_path VARCHAR,
#         call_timestamp_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
#         error_message VARCHAR,
#         duration_ms NUMBER
#     );
#     """,
#     "isrctn_trial_ids": """
#     CREATE TABLE IF NOT EXISTS isrctn_trial_ids (
#         trial_id VARCHAR,
#         job_id VARCHAR, 
#         api_call_id VARCHAR,
#         first_seen_in_job_utc TIMESTAMP_NTZ, -- When this ID was first logged for this job_id
#         PRIMARY KEY (trial_id, job_id) -- A trial might be seen by different jobs over time
#     );
#     """
#     # Optional: isrctn_trial_raw_data if storing full JSON/XML in Snowflake
# }

# def create_schema_if_not_exists():
#     """Creates the necessary tables in Snowflake if they don't exist."""
#     conn = None
#     try:
#         conn = get_snowflake_connection()
#         cur = conn.cursor()
#         for table_name, ddl in DDL_STATEMENTS.items():
#             print(f"Ensuring table {table_name} exists...")
#             cur.execute(ddl)
#         print("Schema setup/check complete.")
#     except ProgrammingError as e:
#         print(f"Snowflake ProgrammingError during schema setup: {e}")
#     except Exception as e:
#         print(f"General error during schema setup: {e}")
#     finally:
#         if conn:
#             cur.close()
#             conn.close()

# --- Job Management ---
def initialize_backfill_jobs(start_date_str: str, end_date_str: str, segment_days: int = 365):
    """Populates isrctn_extraction_jobs with initial backfill ranges."""
    conn = None
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        final_end_dt = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        
        current_segment_start_dt = start_dt
        while current_segment_start_dt < final_end_dt:
            job_id = str(uuid.uuid4())
            segment_end_dt = min(current_segment_start_dt + timedelta(days=segment_days), final_end_dt)
            
            # Check if a similar job already exists to avoid duplicates on re-runs
            cur.execute(
                "SELECT COUNT(*) FROM isrctn_extraction_jobs WHERE requested_start_datetime_utc = %s AND requested_end_datetime_utc = %s",
                (current_segment_start_dt, segment_end_dt)
            )
            if cur.fetchone()[0] == 0:
                sql = """
                    INSERT INTO isrctn_extraction_jobs 
                    (job_id, requested_start_datetime_utc, requested_end_datetime_utc, status, job_type, attempt_count)
                    VALUES (%s, %s, %s, 'PENDING', 'INITIAL_BACKFILL', 0)
                """
                cur.execute(sql, (job_id, current_segment_start_dt, segment_end_dt))
                print(f"Initialized job {job_id} for range: {current_segment_start_dt} to {segment_end_dt}")
            else:
                print(f"Job for range {current_segment_start_dt} to {segment_end_dt} already exists. Skipping.")
            current_segment_start_dt = segment_end_dt
        conn.commit()
    except Exception as e:
        print(f"Error initializing backfill jobs: {e}")
        if conn: conn.rollback()
    finally:
        if conn: 
            cur.close()
            conn.close()

def get_next_extraction_job(instance_id: str):
    """
    Atomically fetches a PENDING job or a stale PROCESSING/FAILED job and marks it as PROCESSING.
    """
    conn = None
    job_details = None
    try:
        conn = get_snowflake_connection()
        conn.autocommit = False # Ensure transactional integrity
        cur = conn.cursor()

        # Prioritize PENDING jobs, then FAILED jobs (with retry limits), then stale PROCESSING jobs
        # This query can be made more sophisticated (e.g., max retries for FAILED)
        # Stale: processing for more than X hours (e.g., 4 hours)
        stale_threshold = datetime.now(timezone.utc) - timedelta(hours=4)

        # Order by job_creation_timestamp_utc for PENDING to process older jobs first.
        sql_select_job = f"""
            SELECT job_id, requested_start_datetime_utc, requested_end_datetime_utc, 
                   last_processed_api_end_datetime_utc, attempt_count
            FROM isrctn_extraction_jobs
            WHERE (status = 'PENDING') 
               OR (status = 'FAILED' AND attempt_count < 3) 
               OR (status = 'PROCESSING' AND last_attempt_timestamp_utc < '{stale_threshold.strftime('%Y-%m-%d %H:%M:%S')}')
            ORDER BY CASE status
                        WHEN 'PENDING' THEN 1
                        WHEN 'FAILED' THEN 2
                        WHEN 'PROCESSING' THEN 3
                        ELSE 4
                     END, 
                     job_creation_timestamp_utc
            LIMIT 1;
        """
        cur.execute(sql_select_job)
        row = cur.fetchone()

        if row:
            job_id, req_start, req_end, last_proc_end, attempt_count = row
            
            sql_update_job_status = """
                UPDATE isrctn_extraction_jobs
                SET status = 'PROCESSING', 
                    processing_instance_id = %s,
                    last_attempt_timestamp_utc = CURRENT_TIMESTAMP(),
                    attempt_count = attempt_count + 1 
                WHERE job_id = %s 
                  AND (status = 'PENDING' 
                       OR (status = 'FAILED' AND attempt_count < 3)
                       OR (status = 'PROCESSING' AND last_attempt_timestamp_utc < %s)
                      ); 
            """ # The WHERE clause in UPDATE is critical for atomicity
            
            cur.execute(sql_update_job_status, (instance_id, job_id, stale_threshold))
            
            if cur.rowcount > 0: # Successfully claimed the job
                conn.commit()
                job_details = {
                    "job_id": job_id,
                    "requested_start_datetime_utc": req_start.replace(tzinfo=timezone.utc) if req_start else None,
                    "requested_end_datetime_utc": req_end.replace(tzinfo=timezone.utc) if req_end else None,
                    "last_processed_api_end_datetime_utc": last_proc_end.replace(tzinfo=timezone.utc) if last_proc_end else None,
                    "attempt_count": attempt_count + 1 # Reflects current attempt
                }
                print(f"Instance {instance_id} claimed job: {job_id}")
            else: # Job was claimed by another instance between SELECT and UPDATE
                conn.rollback()
                print(f"Instance {instance_id} failed to claim job {job_id}, likely claimed by another instance.")
        else:
            print(f"Instance {instance_id}: No suitable jobs found.")
            conn.rollback() # Not strictly necessary as no changes made
            
    except Exception as e:
        print(f"Error in get_next_extraction_job: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            conn.autocommit = True # Reset autocommit
            cur.close()
            conn.close()
    return job_details

def log_api_call(job_id: str, api_call_id: str, req_start_utc: datetime, req_end_utc: datetime, 
                 api_url: str, status_code: int = None, resp_total_count: int = None, 
                 ids_retrieved: int = None, proc_status: str = 'UNKNOWN', 
                 local_path: str = None, r2_path: str = None, error_msg: str = None, duration_ms: int = None):
    conn = None
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        sql = """
            INSERT INTO isrctn_api_call_log (
                api_call_id, job_id, api_call_requested_start_utc, api_call_requested_end_utc,
                api_url, api_status_code, api_response_total_count, api_response_ids_retrieved,
                processing_status, raw_response_local_path, raw_response_r2_path, error_message, duration_ms
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (api_call_id, job_id, req_start_utc, req_end_utc, api_url, status_code, 
                           resp_total_count, ids_retrieved, proc_status, local_path, r2_path, error_msg, duration_ms))
        conn.commit()
    except Exception as e:
        print(f"Error logging API call {api_call_id} for job {job_id}: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def store_trial_ids(job_id: str, api_call_id: str, trial_ids: list, first_seen_utc: datetime):
    if not trial_ids:
        return
    conn = None
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        
        # Use MERGE for idempotency: insert if not exists for this job, or just let PK violation be caught if simpler.
        # For this, we assume a trial ID can be associated with multiple jobs if reprocessed,
        # but unique (trial_id, job_id)
        # Simpler: INSERT IGNORE equivalent (Snowflake doesn't have INSERT IGNORE directly)
        # We'll attempt to insert and rely on PK (trial_id, job_id) to prevent true duplicates.
        # A more robust way is MERGE or checking existence first for large batches.
        
        # For batch insert, prepare tuples
        data_to_insert = []
        for trial_id in trial_ids:
            data_to_insert.append((trial_id, job_id, api_call_id, first_seen_utc))
        
        # Snowflake's MERGE is better for "insert if not exists"
        # For simplicity with execute_many, let's stage and merge or handle errors if PK is just trial_id
        # Assuming (trial_id, job_id) is PK
        # We can use a temporary staging table for efficient MERGE if needed.
        # Direct MERGE example (can be slow for many individual merges):
        for trial_id_str in trial_ids:
            merge_sql = """
                MERGE INTO isrctn_trial_ids TGT
                USING (SELECT %s AS trial_id, %s AS job_id, %s AS api_call_id, %s AS first_seen_in_job_utc) SRC
                ON TGT.trial_id = SRC.trial_id AND TGT.job_id = SRC.job_id
                WHEN NOT MATCHED THEN
                    INSERT (trial_id, job_id, api_call_id, first_seen_in_job_utc)
                    VALUES (SRC.trial_id, SRC.job_id, SRC.api_call_id, SRC.first_seen_in_job_utc);
            """
            try:
                cur.execute(merge_sql, (trial_id_str, job_id, api_call_id, first_seen_utc))
            except ProgrammingError as pe: # Catch if, for example, another parallel process inserted it
                if "constraint" in str(pe).lower(): # Highly simplified error check
                    print(f"Trial ID {trial_id_str} for job {job_id} likely already exists (PK violation). Skipping.")
                else:
                    raise # Re-raise other programming errors
        conn.commit()
        print(f"Stored/merged {len(trial_ids)} trial IDs for job {job_id}, API call {api_call_id}.")

    except Exception as e:
        print(f"Error storing trial IDs for job {job_id}: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def update_job_progress(job_id: str, last_processed_api_end_dt: datetime, 
                        current_total_api_calls: int, current_total_ids: int):
    conn = None
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        sql = """
            UPDATE isrctn_extraction_jobs
            SET last_processed_api_end_datetime_utc = %s,
                total_api_calls_for_job = COALESCE(total_api_calls_for_job, 0) + %s, -- Increment
                total_unique_ids_found_for_job = COALESCE(total_unique_ids_found_for_job, 0) + %s -- Increment
            WHERE job_id = %s;
        """
        # Note: total_api_calls and total_ids here should be the DELTA for this chunk, not running total
        # Or the main script must manage the running total and pass it.
        # Let's assume the main script will pass the DELTA for this chunk.
        # A better way: main script sums and passes final total when job completes.
        # For now, let's assume we update with the latest overall state
        # This needs to be thought through: if we are summing per chunk or at the end.
        # Let's modify it to set the last_processed_api_end_datetime_utc only.
        # Totals will be updated at job completion.
        sql_progress = """
             UPDATE isrctn_extraction_jobs
             SET last_processed_api_end_datetime_utc = %s
             WHERE job_id = %s;
        """
        cur.execute(sql_progress, (last_processed_api_end_dt, job_id))
        conn.commit()
    except Exception as e:
        print(f"Error updating job progress for {job_id}: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def finalize_job_status(job_id: str, status: str, notes: str = None, 
                        total_api_calls: int = 0, total_ids: int = 0):
    conn = None
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        sql = """
            UPDATE isrctn_extraction_jobs
            SET status = %s, 
                job_completion_timestamp_utc = CASE WHEN %s IN ('COMPLETED', 'FAILED') THEN CURRENT_TIMESTAMP() ELSE NULL END,
                job_notes = %s,
                total_api_calls_for_job = %s,
                total_unique_ids_found_for_job = %s
            WHERE job_id = %s;
        """
        cur.execute(sql, (status, status, notes, total_api_calls, total_ids, job_id))
        conn.commit()
        print(f"Finalized job {job_id} with status: {status}")
    except Exception as e:
        print(f"Error finalizing job {job_id}: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

# --- Main for schema setup or job initialization ---
if __name__ == '__main__':
    print("Running Snowflake Controller directly...")
    # 1. Create schema
    # create_schema_if_not_exists() # Run once to set up tables

    # 2. Initialize backfill jobs (example)
    # Ensure SNOWFLAKE_... environment variables are set before running this.
    # print("Initializing backfill jobs...")
    # backfill_start = "2005-01-01T00:00:00"
    # backfill_end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") # Up to now
    # initialize_backfill_jobs(backfill_start, backfill_end, segment_days=90) # e.g., 90-day job segments
    # print("Backfill job initialization complete (if any new jobs were needed).")

    # 3. Test get_next_extraction_job
    # print("Testing get_next_extraction_job...")
    # test_instance_id = f"test_instance_{uuid.uuid4()}"
    # job = get_next_extraction_job(test_instance_id)
    # if job:
    #     print(f"Test instance claimed job: {job}")
        # Remember to manually reset status if testing, or use finalize_job_status
        # finalize_job_status(job['job_id'], 'PENDING', 'Reset after test') # Example reset
    # else:
    #     print("No job available for test instance.")
    pass