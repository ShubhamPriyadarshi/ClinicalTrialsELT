CREATE TABLE IF NOT EXISTS isrctn_extraction_jobs (
    job_id VARCHAR PRIMARY KEY,
    job_type VARCHAR DEFAULT 'INITIAL_LOAD',
    requested_start_datetime_utc TIMESTAMP_NTZ,
    requested_end_datetime_utc TIMESTAMP_NTZ,
    status VARCHAR, -- PENDING, PROCESSING, COMPLETED, FAILED, PARTIAL_COMPLETED
    last_processed_api_end_datetime_utc TIMESTAMP_NTZ,
    attempt_count NUMBER DEFAULT 0,
    last_attempt_timestamp_utc TIMESTAMP_NTZ,
    processing_instance_id VARCHAR,
    job_creation_timestamp_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    job_completion_timestamp_utc TIMESTAMP_NTZ,
    job_notes VARCHAR,
    total_api_calls_for_job NUMBER,
    total_unique_ids_found_for_job NUMBER
);