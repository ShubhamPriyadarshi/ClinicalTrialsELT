CREATE TABLE IF NOT EXISTS isrctn_api_call_log (
        api_call_id VARCHAR PRIMARY KEY,
        job_id VARCHAR, -- FK to isrctn_extraction_jobs.job_id (not enforced by FK for simplicity here)
        api_call_requested_start_utc TIMESTAMP_NTZ,
        api_call_requested_end_utc TIMESTAMP_NTZ,
        api_url VARCHAR,
        api_status_code NUMBER,
        api_response_total_count NUMBER, -- totalCount from API
        api_response_ids_retrieved NUMBER, -- actual IDs in this response
        processing_status VARCHAR, -- SUCCESS, API_ERROR, PARSE_ERROR, TIMEOUT, NO_DATA
        raw_response_local_path VARCHAR,
        raw_response_r2_path VARCHAR,
        call_timestamp_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        error_message VARCHAR,
        duration_ms NUMBER
    );