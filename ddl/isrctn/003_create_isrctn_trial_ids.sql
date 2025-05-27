CREATE TABLE IF NOT EXISTS isrctn_trial_ids (
        trial_id VARCHAR,
        job_id VARCHAR, 
        api_call_id VARCHAR,
        first_seen_in_job_utc TIMESTAMP_NTZ, -- When this ID was first logged for this job_id
        PRIMARY KEY (trial_id, job_id) -- A trial might be seen by different jobs over time
    );