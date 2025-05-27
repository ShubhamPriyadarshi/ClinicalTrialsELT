from __future__ import annotations

import pendulum
import os
import sys
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param # For DAG params
from datetime import datetime

PROJECT_ROOT = "/Users/shubhampriyadarshi/PycharmProjects/ClinicalTrialsELT"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from scripts.extraction.isrctn_extractor import trigger_isrctn_backfill_initialization

# Default start date for the backfill period, can be overridden at DAG trigger time
DEFAULT_BACKFILL_START_DATE = "2005-01-01T00:00:00"
# Default end date for the backfill period (e.g., today), can be overridden
DEFAULT_BACKFILL_END_DATE = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
DEFAULT_SEGMENT_DAYS = 90


def run_backfill_initialization_callable(params):
    """Callable for PythonOperator to trigger backfill initialization."""
    start_date = params.get("backfill_start_date", DEFAULT_BACKFILL_START_DATE)
    end_date = params.get("backfill_end_date", DEFAULT_BACKFILL_END_DATE)
    segment_days = params.get("segment_days", DEFAULT_SEGMENT_DAYS)

    print(f"DAG Run Params: Start Date='{start_date}', End Date='{end_date}', Segment Days={segment_days}")

    # Validate segment_days to ensure it's an integer
    try:
        segment_days_int = int(segment_days)
        if segment_days_int <= 0:
            raise ValueError("segment_days must be a positive integer.")
    except ValueError as e:
        print(f"Error: Invalid segment_days parameter: {segment_days}. {e}")
        raise # Fail the task

    trigger_isrctn_backfill_initialization(start_date, end_date, segment_days_int)


with DAG(
    dag_id="isrctn_backfill_job_initializer",
    start_date=days_ago(1), # Or your preferred start date for the DAG itself
    schedule=None,  # Meant for manual trigger with config
    catchup=False,
    tags=["isrctn", "backfill", "setup"],
    doc_md="""
    ### ISRCTN Backfill Job Initializer DAG

    This DAG is used to initialize (or re-initialize) backfill jobs in the Snowflake control table.
    It calls a function in `isrctn_extractor.py` which first ensures the necessary schema exists
    and then populates the jobs table based on the provided date range and segment size.

    **Trigger with Configuration:**
    - `backfill_start_date`: (String) The overall start date for the backfill (e.g., "2005-01-01T00:00:00").
    - `backfill_end_date`: (String) The overall end date for the backfill (e.g., current UTC datetime as string).
    - `segment_days`: (Integer) The number of days each backfill job segment should cover (e.g., 90).
    """,
    params={
        "backfill_start_date": Param(DEFAULT_BACKFILL_START_DATE, type="string", title="Backfill Start Date (YYYY-MM-DDTHH:MM:SS)"),
        "backfill_end_date": Param(DEFAULT_BACKFILL_END_DATE, type="string", title="Backfill End Date (YYYY-MM-DDTHH:MM:SS)"),
        "segment_days": Param(DEFAULT_SEGMENT_DAYS, type="integer", title="Segment Days for each job"),
    }
) as dag:
    
    initialize_backfill_task = PythonOperator(
        task_id="trigger_backfill_initialization",
        python_callable=run_backfill_initialization_callable,
        # op_kwargs will be automatically populated from dag.params if not specified, 
        # but explicitly passing params from the context is also an option:
        # python_callable=lambda **context: run_backfill_initialization_callable(context['params'])
        # For Airflow 2.2+ and Param class, direct access via context['params'] is standard
        doc_md="""
        #### Trigger Backfill Initialization
        Calls the `trigger_isrctn_backfill_initialization` function from `isrctn_extractor.py`
        using the provided DAG run parameters.
        """
    ) 