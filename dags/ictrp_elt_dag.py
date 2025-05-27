from __future__ import annotations

import pendulum
import os
import sys 
from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/Users/shubhampriyadarshi/PycharmProjects/ClinicalTrialsELT"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Import target functions from your scripts
# Changed from fetch_trials to run_one_isrctn_extraction_job
from scripts.extraction.isrctn_extractor import run_one_isrctn_extraction_job 
from scripts.loading.irsctn_loader import main as irsctn_loader_main
import faulthandler 

# --- Environment Variables for Scripts ---
# Ensure your .env file (or Airflow connections) provides these
# For ictrp_extractor.py
# SHAREPOINT_SITE_URL
# SHAREPOINT_CLIENT_ID
# SHAREPOINT_CLIENT_SECRET
# SHAREPOINT_ICTRP_FOLDER_PATH

# For irsctn_loader.py (assuming it uses similar env vars for Snowflake)
# SNOWFLAKE_USER
# SNOWFLAKE_PASSWORD
# SNOWFLAKE_ACCOUNT
# SNOWFLAKE_WAREHOUSE
# SNOWFLAKE_DATABASE
# SNOWFLAKE_SCHEMA

# The wrapper for fetch_trials is no longer needed as run_one_isrctn_extraction_job handles its own logic
# def run_fetch_trials_with_defaults(): ... 

def run_irsctn_loader_with_faulthandler():
    """Wrapper to call irsctn_loader.main() and enable faulthandler."""
    faulthandler.enable()
    print("PYTHONFAULTHANDLER enabled via faulthandler.enable()")
    print("Starting irsctn_loader.main() via PythonOperator...")
    irsctn_loader_main()
    print("PythonOperator: irsctn_loader_main completed.")

with DAG(
    dag_id="isrctn_elt_pipeline", 
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["isrctn", "elt", "snowflake", "dbt"], 
    doc_md="""
    ### ISRCTN ELT Pipeline

    This DAG performs an ELT process for ISRCTN data:
    1.  **Extract**: Fetches and processes one pending ISRCTN extraction job using `run_one_isrctn_extraction_job` from `isrctn_extractor.py`.
    2.  **Load**: Loads JSON data from the local `data/raw/isrctn_controlled/` directory (populated by the extractor)
        into Snowflake using `irsctn_loader.py`.
    3.  **Transform**: A placeholder for dbt transformations.
    """
) as dag:
    
    start_pipeline = EmptyOperator(task_id="start_pipeline")

    extract_isrctn_data_task = PythonOperator(
        task_id="run_one_isrctn_extraction_job_task", 
        python_callable=run_one_isrctn_extraction_job, 
        doc_md="""
        #### Run One ISRCTN Extraction Job
        Calls `run_one_isrctn_extraction_job` from `scripts.extraction.isrctn_extractor.py`.
        This function will pick up one pending job from the Snowflake control table,
        extract the data, save it locally, and update control tables.
        """
    )

    load_data_to_snowflake = PythonOperator(
        task_id="load_json_to_snowflake",
        python_callable=run_irsctn_loader_with_faulthandler, 
        doc_md="""
        #### Load Data to Snowflake
        Runs the `main` function from `scripts.loading.irsctn_loader.py`
        to load JSON data from `data/raw/isrctn_controlled/` into Snowflake.
        `PYTHONFAULTHANDLER` is enabled for this task.
        """
    )

    dbt_transform_placeholder = PythonOperator(
        task_id="dbt_transform_placeholder",
        python_callable=lambda: print("Simulating dbt transformations. Replace with actual dbt execution logic or use BashOperator."),
        doc_md="""
        #### dbt Transformation Placeholder
        This is a placeholder for your dbt transformation tasks.
        You would typically use `BashOperator` or a dbt-specific operator to execute `dbt run`, `dbt test`, etc.
        Example: `dbt run --project-dir {PROJECT_ROOT}/dbt_project --profiles-dir {PROJECT_ROOT}/dbt_project`
        Ensure your dbt project (`dbt_project.yml`, models, etc.) is set up.
        """
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Define task dependencies
    start_pipeline >> extract_isrctn_data_task >> load_data_to_snowflake >> dbt_transform_placeholder >> end_pipeline 