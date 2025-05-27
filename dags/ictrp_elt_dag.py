from __future__ import annotations

import pendulum
import os
from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Define the base path for your project.
# This helps in constructing absolute paths to your scripts.
PROJECT_ROOT = "/Users/shubhampriyadarshi/PycharmProjects/ClinicalTrialsELT"
PYTHON_EXECUTABLE = os.path.join(PROJECT_ROOT, ".venv/bin/python") # Adjust if your venv is elsewhere

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

with DAG(
    dag_id="ictrp_elt_pipeline",
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup=False,
    schedule=None, # Define your desired schedule, e.g., "@daily", "0 0 * * *"
    tags=["ictrp", "elt", "snowflake", "dbt"],
    doc_md="""
    ### ICTRP ELT Pipeline

    This DAG performs an ELT process for ICTRP data:
    1.  **Extract**: Downloads the latest ICTRP data file from SharePoint using `ictrp_extractor.py`.
    2.  **Load**: Loads JSON data (currently configured for ISRCTN, not ICTRP) into Snowflake using `irsctn_loader.py`.
        *Note: The loading script `irsctn_loader.py` targets ISRCTN data from `data/raw/isrctn/`.
        For a complete ICTRP pipeline, a dedicated loader script for the extracted ICTRP data would be needed,
        or `irsctn_loader.py` would need to be adapted.*
    3.  **Transform**: A placeholder for dbt transformations.
    """
) as dag:
    
    start_pipeline = EmptyOperator(task_id="start_pipeline")

    extract_isrctn_data = BashOperator(
        task_id="extract_ictrp_data_from_sharepoint",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_EXECUTABLE} scripts/extraction/isrctn_extractor.py",
        doc_md="""
        #### Extract ICTRP Data
        Runs the `ictrp_extractor.py` script to download the latest data from SharePoint.
        The script should handle its own environment variable loading for SharePoint credentials
        and save the file to a predefined location (e.g., `data/raw/ictrp/`).
        """
    )

    # IMPORTANT: The current irsctn_loader.py is set up for ISRCTN data, not ICTRP.
    # This task will run the ISRCTN loader as-is.
    # To load the *extracted ICTRP data*, you would need:
    # 1. A separate loader script for ICTRP data.
    # 2. Or, modify irsctn_loader.py to be more generic or to handle ICTRP data.
    #    This might involve changing `LOCAL_JSON_DIRECTORY` and `TARGET_TABLE_NAME` in that script,
    #    or passing them as arguments if the script supports it.
    load_data_to_snowflake = BashOperator(
        task_id="load_json_to_snowflake",
        bash_command=f"export PYTHONFAULTHANDLER=true && cd {PROJECT_ROOT} && {PYTHON_EXECUTABLE} scripts/loading/irsctn_loader.py",
        doc_md="""
        #### Load Data to Snowflake
        Runs the `irsctn_loader.py` script to load data into Snowflake.
        **Note:** This script is currently configured to load ISRCTN JSON data from
        `data/raw/isrctn/` and targets the `raw_isrctn_json_data` table.
        It does *not* directly use the output of the `extract_ictrp_data` task.
        """
    )

    # Placeholder for dbt transformations
    # You would replace this with BashOperator calls to `dbt run`, `dbt test`, etc.
    # Ensure your dbt project is configured and accessible by Airflow.
    dbt_transform_placeholder = BashOperator(
        task_id="dbt_transform_placeholder",
        bash_command="echo 'Simulating dbt transformations. Replace with actual dbt commands.' && dbt --version", # Example dbt command
        doc_md="""
        #### dbt Transformation Placeholder
        This is a placeholder for your dbt transformation tasks.
        You would typically use `BashOperator` to execute `dbt run`, `dbt test`, etc.
        Example: `dbt run --project-dir {PROJECT_ROOT}/dbt_project --profiles-dir {PROJECT_ROOT}/dbt_project`
        Ensure your dbt project (`dbt_project.yml`, models, etc.) is set up.
        """
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Define task dependencies
    start_pipeline >> extract_isrctn_data >> load_data_to_snowflake >> dbt_transform_placeholder >> end_pipeline 