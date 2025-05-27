# Snowflake Data Loader

import snowflake.connector
import os
from dotenv import load_dotenv

# Assuming s3_uploader is in scripts.utils
# from scripts.utils.s3_uploader import upload_file_to_r2

load_dotenv() 
# Snowflake connection parameters - use Airflow connections or environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE") # Default warehouse
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")      # Target database for raw data
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA") # Target schema

def get_snowflake_conn():
    """Establishes a connection to Snowflake."""
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    return conn

def load_xml_to_snowflake(file_path, table_name, stage_name="raw_data_stage"):
    """Loads XML data from a local file to a Snowflake table with a VARIANT column."""
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE};")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA};")
        
        # Create an internal stage if it doesn't exist
        # cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name} FILE_FORMAT = (TYPE = XML);")
        
        # Put the file into the stage
        # Note: For local files, Snowflake path requires 'file://' prefix
        # For cloud storage, use appropriate prefix like 's3://'
        # Ensure the user running this script has permissions to read the file_path
        put_command = f"PUT file://{os.path.abspath(file_path)} @{stage_name};"
        print(f"Executing PUT command: {put_command}")
        cursor.execute(put_command)
        
        # # Create table if not exists (assuming one VARIANT column for raw XML)
        # # Add a load_timestamp column for tracking
        # cursor.execute(f"""
        #     CREATE TABLE IF NOT EXISTS {table_name} (
        #         raw_data VARIANT,
        #         source_file_name VARCHAR,
        #         load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        #     );
        # """)
        
        # Copy data from stage to table
        # The PARSE_XML function can be used here or later in dbt for more complex parsing
        file_name_in_stage = os.path.basename(file_path)
        copy_command = f"""
            COPY INTO {table_name} (raw_data, source_file_name)
            FROM (SELECT PARSE_XML($1), METADATA$FILENAME FROM @{stage_name}/{file_name_in_stage}) 
            FILE_FORMAT = (TYPE = XML STRIP_OUTER_ELEMENT = FALSE);
        """
        print(f"Executing COPY command: {copy_command}")
        cursor.execute(copy_command)
        
        print(f"Successfully loaded {file_path} to {table_name}")
        
    except Exception as e:
        print(f"Error loading data to Snowflake: {e}")
    finally:
        cursor.close()
        conn.close()

# Example usage (will be called by Airflow tasks)
if __name__ == "__main__":
    # This is a placeholder. In Airflow, file_path would be dynamic (from XComs)
    # And table_name would be specific to the data source.
    # Ensure SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT are set as env vars.
    if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT]):
        print("Please set SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, and SNOWFLAKE_ACCOUNT environment variables.")
    else:
        # Create a dummy XML file for testing
        dummy_xml_content = '''<trials><trial><trial_id>TEST001</trial_id></trial></trials>'''
        dummy_file_path = "dummy_trial_data.xml"
        with open(dummy_file_path, "w") as f:
            f.write(dummy_xml_content)
        
        print(f"Attempting to load {dummy_file_path} to Snowflake table TEST_RAW_TRIALS...")
        load_xml_to_snowflake(dummy_file_path, "TEST_RAW_TRIALS")
        os.remove(dummy_file_path) # Clean up dummy file 