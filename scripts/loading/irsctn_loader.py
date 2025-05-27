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
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA") 

# --- Data Configuration ---
# Construct absolute path for LOCAL_JSON_DIRECTORY
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
LOCAL_JSON_DIRECTORY = os.path.join(PROJECT_ROOT, 'data', 'raw', 'isrctn')

TARGET_TABLE_NAME = 'raw_isrctn_json_data' # Made table name more specific
# Fully qualified stage name (ensures it's created in the correct db/schema)
STAGE_NAME = f'{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.isrctn_json_stage' 

def get_snowflake_connection():
    """Establishes and returns a Snowflake connection."""
    if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA]):
        print("Error: One or more Snowflake connection environment variables are not set.")
        print("Please set: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA")
        raise ValueError("Missing Snowflake connection details in environment variables.")
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("Successfully connected to Snowflake.")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise

# def create_stage_if_not_exists(conn, stage_name):
#     """Creates the Snowflake stage if it doesn't already exist."""
#     print(f"\nEnsuring stage '{stage_name}' exists...")
#     # A basic json file format for the stage, specific format applied during COPY
#     create_stage_command = f"CREATE STAGE IF NOT EXISTS {stage_name} FILE_FORMAT = (TYPE = JSON);"
#     try:
#         with conn.cursor() as cur:
#             cur.execute(create_stage_command)
#         print(f"Stage '{stage_name}' is ready.")
#     except Exception as e:
#         print(f"Error creating or ensuring stage '{stage_name}': {e}")
#         raise

# def create_table_if_not_exists(conn, table_name):
#     """Creates the target Snowflake table if it doesn't already exist."""
#     print(f"\nEnsuring table '{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}' exists...")
#     create_table_command = f"""
#     CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name} (
#         FILENAME VARCHAR,
#         JSON_CONTENT VARIANT,
#         LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
#     );
#     """
#     try:
#         with conn.cursor() as cur:
#             cur.execute(create_table_command)
#         print(f"Table '{table_name}' is ready.")
#     except Exception as e:
#         print(f"Error creating or ensuring table '{table_name}': {e}")
#         raise

def stage_local_json_files(conn, local_dir, stage_name):
    """Uploads all json files from a local directory to a Snowflake internal stage."""
    if not os.path.isdir(local_dir):
        print(f"Error: Local directory '{local_dir}' not found.")
        return False

    staged_files_count = 0
    with conn.cursor() as cur:
        print(f"\nStaging json files from '{local_dir}' to '{stage_name}'...")
        for filename in os.listdir(local_dir):
            if filename.lower().endswith('.json'):
                local_file_path = os.path.join(local_dir, filename)
                snowflake_local_file_path = local_file_path.replace('\\', '/')
                
                put_command = f"PUT file://{snowflake_local_file_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
                try:
                    print(f"  Staging {filename}...")
                    cur.execute(put_command)
                    result = cur.fetchone()
                    if result and result[6] == 'UPLOADED':
                        print(f"    Successfully staged: {filename}")
                        staged_files_count += 1
                    else:
                        print(f"    Failed to stage {filename} or status unknown. Result: {result}")
                except Exception as e:
                    print(f"    Error staging {filename}: {e}")
        
        if staged_files_count > 0:
            print(f"\nSuccessfully staged {staged_files_count} json files.")
            return True
        else:
            print("\nNo json files were found in the directory or an error occurred during staging.")
            return False

def copy_json_from_stage_to_table(conn, stage_name, table_name):
    """Copies json files from the Snowflake stage into a target table."""
    print(f"\nCopying data from stage '{stage_name}' to table '{table_name}'...")
    copy_command = f"""
    COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name} (filename, json_content)
    FROM (
        SELECT METADATA$FILENAME, $1
        FROM @{stage_name}
    )
    FILE_FORMAT = (TYPE = JSON)
    PATTERN = '.*.json.*' 
    ON_ERROR = 'CONTINUE'; 
    """
    try:
        with conn.cursor() as cur:
            cur.execute(copy_command)
            result = cur.fetchall()
            print("COPY INTO command executed.")
            loaded_count = 0
            for row in result:
                print(f"  File: {row[0]}, Status: {row[1]}, Rows Parsed: {row[2]}, Rows Loaded: {row[3]}")
                if row[1] == 'LOADED':
                    loaded_count += row[3] # Using rows_loaded
            if loaded_count > 0:
                 print(f"\nSuccessfully loaded data from {loaded_count} records (files) into '{table_name}'.")
            else:
                print(f"\nNo data loaded into '{table_name}'. Check stage content, COPY command, and file patterns.")

    except Exception as e:
        print(f"Error copying data into table {table_name}: {e}")

def main():
    """Main function to orchestrate the json ingestion process."""
    conn = None
    try:
        conn = get_snowflake_connection()
        
        # Ensure Snowflake objects exist
        # create_stage_if_not_exists(conn, STAGE_NAME)
        # create_table_if_not_exists(conn, TARGET_TABLE_NAME)
        
        if stage_local_json_files(conn, LOCAL_JSON_DIRECTORY, STAGE_NAME):
            copy_json_from_stage_to_table(conn, STAGE_NAME, TARGET_TABLE_NAME)
        else:
            print("Skipping COPY INTO operation as no files were staged or staging failed.")

    except Exception as e:
        print(f"An overall error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("\nSnowflake connection closed.")

if __name__ == '__main__':
    # Before running, ensure:
    # 1. You have installed snowflake-connector-python and python-dotenv.
    # 2. You have a .env file with Snowflake connection details (see .env.example).
    # 3. The local directory 'data/raw/isrctn' (relative to project root) exists and contains json files.
    # This script will now attempt to create the stage and table if they don't exist.
    main()
