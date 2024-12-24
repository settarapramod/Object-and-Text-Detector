import os
import pandas as pd
import pyodbc
import re
import logging

# Configuration: File naming patterns and database details
file_db_mapping = {
    r"abc_\w+_\w+\.csv": {
        "server": "your_server_name",
        "database": "your_database_name",
        "schema": "validation",
        "table": "abc",
    },
    # Add more patterns and details as needed
    r"xyz_\d+\.csv": {
        "server": "another_server_name",
        "database": "another_database_name",
        "schema": "another_schema",
        "table": "xyz",
    },
}

# Set up logging
logging.basicConfig(filename="validation_log.log", level=logging.INFO, format="%(asctime)s - %(message)s")

def connect_to_db(server, database):
    """Establish a database connection."""
    connection_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    return pyodbc.connect(connection_str)

def validate_file(file_path, db_details):
    """Validate file data against database data."""
    try:
        # Read file data into a DataFrame
        file_data = pd.read_csv(file_path)
        logging.info(f"Read {file_path} with {len(file_data)} rows.")
        
        # Connect to the database
        conn = connect_to_db(db_details["server"], db_details["database"])
        cursor = conn.cursor()

        # Build and execute the query
        query = f"SELECT * FROM {db_details['schema']}.{db_details['table']}"
        db_data = pd.read_sql(query, conn)
        
        # Validation: Compare counts (basic example)
        file_count = len(file_data)
        db_count = len(db_data)
        validation_status = "PASSED" if file_count == db_count else "FAILED"
        
        # Log results
        logging.info(
            f"File: {file_path}, DB: {db_details['database']}, Schema: {db_details['schema']}, "
            f"Table: {db_details['table']}, File Count: {file_count}, DB Count: {db_count}, Status: {validation_status}"
        )
    except Exception as e:
        logging.error(f"Error validating {file_path}: {e}")

def process_files(base_directory):
    """Process all files in the base directory and validate them."""
    for dirpath, dirnames, filenames in os.walk(base_directory):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            for pattern, db_details in file_db_mapping.items():
                if re.match(pattern, filename):
                    logging.info(f"Processing file {file_path} with pattern {pattern}")
                    validate_file(file_path, db_details)
                    break

# Specify the base directory
base_directory = "path/to/your/base/directory"

# Process the files
process_files(base_directory)
