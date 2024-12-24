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
        "audit_columns": ["created_at", "updated_at"],  # Columns to exclude from DB data
    },
    r"xyz_\d+\.csv": {
        "server": "another_server_name",
        "database": "another_database_name",
        "schema": "another_schema",
        "table": "xyz",
        "audit_columns": ["audit_id"],  # Example audit column
    },
}

# Set up logging
logging.basicConfig(filename="validation_log.log", level=logging.INFO, format="%(asctime)s - %(message)s")


def connect_to_db(server, database):
    """Establish a database connection."""
    connection_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    return pyodbc.connect(connection_str)


def read_db_data(db_details):
    """Fetch data from the database while excluding audit columns."""
    try:
        conn = connect_to_db(db_details["server"], db_details["database"])
        query = f"SELECT * FROM {db_details['schema']}.{db_details['table']}"
        db_data = pd.read_sql(query, conn)
        audit_columns = db_details.get("audit_columns", [])
        db_data = db_data.drop(columns=audit_columns, errors="ignore")  # Remove audit columns
        return db_data
    except Exception as e:
        logging.error(f"Error fetching data from {db_details['schema']}.{db_details['table']}: {e}")
        return pd.DataFrame()


def validate_data(file_path, db_data):
    """Validate data between the file and database record-by-record."""
    try:
        # Read the file into a DataFrame, skipping the header
        file_data = pd.read_csv(file_path, skiprows=1)  # Skip header
        file_data.columns = [col.strip() for col in file_data.columns]  # Clean column names

        # Ensure the dataframes have the same columns
        common_columns = file_data.columns.intersection(db_data.columns)
        file_data = file_data[common_columns].sort_values(by=common_columns[0]).reset_index(drop=True)
        db_data = db_data[common_columns].sort_values(by=common_columns[0]).reset_index(drop=True)

        # Record-by-record comparison
        match_status = file_data.equals(db_data)
        return "PASSED" if match_status else "FAILED"
    except Exception as e:
        logging.error(f"Error validating data for file {file_path}: {e}")
        return "FAILED"


def process_files(base_directory):
    """Process all files in the base directory and validate them."""
    for dirpath, dirnames, filenames in os.walk(base_directory):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            for pattern, db_details in file_db_mapping.items():
                if re.match(pattern, filename):
                    logging.info(f"Processing file {file_path} with pattern {pattern}")
                    
                    # Read data from the database
                    db_data = read_db_data(db_details)
                    if db_data.empty:
                        logging.warning(f"No data fetched from DB for file {file_path}")
                        continue
                    
                    # Validate data
                    validation_status = validate_data(file_path, db_data)
                    
                    # Log results
                    logging.info(
                        f"File: {file_path}, DB: {db_details['database']}, Schema: {db_details['schema']}, "
                        f"Table: {db_details['table']}, Status: {validation_status}"
                    )
                    break


# Specify the base directory
base_directory = "path/to/your/base/directory"

# Process the files
process_files(base_directory)
