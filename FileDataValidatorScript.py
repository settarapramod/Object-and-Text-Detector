import os
import pandas as pd
import pyodbc
import fnmatch
import logging

# Configuration: File naming patterns and database details
file_db_mapping = {
    "abc_*.csv": {
        "server": "your_server_name",
        "database": "your_database_name",
        "schema": "validation",
        "table": "abc_table",
        "audit_columns": ["created_at", "updated_at"],  # Columns to exclude from DB data
    },
    "xyz_*.csv": {
        "server": "another_server_name",
        "database": "another_database_name",
        "schema": "another_schema",
        "table": "xyz_table",
        "audit_columns": ["audit_id"],  # Example audit column
    },
}

# Set up logging
logging.basicConfig(filename="validation_log.log", level=logging.INFO, format="%(asctime)s - %(message)s")


def connect_to_db(server, database):
    """Establish a database connection."""
    connection_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    return pyodbc.connect(connection_str)


def read_file_data(file_path):
    """Read data from a CSV file."""
    try:
        # Try reading the file with a guessed delimiter
        file_data = pd.read_csv(file_path, skiprows=1)  # Skip header
        if file_data.empty:
            logging.warning(f"File {file_path} is empty.")
        return file_data
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return pd.DataFrame()


def read_db_data(db_details):
    """Fetch data from the database while excluding audit columns."""
    try:
        conn = connect_to_db(db_details["server"], db_details["database"])
        query = f"SELECT * FROM {db_details['schema']}.{db_details['table']}"
        db_data = pd.read_sql(query, conn)
        audit_columns = db_details.get("audit_columns", [])
        db_data = db_data.drop(columns=audit_columns, errors="ignore")  # Remove audit columns
        if db_data.empty:
            logging.warning(f"No data fetched from DB table {db_details['schema']}.{db_details['table']}")
        return db_data
    except Exception as e:
        logging.error(f"Error fetching data from {db_details['schema']}.{db_details['table']}: {e}")
        return pd.DataFrame()


def validate_data(file_path, db_data):
    """Validate data between the file and database record-by-record."""
    try:
        # Read file data
        file_data = read_file_data(file_path)

        # Check if both file and DB data are non-empty
        if file_data.empty or db_data.empty:
            return "FAILED"

        # Align columns and sort for comparison
        common_columns = file_data.columns.intersection(db_data.columns)
        if common_columns.empty:
            logging.warning(f"No common columns between file {file_path} and DB data.")
            return "FAILED"

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
                if fnmatch.fnmatch(filename, pattern):  # Match pattern using fnmatch
                    logging.info(f"Processing file {file_path} with pattern {pattern}")
                    
                    # Read data from the database
                    db_data = read_db_data(db_details)
                    
                    # Validate data
                    validation_status = validate_data(file_path, db_data)
                    
                    # Log results
                    logging.info(
                        f"File: {file_path}, DB: {db_details['database']}, Schema: {db_details['schema']}, "
                        f"Table: {db_details['table']}, Status: {validation_status}"
                    )
                    break


# Specify the base directory
base_directory = "G:/programdata/Test"

# Process the files
process_files(base_directory)
