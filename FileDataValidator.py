import os
import pandas as pd
import pyodbc
import re
import logging


def setup_logging(log_file="validation_log.log"):
    """Set up logging configuration."""
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def connect_to_db(server, database):
    """Establish a database connection."""
    logging.info(f"Connecting to database '{database}' on server '{server}'...")
    connection_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    return pyodbc.connect(connection_str)


def convert_booleans(dataframe):
    """Convert boolean values in the DataFrame to integers (0 or 1)."""
    logging.info("Converting boolean values to 0/1...")
    for column in dataframe.columns:
        if dataframe[column].dtype == "bool":
            dataframe[column] = dataframe[column].astype(int)
    return dataframe


def normalize_data(dataframe):
    """Normalize data by replacing None and NaN with pd.NA and casting everything to string."""
    logging.info("Normalizing data by converting all values to strings...")
    dataframe = dataframe.fillna(pd.NA).astype(str)
    return dataframe


def log_mismatches(file_data, db_data, file_path):
    """Log specific column mismatches with values from both file and database."""
    mismatches = []
    for column in file_data.columns:
        if not file_data[column].equals(db_data[column]):
            for idx, (file_value, db_value) in enumerate(zip(file_data[column], db_data[column])):
                if file_value != db_value:
                    mismatches.append(
                        f"Mismatch in column '{column}' at row {idx + 1}: File Value='{file_value}', DB Value='{db_value}'"
                    )
    if mismatches:
        logging.error(f"Mismatches found for file {file_path}:\n" + "\n".join(mismatches))
    return "FAILED" if mismatches else "PASSED"


def validate_data(file_data, db_data, file_path, db_details):
    """Validate file data against database data."""
    try:
        logging.info(f"Validating data for file: {file_path}...")
        
        # Drop specified columns
        file_data = file_data.drop(columns=db_details.get("skip_file_columns", []), errors="ignore")
        db_data = db_data.drop(columns=db_details.get("skip_db_columns", []), errors="ignore")

        # Convert boolean values
        db_data = convert_booleans(db_data)

        # Normalize data
        file_data = normalize_data(file_data)
        db_data = normalize_data(db_data)

        # Check if columns match
        if set(file_data.columns) != set(db_data.columns):
            logging.error(
                f"Column mismatch for file {file_path}. File columns: {file_data.columns}, DB columns: {db_data.columns}"
            )
            return "FAILED"

        # Normalize order of columns
        file_data = file_data.sort_index(axis=1)
        db_data = db_data.sort_index(axis=1)

        # Log mismatches
        return log_mismatches(file_data, db_data, file_path)
    except Exception as e:
        logging.error(f"Error validating data for {file_path}: {e}")
        return "FAILED"


def validate_file(file_path, db_details):
    """Validate a single file."""
    try:
        logging.info(f"Starting validation for file: {file_path}...")
        
        # Read file data
        file_data = pd.read_csv(file_path, dtype=str)
        logging.info(f"Loaded file {file_path} with {len(file_data)} rows.")

        # Connect to the database
        conn = connect_to_db(db_details["server"], db_details["database"])
        query = f"SELECT * FROM {db_details['schema']}.{db_details['table']}"
        db_data = pd.read_sql(query, conn)
        db_data = convert_booleans(db_data)
        db_data = db_data.astype(str)
        conn.close()
        logging.info(f"Loaded database data with {len(db_data)} rows.")

        # Perform validation
        validation_status = validate_data(file_data, db_data, file_path, db_details)

        # Log results
        logging.info(
            f"Validation result for file: {file_path} - Database: {db_details['database']}, "
            f"Schema: {db_details['schema']}, Table: {db_details['table']}, Status: {validation_status}"
        )
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")


def process_files(config_mapping, base_directory):
    """Process all files based on configuration and validate them."""
    logging.info(f"Starting to process files in directory: {base_directory}...")
    for dirpath, dirnames, filenames in
