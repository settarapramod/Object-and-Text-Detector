import os
import pandas as pd
import pyodbc
import re
import logging


def setup_logging():
    """Set up the logging configuration."""
    logging.basicConfig(
        filename="validation_log.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.info("Logging setup complete.")


def connect_to_db(server, database):
    """Establish a database connection."""
    try:
        logging.info(f"Connecting to database: {database} on server: {server}")
        connection_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        conn = pyodbc.connect(connection_str)
        logging.info("Database connection successful.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        raise


def read_file(file_path):
    """Read data from a CSV file."""
    try:
        logging.info(f"Reading file: {file_path}")
        data = pd.read_csv(file_path, dtype=str)  # Read file data as strings
        logging.info(f"Successfully read file: {file_path} with {len(data)} rows.")
        return data
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        raise


def fetch_db_data(conn, schema, table):
    """Fetch data from the database."""
    try:
        query = f"SELECT * FROM {schema}.{table}"
        logging.info(f"Executing query: {query}")
        data = pd.read_sql(query, conn)
        logging.info(f"Fetched {len(data)} rows from database table: {schema}.{table}")
        return data
    except Exception as e:
        logging.error(f"Error fetching data from database table {schema}.{table}: {e}")
        raise


def convert_booleans(dataframe):
    """Convert boolean values in the DataFrame to integers (0 or 1)."""
    for column in dataframe.columns:
        if dataframe[column].dtype == "bool":
            dataframe[column] = dataframe[column].astype(int)
    return dataframe


def normalize_data(dataframe):
    """Normalize data by replacing None and NaN with pd.NA and casting everything to string."""
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
        # Drop specified columns from file and database data
        file_data = file_data.drop(columns=db_details.get("skip_file_columns", []), errors="ignore")
        db_data = db_data.drop(columns=db_details.get("skip_db_columns", []), errors="ignore")

        # Convert boolean values in database data to 0/1
        db_data = convert_booleans(db_data)

        # Normalize data
        file_data = normalize_data(file_data)
        db_data = normalize_data(db_data)

        # Check if columns match
        if set(file_data.columns) != set(db_data.columns):
            logging.error(f"Column mismatch for file {file_path}. File columns: {file_data.columns}, DB columns: {db_data.columns}")
            return "FAILED"

        # Normalize order of columns
        file_data = file_data.sort_index(axis=1)
        db_data = db_data.sort_index(axis=1)

        # Log mismatches and return validation result
        return log_mismatches(file_data, db_data, file_path)
    except Exception as e:
        logging.error(f"Error validating data for {file_path}: {e}")
        return "FAILED"


def process_file(file_path, db_details):
    """Process and validate a single file."""
    try:
        file_data = read_file(file_path)
        conn = connect_to_db(db_details["server"], db_details["database"])
        db_data = fetch_db_data(conn, db_details["schema"], db_details["table"])
        validation_status = validate_data(file_data, db_data, file_path, db_details)
        logging.info(
            f"File: {file_path}, DB: {db_details['database']}, Schema: {db_details['schema']}, "
            f"Table: {db_details['table']}, Status: {validation_status}"
        )
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")


def process_directory(base_directory, config_mapping):
    """Process all files in the base directory and validate them."""
    for dirpath, dirnames, filenames in os.walk(base_directory):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            for pattern, db_details in config_mapping.items():
                if re.match(pattern, filename):
                    logging.info(f"Processing file {file_path} with pattern {pattern}")
                    process_file(file_path, db_details)
                    break


def main(config_mapping, base_directory):
    """Main function to process files using the provided configuration."""
    setup_logging()
    try:
        logging.info("Starting file validation process.")
        process_directory(base_directory, config_mapping)
        logging.info("File validation process completed.")
    except Exception as e:
        logging.error(f"Critical error in main process: {e}")


if __name__ == "__main__":
    # Define the configuration mapping
    config_mapping = {
        r"abc_\w+_\w+\.csv": {
            "server": "your_server_name",
            "database": "your_database_name",
            "schema": "validation",
            "table": "abc",
            "skip_db_columns": ["created_at", "updated_at"],  # Columns to skip from the database
            "skip_file_columns": ["extra_col"],  # Columns to skip from the file
        },
        r"xyz_\d+\.csv": {
            "server": "another_server_name",
            "database": "another_database_name",
            "schema": "another_schema",
            "table": "xyz",
            "skip_db_columns": ["audit_col"],
            "skip_file_columns": ["ignore_col"],
        },
    }

    # Define the base directory containing the files
    base_directory = "path/to/your/base/directory"

    # Run the main function
    main(config_mapping, base_directory)
