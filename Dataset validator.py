import pandas as pd
import pyodbc
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Example datasets as a dictionary (key: dataset name, value: pandas DataFrame)
datasets = {
    'Dataset1': pd.DataFrame({'ID': [1, 2], 'Value': ['A', 'B']}),
    'Dataset2': pd.DataFrame({'ID': [3, 4], 'Value': ['C', 'D']})
}

# Mapping dictionary
mapping = {
    'Dataset1': {
        'server': 'Server1',
        'database': 'DB1',
        'schema': 'dbo',
        'table': 'Table1',
        'exclude_columns': []
    },
    'Dataset2': {
        'server': 'Server2',
        'database': 'DB2',
        'schema': 'dbo',
        'table': 'Table2',
        'exclude_columns': ['Value']
    }
}

def fetch_table_data(server, database, schema, table):
    """
    Fetch data from a table using the provided connection details.
    """
    conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str)
    query = f"SELECT * FROM {schema}.{table}"
    return pd.read_sql(query, conn)

def validate_data(dataset, table_data, exclude_columns):
    """
    Validate dataset vs. table data.
    """
    # Exclude specified columns
    if exclude_columns:
        dataset = dataset.drop(columns=exclude_columns, errors='ignore')
        table_data = table_data.drop(columns=exclude_columns, errors='ignore')

    # Check column names
    dataset_columns = set(dataset.columns)
    table_columns = set(table_data.columns)

    if dataset_columns != table_columns:
        missing_in_table = dataset_columns - table_columns
        extra_in_table = table_columns - dataset_columns
        logger.error(f"Column mismatch: Missing in table - {missing_in_table}, Extra in table - {extra_in_table}")
        return False

    # Check record count
    if len(dataset) != len(table_data):
        logger.error(f"Record count mismatch: Dataset has {len(dataset)} records, Table has {len(table_data)} records.")
        return False

    # Value-by-value comparison
    mismatched_rows = []
    for i, row in dataset.iterrows():
        row_dict = row.to_dict()
        if not any((table_data == row_dict).all(axis=1)):
            mismatched_rows.append(row_dict)

    if mismatched_rows:
        logger.error(f"Value mismatches found: {len(mismatched_rows)} rows do not match. Details: {mismatched_rows}")
        return False

    logger.info("Validation successful: Dataset matches table data.")
    return True

def main():
    for dataset_name, dataset in datasets.items():
        logger.info(f"Processing dataset: {dataset_name}")
        details = mapping.get(dataset_name)

        if not details:
            logger.error(f"No mapping details found for {dataset_name}")
            continue

        server = details['server']
        database = details['database']
        schema = details['schema']
        table = details['table']
        exclude_columns = details.get('exclude_columns', [])

        logger.info(f"Fetching data from {server}.{database}.{schema}.{table}")
        try:
            table_data = fetch_table_data(server, database, schema, table)
            logger.info(f"Data fetched successfully for {dataset_name}")
        except Exception as e:
            logger.error(f"Error fetching table data: {e}")
            continue

        logger.info(f"Validating dataset: {dataset_name}")
        validation_result = validate_data(dataset, table_data, exclude_columns)

        if validation_result:
            logger.info(f"Dataset {dataset_name} validation PASSED")
        else:
            logger.error(f"Dataset {dataset_name} validation FAILED")

if __name__ == "__main__":
    main()
