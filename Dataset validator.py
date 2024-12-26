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
    if exclude_columns:
        dataset = dataset.drop(columns=exclude_columns, errors='ignore')
        table_data = table_data.drop(columns=exclude_columns, errors='ignore')

    if dataset.shape != table_data.shape:
        logger.warning("Mismatch in shape between dataset and table data.")
        return False

    # Value-by-value comparison
    for _, row in dataset.iterrows():
        if not any((table_data == row.to_dict()).all(axis=1)):
            logger.error(f"Row mismatch found: {row.to_dict()}")
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
