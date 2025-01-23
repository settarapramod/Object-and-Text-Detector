import pandas as pd
import json
import pyodbc

# Function to extract datasets
def extract_datasets(json_obj, structure, id_config, parent_key="root", datasets=None, root_fields=None):
    if datasets is None:
        datasets = {}
    if root_fields is None:
        root_fields = {}

    current_data = {}
    for key, value in json_obj.items():
        if isinstance(value, dict):  # Embedded JSON
            new_key = f"{parent_key}_{key}"
            extract_datasets(value, structure, id_config, new_key, datasets, root_fields)
        elif isinstance(value, list):  # List of JSON objects
            new_key = f"{parent_key}_{key}"
            for item in value:
                if isinstance(item, dict):  # Handle list of dicts
                    extract_datasets(item, structure, id_config, new_key, datasets, root_fields)
                else:  # Handle simple lists
                    if new_key not in datasets:
                        datasets[new_key] = []
                    datasets[new_key].append({**root_fields, "value": item})
        else:  # Simple key-value
            current_data[key] = value

    # Add current level data to datasets
    if current_data:
        # Ensure dataset matches the predefined structure
        if parent_key in structure:
            defined_columns = structure[parent_key]
            # Filter and add missing columns
            filtered_data = {col: current_data.get(col, None) for col in defined_columns}

            # Add ID column based on configuration
            id_column_name = id_config.get(parent_key, {}).get("id_column", "ID")
            id_source = id_config.get(parent_key, {}).get("source", None)
            if id_source:
                filtered_data[id_column_name] = root_fields.get(id_source, None)
            else:
                filtered_data[id_column_name] = None

            # Append to the dataset
            if parent_key not in datasets:
                datasets[parent_key] = []
            datasets[parent_key].append(filtered_data)

    return datasets

# Function to convert datasets to Pandas DataFrames
def convert_to_dataframes(datasets):
    dataframes = {}
    for key, rows in datasets.items():
        dataframes[key] = pd.DataFrame(rows)
    return dataframes

# Function to validate datasets against SQL tables
def validate_datasets(dataframes, mapping, connection_string):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    for dataset_name, dataframe in dataframes.items():
        if dataset_name not in mapping:
            print(f"No mapping found for dataset: {dataset_name}")
            continue

        table_info = mapping[dataset_name]
        table_name = table_info["table_name"]
        database_name = table_info["database_name"]
        audit_columns = table_info["audit_columns"]
        key_columns = table_info["key_columns"]

        # Exclude audit columns
        df_columns = [col for col in dataframe.columns if col not in audit_columns]
        dataframe = dataframe[df_columns]

        # Generate SQL query
        query = f"SELECT {', '.join(df_columns)} FROM {database_name}.dbo.{table_name}"
        sql_table_data = pd.read_sql_query(query, conn)

        # Validate record by record
        print(f"Validating dataset: {dataset_name}")
        mismatches = dataframe.merge(
            sql_table_data,
            how="outer",
            on=key_columns,
            indicator=True
        ).query("_merge != 'both'")

        if mismatches.empty:
            print("Validation successful: All records match!")
        else:
            print("Validation failed: Mismatched records found.")
            print(mismatches)

    conn.close()

# Read JSON file and process
def process_json_file(file_path, structure, id_config, connection_string):
    with open(file_path, 'r') as file:
        json_data = json.load(file)

    # Prepare datasets
    if isinstance(json_data, list):  # Handle list of JSON objects
        combined_datasets = {}
        for item in json_data:
            datasets = extract_datasets(item, structure, id_config)
            for key, rows in datasets.items():
                if key not in combined_datasets:
                    combined_datasets[key] = []
                combined_datasets[key].extend(rows)
        datasets = combined_datasets
    else:  # Single JSON object
        datasets = extract_datasets(json_data, structure, id_config)

    # Convert to Pandas DataFrames
    dataframes = convert_to_dataframes(datasets)

    # Validate datasets against SQL tables
    validate_datasets(dataframes, structure, connection_string)

# Example usage
file_path = "your_json_file.json"  # Replace with your JSON file path

# Predefined structure for datasets
structure = {
    "root": ["A", "B"],
    "root_C_d": ["y", "ID"]
}

# ID configuration for each dataset
id_config = {
    "root": {"id_column": "ID", "source": "A"},  # ID for root comes from "A"
    "root_C_d": {"id_column": "ParentID", "source": "A"}  # ID for nested table comes from "A"
}

# SQL connection string
connection_string = "Driver={SQL Server};Server=YOUR_SERVER;Database=TestDB;Trusted_Connection=yes;"

process_json_file(file_path, structure, id_config, connection_string)
