import pandas as pd
import json
import pyodbc  # For SQL Server connection

# Function to extract datasets
def extract_datasets(json_obj, parent_key="root", datasets=None, root_fields=None):
    if datasets is None:
        datasets = {}
    if root_fields is None:
        root_fields = {}

    current_data = {}
    for key, value in json_obj.items():
        if isinstance(value, dict):  # Embedded JSON
            new_key = f"{parent_key}_{key}"
            extract_datasets(value, new_key, datasets, root_fields)
        elif isinstance(value, list):  # List of JSON objects
            new_key = f"{parent_key}_{key}"
            for item in value:
                if isinstance(item, dict):  # Handle list of dicts
                    extract_datasets(item, new_key, datasets, root_fields)
                else:  # Handle simple lists
                    if new_key not in datasets:
                        datasets[new_key] = []
                    datasets[new_key].append({**root_fields, "value": item})
        else:  # Simple key-value
            current_data[key] = value

    # Add current level data to datasets
    if current_data:
        if parent_key not in datasets:
            datasets[parent_key] = []
        datasets[parent_key].append({**root_fields, **current_data})

    return datasets

# Function to convert datasets to Pandas DataFrames
def convert_to_dataframes(datasets):
    dataframes = {}
    for key, rows in datasets.items():
        dataframes[key] = pd.DataFrame(rows)
    return dataframes

# Function to create mapping object
def create_mapping_object(dataframes, table_metadata):
    mapping = {}
    for key in dataframes.keys():
        if key in table_metadata:
            mapping[key] = {
                "table_name": table_metadata[key]["table_name"],
                "database_name": table_metadata[key]["database_name"],
                "audit_columns": table_metadata[key].get("audit_columns", []),
                "key_columns": table_metadata[key]["key_columns"]
            }
    return mapping

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
def process_json_file(file_path, root_id_field, table_metadata, connection_string):
    with open(file_path, 'r') as file:
        json_data = json.load(file)
    
    # Prepare datasets
    if isinstance(json_data, list):  # Handle list of JSON objects
        combined_datasets = {}
        for item in json_data:
            root_fields = {root_id_field: item.get(root_id_field)}
            datasets = extract_datasets(item, root_fields=root_fields)
            for key, rows in datasets.items():
                if key not in combined_datasets:
                    combined_datasets[key] = []
                combined_datasets[key].extend(rows)
        datasets = combined_datasets
    else:  # Single JSON object
        root_fields = {root_id_field: json_data.get(root_id_field)}
        datasets = extract_datasets(json_data, root_fields=root_fields)
    
    # Convert to Pandas DataFrames
    dataframes = convert_to_dataframes(datasets)
    
    # Create mapping object
    mapping = create_mapping_object(dataframes, table_metadata)
    
    # Validate datasets against SQL tables
    validate_datasets(dataframes, mapping, connection_string)

# Example usage
file_path = "your_json_file.json"  # Replace with your JSON file path
table_metadata = {
    "root": {
        "table_name": "RootTable",
        "database_name": "TestDB",
        "audit_columns": ["created_at", "updated_at"],
        "key_columns": ["A"]
    },
    "root_C_d": {
        "table_name": "CDTable",
        "database_name": "TestDB",
        "audit_columns": ["created_at", "updated_at"],
        "key_columns": ["A", "y"]
    }
}
connection_string = "Driver={SQL Server};Server=YOUR_SERVER;Database=TestDB;Trusted_Connection=yes;"
process_json_file(file_path, root_id_field="A", table_metadata=table_metadata, connection_string=connection_string)
