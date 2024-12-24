import pandas as pd
import json

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

# Read JSON file and process
def process_json_file(file_path, root_id_field):
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
    
    # Print all datasets
    for key, df in dataframes.items():
        print(f"Dataset: {key}")
        print(df)
        print("\n")

# Example usage
file_path = "your_json_file.json"  # Replace with your JSON file path
process_json_file(file_path, root_id_field="A")
