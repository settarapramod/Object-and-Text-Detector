import pandas as pd
import json
import pyodbc


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
                    # Pass current_data as the new parent fields for nested objects
                    extract_datasets(item, structure, id_config, new_key, datasets, current_data)
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
                # Fetch ID from the immediate parent fields
                filtered_data[id_column_name] = root_fields.get(id_source, None)
            else:
                filtered_data[id_column_name] = None

            # Append to the dataset
            if parent_key not in datasets:
                datasets[parent_key] = []
            datasets[parent_key].append(filtered_data)

    return datasets


def convert_to_dataframes(datasets):
    dataframes = {}
    for key, rows in datasets.items():
        dataframes[key] = pd.DataFrame(rows)
    return dataframes


def process_json_file(file_path, structure, id_config):
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

    # Print results
    for name, df in dataframes.items():
        print(f"Dataset: {name}")
        print(df)
        print("-" * 50)


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
    "root_C_d": {"id_column": "ParentID", "source": "A"}  # ID for nested table comes from immediate parent
}

process_json_file(file_path, structure, id_config)
