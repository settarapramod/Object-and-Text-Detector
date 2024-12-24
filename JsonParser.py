import pandas as pd

def extract_datasets(json_obj, parent_key="root", datasets=None):
    if datasets is None:
        datasets = {}

    # Current level data
    current_data = {}
    for key, value in json_obj.items():
        if isinstance(value, dict):  # Embedded JSON
            new_key = f"{parent_key}_{key}"
            extract_datasets(value, new_key, datasets)
        elif isinstance(value, list):  # List of JSON objects
            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    new_key = f"{parent_key}_{key}_{idx}"
                    extract_datasets(item, new_key, datasets)
                else:
                    current_data[f"{key}_{idx}"] = item
        else:  # Simple key-value
            current_data[key] = value

    # Add current level data to datasets
    if current_data:
        if parent_key not in datasets:
            datasets[parent_key] = []
        datasets[parent_key].append(current_data)

    return datasets

# Convert datasets to Pandas DataFrames
def convert_to_dataframes(datasets):
    dataframes = {}
    for key, rows in datasets.items():
        dataframes[key] = pd.DataFrame(rows)
    return dataframes

# Example usage
import json

json_data = """
{
    "id": 1,
    "name": "Sample",
    "details": {
        "age": 25,
        "address": {
            "city": "New York",
            "zipcode": "10001"
        }
    },
    "tags": ["json", "pandas"],
    "metadata": [
        {"key": "color", "value": "blue"},
        {"key": "size", "value": "medium"}
    ]
}
"""

json_obj = json.loads(json_data)
datasets = extract_datasets(json_obj)
dataframes = convert_to_dataframes(datasets)

# Print datasets
for key, df in dataframes.items():
    print(f"Dataset: {key}")
    print(df)
