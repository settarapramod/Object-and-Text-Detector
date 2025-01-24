import pandas as pd
import json
from collections import defaultdict

def process_json(json_data, structure):
    datasets = defaultdict(list)

    def extract_data(obj, table_name):
        """Extract data from JSON objects and populate datasets."""
        if isinstance(obj, dict):
            dataset = {}
            for key, value in obj.items():
                if isinstance(value, (dict, list)):
                    nested_table_name = f"{table_name}_{key}"
                    extract_data(value, nested_table_name)
                elif key in structure.get(table_name, []):
                    dataset[key] = value
            if dataset:
                datasets[table_name].append(dataset)
        elif isinstance(obj, list):
            for item in obj:
                extract_data(item, table_name)

    # Start processing from the root level
    extract_data(json_data, "root")

    # Convert collected data to pandas DataFrames
    final_datasets = {}
    for table, rows in datasets.items():
        final_datasets[table] = pd.DataFrame(rows)

    return final_datasets


def main(json_file_path, structure):
    # Read JSON from file
    with open(json_file_path, 'r') as f:
        json_data = json.load(f)

    # Process the JSON
    datasets = process_json(json_data, structure)

    # Print the resulting datasets
    for table, df in datasets.items():
        print(f"Dataset: {table}")
        print(df)
        print("-" * 50)


# Predefined structure dictionary
structure = {
    "root": ["id", "name", "No"],
    "root_current_address": ["HNO", "Floor", "Street", "State"],
    "root_Permanent_address": ["HNO", "Floor", "Street", "District", "State"],
    "root_Projects_A": ["Name", "Stack", "Time", "Active"],
    "root_Projects_on_hold_A": ["Name", "Stack", "Time", "Active"],
    "root_Projects_on_hold_B": ["Name", "Stack", "Active"],
}

# Run the main function
if __name__ == "__main__":
    json_file_path = "test.json"  # Replace this with the path to your JSON file
    main(json_file_path, structure)
