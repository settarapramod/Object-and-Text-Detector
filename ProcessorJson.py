import pandas as pd
import json
from collections import defaultdict

def process_json(json_data, structure, id_config):
    datasets = defaultdict(list)

    def extract_data(obj, table_name, parent_data=None, parent_hierarchy=None):
        """Extract data from JSON objects and populate datasets."""
        if isinstance(obj, dict):
            dataset = {}
            # Initialize parent hierarchy for deeper traversal
            if parent_hierarchy is None:
                parent_hierarchy = []

            # Add ID from parent hierarchy if configured
            if parent_hierarchy and table_name in id_config:
                id_column = id_config[table_name]["id_column"]
                source_key = id_config[table_name]["source"]
                # Search in the parent hierarchy for the key
                for parent in reversed(parent_hierarchy):
                    if source_key in parent:
                        dataset[id_column] = parent[source_key]
                        break
                else:
                    dataset[id_column] = None  # Populate as None if not found

            for key, value in obj.items():
                if isinstance(value, (dict, list)):
                    nested_table_name = f"{table_name}_{key}"
                    extract_data(value, nested_table_name, obj, parent_hierarchy + [obj])
                elif key in structure.get(table_name, []):
                    dataset[key] = value

            if dataset:
                datasets[table_name].append(dataset)
        elif isinstance(obj, list):
            for item in obj:
                extract_data(item, table_name, parent_data, parent_hierarchy)

    # Start processing from the root level
    extract_data(json_data, "root")

    # Convert collected data to pandas DataFrames
    final_datasets = {}
    for table, rows in datasets.items():
        final_datasets[table] = pd.DataFrame(rows)

    return final_datasets


def main(json_file_path, structure, id_config):
    # Read JSON from file
    with open(json_file_path, 'r') as f:
        json_data = json.load(f)

    # Process the JSON
    datasets = process_json(json_data, structure, id_config)

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

# ID configuration for child tables with search in the parent hierarchy
id_config = {
    "root_current_address": {"id_column": "ParentID", "source": "id"},  # ID for child comes from "id" in the parent
    "root_Permanent_address": {"id_column": "ParentID", "source": "id"},
    "root_Projects_A": {"id_column": "ParentID", "source": "id"},  # If not found in immediate parent, search higher up
    "root_Projects_on_hold_A": {"id_column": "ParentID", "source": "id"},
    "root_Projects_on_hold_B": {"id_column": "ParentID", "source": "id"},
}

# Run the main function
if __name__ == "__main__":
    json_file_path = "test.json"  # Replace this with the path to your JSON file
    main(json_file_path, structure, id_config)
