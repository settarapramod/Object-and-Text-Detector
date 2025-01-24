import pandas as pd
import json

# Function to flatten nested JSON and create datasets
def create_datasets_from_json(json_data, structure, id_config):
    datasets = {}

    def search_key_in_json(json_obj, key_to_find):
        """
        Search for the first occurrence of a key in the entire JSON object.
        """
        if isinstance(json_obj, dict):
            for key, value in json_obj.items():
                if key == key_to_find:
                    return value
                result = search_key_in_json(value, key_to_find)
                if result is not None:
                    return result
        elif isinstance(json_obj, list):
            for item in json_obj:
                result = search_key_in_json(item, key_to_find)
                if result is not None:
                    return result
        return None

    def process_node(node, parent_id, parent_key):
        if parent_key is None:  # Handle the root dataset separately
            table_name = "root"  # Explicitly use "root" as the table name
            if table_name in structure:
                columns = structure[table_name]
                rows = []
                print(f"Processing root dataset with columns: {columns}")  # Debug print

                # Assuming the root is a dictionary
                row = {}
                for col in columns:
                    row[col] = node.get(col, None)  # Extract values for the root dataset
                # Add ID column if configured
                if table_name in id_config:
                    id_info = id_config[table_name]
                    id_column = id_info["id_column"]
                    source_key = id_info["source"]
                    row[id_column] = node.get(source_key, None)  # Directly from root if available

                rows.append(row)

                # Add root rows to the dataset
                datasets[table_name] = pd.DataFrame(rows, columns=columns + [id_config[table_name]["id_column"]] if table_name in id_config else columns)
                print(f"Root dataset: {datasets[table_name]}")  # Debug print
                return  # No need to continue as we have processed the root dataset already

        for key, value in node.items():
            print(f"Processing node: {key}, parent_key: {parent_key}")  # Debug print
            table_name = f"{parent_key}_{key}" if parent_key else key
            print(f"Table Name: {table_name}")  # Debug print

            # If key is in the structure
            if table_name in structure:
                columns = structure[table_name]
                rows = []
                print(f"Columns: {columns}")  # Debug print

                if isinstance(value, list):
                    for item in value:
                        row = {}
                        for col in columns:
                            row[col] = item.get(col, None)
                        print(f"Row: {row}")  # Debug print for each row being created
                        # Add ID column if configured
                        if table_name in id_config:
                            id_info = id_config[table_name]
                            id_column = id_info["id_column"]
                            source_key = id_info["source"]

                            # Try to fetch the ID from the immediate parent
                            if source_key in node:
                                row[id_column] = node[source_key]
                            # If not in immediate parent, search entire JSON
                            else:
                                row[id_column] = search_key_in_json(json_data, source_key)

                        rows.append(row)

                elif isinstance(value, dict):
                    row = {}
                    for col in columns:
                        row[col] = value.get(col, None)
                    print(f"Row: {row}")  # Debug print for each row being created
                    # Add ID column if configured
                    if table_name in id_config:
                        id_info = id_config[table_name]
                        id_column = id_info["id_column"]
                        source_key = id_info["source"]

                        # Try to fetch the ID from the immediate parent
                        if source_key in node:
                            row[id_column] = node[source_key]
                        # If not in immediate parent, search entire JSON
                        else:
                            row[id_column] = search_key_in_json(json_data, source_key)

                    rows.append(row)

                # Add rows to the dataset
                if rows:
                    datasets[table_name] = pd.DataFrame(rows, columns=columns + [id_config[table_name]["id_column"]] if table_name in id_config else columns)
                    print(f"Dataset {table_name} created with rows: {rows}")  # Debug print

            # Process nested dictionaries or lists
            if isinstance(value, dict):
                process_node(value, parent_id, table_name)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        process_node(item, parent_id, table_name)

    # Start processing from the root node
    process_node(json_data, None, None)
    return datasets


# Predefined structure for datasets
structure = {
    "root": ["id", "name", "No"],
    "root_current_address": ["ΗΝΟ", "Floor", "Street", "State"],
    "root_Permanent_address": ["ΗΝΟ", "Floor", "Street", "District", "State"],
    "root_Projects_A": ["Name", "Stack", "Active"],
    "root_Projects_on_hold_A": ["Name", "Stack", "Time"],
    "root_Projects_on_hold_B": ["Name", "Stack", "Active"]
}

# ID configuration for each dataset
id_config = {
    "root": {"id_column": "ID", "source": "id"},
    "root_current_address": {"id_column": "ParentID", "source": "id"},
    "root_Permanent_address": {"id_column": "CustomID", "source": "Stack"},  # Example for global search
    "root_Projects_A": {"id_column": "ParentID", "source": "id"},
    "root_Projects_on_hold_A": {"id_column": "ProjectID", "source": "id"},
    "root_Projects_on_hold_B": {"id_column": "TaskID", "source": "Name"}
}

# Read JSON from a file
with open("test.json", "r") as file:
    json_data = json.load(file)

# Generate datasets
datasets = create_datasets_from_json(json_data, structure, id_config)

# Print the datasets
for name, df in datasets.items():
    print(f"Dataset: {name}")
    print(df)
    print("-" * 50)
