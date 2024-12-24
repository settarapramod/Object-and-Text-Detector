import os
import pandas as pd
from pathlib import Path

def detect_delimiter(file_path):
    """Detects the delimiter of a CSV file."""
    with open(file_path, 'r') as file:
        line = file.readline()
        if ',' in line:
            return ','
        elif '\t' in line:
            return '\t'
        elif ';' in line:
            return ';'
        elif '|' in line:
            return '|'
        else:
            raise ValueError(f"Unknown delimiter in file: {file_path}")

def process_directory(base_directory):
    """Processes all CSV files in a directory structure."""
    datasets = {}  # Dictionary to store DataFrames
    for dirpath, dirnames, filenames in os.walk(base_directory):
        for filename in filenames:
            if filename.endswith('.csv'):
                file_path = os.path.join(dirpath, filename)
                try:
                    # Detect delimiter and read CSV
                    delimiter = detect_delimiter(file_path)
                    df = pd.read_csv(file_path, delimiter=delimiter)
                    datasets[file_path] = df
                    print(f"Processed: {file_path}")
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
    return datasets

# Specify your base directory
base_directory = "path/to/your/base/directory"

# Process the directory and get the datasets
datasets = process_directory(base_directory)

# Example: Access a specific DataFrame
for file_path, df in datasets.items():
    print(f"\nData from {file_path}:\n")
    print(df.head())
