import os

def validate_files(file_list_path):
    with open(file_list_path, 'r') as file:
        file_paths = [line.strip() for line in file]

    missing_files = []
    for file_path in file_paths:
        if not os.path.exists(file_path):
            missing_files.append(file_path)

    if missing_files:
        print("The following files are missing:")
        for missing_file in missing_files:
            print(missing_file)
    else:
        print("All files are present.")

# Example Usage
file_list_path = "all_files.txt"
validate_files(file_list_path)
