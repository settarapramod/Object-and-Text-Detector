import os
from datetime import datetime

def validate_files(file_list_path, log_file_path):
    with open(file_list_path, 'r') as file:
        file_paths = [line.strip() for line in file]

    log_lines = []
    log_lines.append(f"File Validation Start - {datetime.now()}\n")
    log_lines.append("=" * 50 + "\n")

    for file_path in file_paths:
        if os.path.exists(file_path):
            status = "PASSED"
        else:
            status = "FAILED"

        log_lines.append(f"File: {os.path.basename(file_path)} | Path: {file_path} | Status: {status}\n")
        print(f"File: {os.path.basename(file_path)} | Path: {file_path} | Status: {status}")

    log_lines.append("=" * 50 + "\n")
    log_lines.append(f"File Validation End - {datetime.now()}\n")

    # Write log to file
    with open(log_file_path, 'w') as log_file:
        log_file.writelines(log_lines)
    print(f"\nValidation log saved to {log_file_path}.")

# Example Usage
file_list_path = "all_files.txt"
log_file_path = "validation_log.txt"
validate_files(file_list_path, log_file_path)
