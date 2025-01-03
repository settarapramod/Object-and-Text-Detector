import os
from datetime import datetime

def validate_files(file_list_path, validation_log_path, failed_log_path, display_console=True):
    with open(file_list_path, 'r') as file:
        file_paths = [line.strip() for line in file]

    validation_log = []
    failed_files_log = []

    # Start validation log
    validation_log.append(f"File Validation Start - {datetime.now()}\n")
    validation_log.append("=" * 100 + "\n")

    if display_console:
        print("=" * 100)
        print(f"{'FILE NAME':<30} | {'FILE PATH':<60} | {'STATUS':<10}")
        print("-" * 100)

    for file_path in file_paths:
        if os.path.exists(file_path):
            status = "PASSED"
        else:
            status = "FAILED"
            failed_files_log.append(file_path)  # Add failed file path to failed log

        file_name = os.path.basename(file_path)
        log_entry = f"{file_name:<30} | {file_path:<60} | {status:<10}\n"
        validation_log.append(log_entry)

        # Conditionally print to console
        if display_console:
            print(f"{file_name:<30} | {file_path:<60} | {status:<10}")

    if display_console:
        print("=" * 100)

    validation_log.append("=" * 100 + "\n")
    validation_log.append(f"File Validation End - {datetime.now()}\n")

    # Write validation log
    with open(validation_log_path, 'w') as log_file:
        log_file.writelines(validation_log)
    if display_console:
        print(f"Validation log saved to {validation_log_path}.")

    # Write failed files log if there are missing files
    if failed_files_log:
        with open(failed_log_path, 'w') as failed_log_file:
            failed_log_file.write(f"Failed File Validation - {datetime.now()}\n")
            failed_log_file.write("=" * 100 + "\n")
            for failed_file in failed_files_log:
                failed_log_file.write(f"{failed_file}\n")
            failed_log_file.write("=" * 100 + "\n")
        if display_console:
            print(f"Failed files log saved to {failed_log_path}.")
    else:
        if display_console:
            print("All files passed validation. No failed log generated.")

# Example Usage
file_list_path = "all_files.txt"
validation_log_path = "validation_log.txt"
failed_log_path = "failed_log.txt"
display_console_messages = True  # Set to False to suppress console messages
validate_files(file_list_path, validation_log_path, failed_log_path, display_console=display_console_messages)
