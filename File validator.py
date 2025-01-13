import os
import logging
from datetime import datetime

def setup_logger(log_dir, log_filename_prefix):
    """Sets up a logger that logs to a file and console."""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{log_filename_prefix}_{timestamp}.log"
    log_path = os.path.join(log_dir, log_filename)

    logger = logging.getLogger(log_filename_prefix)
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger, log_path

def validate_files_and_folders(file_list_path, log_dir, display_console=True):
    """Validates whether files and folders exist and logs results separately."""
    
    validation_logger, validation_log_path = setup_logger(log_dir, "validation_log")
    failed_logger, failed_log_path = setup_logger(log_dir, "failed_log")

    validation_logger.info("Validation Process Started")
    validation_logger.info("=" * 100)

    with open(file_list_path, 'r') as file:
        paths = [line.strip() for line in file]

    failed_entries = []

    if display_console:
        print("\n" + "=" * 100)
        print(f"{'TYPE':<10} | {'NAME':<30} | {'PATH':<60} | {'STATUS':<10}")
        print("-" * 100)

    for path in paths:
        if os.path.exists(path):
            status = "PASSED"
        else:
            status = "FAILED"
            failed_entries.append(path)

        entry_type = "Folder" if os.path.isdir(path) else "File"
        entry_name = os.path.basename(path)

        log_entry = f"{entry_type:<10} | {entry_name:<30} | {path:<60} | {status:<10}"
        validation_logger.info(log_entry)

        if display_console:
            print(log_entry)

    if display_console:
        print("=" * 100)

    validation_logger.info("=" * 100)
    validation_logger.info("Validation Process Completed")

    # Log failed entries separately
    if failed_entries:
        failed_logger.info("Failed Validations")
        failed_logger.info("=" * 100)
        for entry in failed_entries:
            failed_logger.info(f"FAILED: {entry}")
        failed_logger.info("=" * 100)

        if display_console:
            print(f"\nFailed entries log saved to {failed_log_path}")
    else:
        validation_logger.info("All files and folders exist. No failed log generated.")
        if display_console:
            print("\nAll files and folders exist. No failed log generated.")

# Example Usage
file_list_path = "all_files.txt"  # File containing the list of file/folder paths
log_dir = "logs"  # Directory to save logs
display_console_messages = True  # Set to False to suppress console messages
validate_files_and_folders(file_list_path, log_dir, display_console=display_console_messages)
