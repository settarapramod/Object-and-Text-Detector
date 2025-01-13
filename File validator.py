import os
import logging
from datetime import datetime

def setup_logger(log_dir, log_filename_prefix):
    """Sets up a logger that logs to a file and console."""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{log_filename_prefix}_{timestamp}.log"
    log_path = os.path.join(log_dir, log_filename)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    return log_path

def setup_failed_logger(log_dir, log_filename_prefix):
    """Sets up a separate logger for failed validations."""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{log_filename_prefix}_{timestamp}.log"
    log_path = os.path.join(log_dir, log_filename)

    failed_logger = logging.getLogger("failed_logger")
    failed_logger.setLevel(logging.INFO)
    failed_handler = logging.FileHandler(log_path)
    failed_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    failed_logger.addHandler(failed_handler)

    return failed_logger, log_path

def validate_files_and_folders(file_list_path, log_dir, display_console=True):
    """Validates whether files and folders exist and logs the results."""
    setup_logger(log_dir, "validation_log")
    failed_logger, failed_log_path = setup_failed_logger(log_dir, "failed_log")

    logging.info("Validation Process Started")
    logging.info("=" * 100)

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
        logging.info(log_entry)

        if display_console:
            print(log_entry)

    if display_console:
        print("=" * 100)

    logging.info("=" * 100)
    logging.info("Validation Process Completed")

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
        logging.info("All files and folders exist. No failed log generated.")
        if display_console:
            print("\nAll files and folders exist. No failed log generated.")

# Example Usage
file_list_path = "all_files.txt"  # File containing the list of file/folder paths
log_dir = "logs"  # Directory to save logs
display_console_messages = True  # Set to False to suppress console messages
validate_files_and_folders(file_list_path, log_dir, display_console=display_console_messages)
