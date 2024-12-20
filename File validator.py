import os
import logging
from datetime import datetime

def setup_logger(log_dir, log_filename_prefix):
    # Ensure the log directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Generate a log filename with a timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{log_filename_prefix}_{timestamp}.log"
    log_path = os.path.join(log_dir, log_filename)

    # Configure the logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()  # Also log to the console
        ]
    )
    return log_path

def validate_files(file_list_path, log_dir, display_console=True):
    # Set up the logger
    validation_log_path = setup_logger(log_dir, "validation_log")
    failed_log_path = setup_logger(log_dir, "failed_log")

    logging.info("File Validation Start")
    logging.info("=" * 100)

    # Read the file paths
    with open(file_list_path, 'r') as file:
        file_paths = [line.strip() for line in file]

    failed_files = []

    if display_console:
        # Print a formatted header for the console log
        print("\n" + "=" * 100)
        print(f"{'FILE NAME':<30} | {'FILE PATH':<60} | {'STATUS':<10}")
        print("-" * 100)

    for file_path in file_paths:
        # Determine if the file exists
        if os.path.exists(file_path):
            status = "PASSED"
        else:
            status = "FAILED"
            failed_files.append(file_path)

        # Extract the file name for logging and console display
        file_name = os.path.basename(file_path)

        # Format the log entry
        log_entry = f"{file_name:<30} | {file_path:<60} | {status:<10}"
        logging.info(log_entry)

        # Print the entry to the console if enabled
        if display_console:
            print(f"{file_name:<30} | {file_path:<60} | {status:<10}")

    if display_console:
        print("=" * 100)

    logging.info("=" * 100)
    logging.info("File Validation End")

    # Log and display failed files
    if failed_files:
        logging.info("Failed File Validation Start")
        logging.info("=" * 100)
        for failed_file in failed_files:
            logging.info(f"FAILED: {failed_file}")
        logging.info("=" * 100)
        logging.info("Failed File Validation End")
        if display_console:
            print(f"\nFailed files log saved to {failed_log_path}")
    else:
        logging.info("All files passed validation. No failed log generated.")
        if display_console:
            print("\nAll files passed validation. No failed log generated.")

# Example Usage
file_list_path = "all_files.txt"  # File containing the list of file paths
log_dir = "logs"  # Directory to save logs
display_console_messages = True  # Set to False to suppress console messages
validate_files(file_list_path, log_dir, display_console=display_console_messages)
