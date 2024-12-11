import os
import shutil
from pathlib import Path

def replicate_and_delete_files(base_to_dest_map):
    """
    Replicate files from base directories to their respective destination directories
    and delete them from the base directory.

    :param base_to_dest_map: A dictionary where keys are base directories, and values are lists of destination directories.
    """
    for base_dir, dest_dirs in base_to_dest_map.items():
        if not os.path.exists(base_dir):
            print(f"Base directory '{base_dir}' does not exist. Skipping...")
            continue

        # Ensure destination directories exist
        for dest_dir in dest_dirs:
            os.makedirs(dest_dir, exist_ok=True)

        # Process each file in the base directory
        for file_name in os.listdir(base_dir):
            file_path = os.path.join(base_dir, file_name)
            
            if os.path.isfile(file_path):  # Ensure it's a file
                # Copy file to each destination directory
                for dest_dir in dest_dirs:
                    dest_path = os.path.join(dest_dir, file_name)
                    shutil.copy(file_path, dest_path)
                    print(f"Copied '{file_path}' to '{dest_path}'")

                # Delete the file from the base directory
                os.remove(file_path)
                print(f"Deleted '{file_path}' from base directory")

# Define the mapping of base directories to destination directories
base_to_dest_map = {
    "A": ["aa", "ab", "ac"],
    "B": ["ba", "bb", "bc"],
    "C": ["ca", "cb", "cc"],
    # Add more base directories and their destinations here
}

# Execute the replication and deletion process
replicate_and_delete_files(base_to_dest_map)
