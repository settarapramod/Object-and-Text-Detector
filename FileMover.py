import os
import shutil

def move_files_by_pattern(base_dir, dest_dir, patterns):
    """
    Moves files from the base directory to the destination directory based on naming patterns.

    :param base_dir: Path to the base directory containing the files.
    :param dest_dir: Path to the destination directory where files will be moved.
    :param patterns: Dictionary with patterns as keys and folder names as values.
    """
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    for file_name in os.listdir(base_dir):
        file_path = os.path.join(base_dir, file_name)
        if os.path.isfile(file_path):
            for pattern, folder_name in patterns.items():
                if file_name.startswith(pattern):
                    target_folder = os.path.join(dest_dir, folder_name)
                    if not os.path.exists(target_folder):
                        os.makedirs(target_folder)
                    shutil.move(file_path, os.path.join(target_folder, file_name))
                    print(f"Moved {file_name} to {target_folder}")
                    break

# Example usage
if __name__ == "__main__":
    base_directory = "/path/to/base/directory"
    destination_directory = "/path/to/destination/directory"
    
    # Define patterns and their corresponding folder names
    file_patterns = {
        "abc": "abc",
        "bcd": "bcd",
        "ccd": "ccd"
    }
    
    move_files_by_pattern(base_directory, destination_directory, file_patterns)
