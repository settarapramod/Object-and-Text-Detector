import os
import shutil
from itertools import islice

def chunked_iterator(iterable, chunk_size):
    """
    Yield successive chunks from an iterable.
    :param iterable: Iterable to chunk.
    :param chunk_size: Number of items in each chunk.
    """
    it = iter(iterable)
    while chunk := list(islice(it, chunk_size)):
        yield chunk

def move_files_by_pattern_in_chunks(base_dir, dest_dir, patterns, chunk_size=1000):
    """
    Moves files in chunks from the base directory to the destination directory based on naming patterns.

    :param base_dir: Path to the base directory containing the files.
    :param dest_dir: Path to the destination directory where files will be moved.
    :param patterns: Dictionary with patterns as keys and folder names as values.
    :param chunk_size: Number of files to process in one batch.
    """
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    all_files = [file for file in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, file))]
    
    for file_chunk in chunked_iterator(all_files, chunk_size):
        for file_name in file_chunk:
            file_path = os.path.join(base_dir, file_name)
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
    
    # Configurable chunk size
    chunk_size = 1000  # Adjust as needed
    
    move_files_by_pattern_in_chunks(base_directory, destination_directory, file_patterns, chunk_size)
