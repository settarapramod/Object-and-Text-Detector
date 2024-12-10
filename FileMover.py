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

def move_files_by_pattern_in_chunks(base_dir, pattern_to_dest, chunk_size=1000):
    """
    Moves files in chunks from the base directory to destinations based on naming patterns.

    :param base_dir: Path to the base directory containing the files.
    :param pattern_to_dest: Dictionary with patterns as keys and their destination paths as values.
    :param chunk_size: Number of files to process in one batch.
    """
    all_files = [file for file in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, file))]
    
    for file_chunk in chunked_iterator(all_files, chunk_size):
        for file_name in file_chunk:
            file_path = os.path.join(base_dir, file_name)
            for pattern, dest_path in pattern_to_dest.items():
                if file_name.startswith(pattern):
                    if not os.path.exists(dest_path):
                        os.makedirs(dest_path)
                    shutil.move(file_path, os.path.join(dest_path, file_name))
                    print(f"Moved {file_name} to {dest_path}")
                    break

# Example usage
if __name__ == "__main__":
    base_directory = "/path/to/base/directory"
    
    # Define patterns and their corresponding destination paths
    pattern_to_destination = {
        "abc": "/path/to/destination/abc",
        "bcd": "/path/to/destination/bcd",
        "ccd": "/path/to/destination/ccd"
    }
    
    # Configurable chunk size
    chunk_size = 1000  # Adjust as needed
    
    move_files_by_pattern_in_chunks(base_directory, pattern_to_destination, chunk_size)
