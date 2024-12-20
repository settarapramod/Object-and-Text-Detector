import os

def get_all_files(root_dir, output_file, excluded_dirs=None, excluded_file_types=None):
    if excluded_dirs is None:
        excluded_dirs = []
    if excluded_file_types is None:
        excluded_file_types = []

    with open(output_file, 'w') as file:
        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Exclude specified directories
            dirnames[:] = [d for d in dirnames if os.path.join(dirpath, d) not in excluded_dirs]
            
            for filename in filenames:
                # Check if file type is excluded
                if any(filename.endswith(ext) for ext in excluded_file_types):
                    continue
                
                file_path = os.path.join(dirpath, filename)
                file.write(file_path + '\n')

# Example Usage
root_directory = "/path/to/root/directory"
output_file = "all_files.txt"
excluded_directories = ["/path/to/root/directory/exclude_dir1", "/path/to/root/directory/exclude_dir2"]
excluded_file_types = [".tmp", ".log"]

get_all_files(root_directory, output_file, excluded_dirs=excluded_directories, excluded_file_types=excluded_file_types)
print(f"Filtered file paths are saved in {output_file}.")
