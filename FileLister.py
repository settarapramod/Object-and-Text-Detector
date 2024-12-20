import os

def get_all_files(root_dir, output_file, excluded_dir_names=None, excluded_file_types=None):
    if excluded_dir_names is None:
        excluded_dir_names = []
    if excluded_file_types is None:
        excluded_file_types = []

    with open(output_file, 'w') as file:
        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Exclude directories based on their names
            dirnames[:] = [d for d in dirnames if d not in excluded_dir_names]
            
            for filename in filenames:
                # Exclude files based on their extensions
                if any(filename.endswith(ext) for ext in excluded_file_types):
                    continue
                
                file_path = os.path.join(dirpath, filename)
                file.write(file_path + '\n')

# Example Usage
root_directory = "/path/to/root/directory"
output_file = "all_files.txt"
excluded_directory_names = ["temp", "backup"]  # Exclude directories named 'temp' and 'backup'
excluded_file_types = [".tmp", ".log"]        # Exclude file types '.tmp' and '.log'

get_all_files(root_directory, output_file, excluded_dir_names=excluded_directory_names, excluded_file_types=excluded_file_types)
print(f"Filtered file paths are saved in {output_file}.")
