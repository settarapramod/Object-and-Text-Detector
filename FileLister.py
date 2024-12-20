import os

def get_all_files(root_dir, output_file):
    with open(output_file, 'w') as file:
        for dirpath, _, filenames in os.walk(root_dir):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                file.write(file_path + '\n')

# Example Usage
root_directory = "/path/to/root/directory"
output_file = "all_files.txt"
get_all_files(root_directory, output_file)
print(f"All file paths are saved in {output_file}.")
