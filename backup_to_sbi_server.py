import os
import shutil

def copy_files(source_dir, destination_dir):
    # Create the destination directory if it doesn't exist
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    # List files in the source directory
    source_files = os.listdir(source_dir)

    for file_name in source_files:
        source_path = os.path.join(source_dir, file_name)
        destination_path = os.path.join(destination_dir, file_name)

        try:
            # Attempt to copy the file
            shutil.copy2(source_path, destination_path)
            print(f"Copied: {file_name}")
        except PermissionError as e:
            # Handle permission errors gracefully
            print(f"Skipped: {file_name} (Permission denied)")
        except Exception as e:
            # Handle other exceptions
            print(f"Skipped: {file_name} ({str(e)})")

if __name__ == "__main__":
    source_directory = r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\sp500_data"
    destination_directory = r"X:\sp500_data"

    copy_files(source_directory, destination_directory)
