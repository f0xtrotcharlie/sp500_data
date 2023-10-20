import os
import shutil

# Source folder path
source_folder = "C:/Users/Jonat/Documents/MEGAsync/MEGAsync/Github/sp500_data"

# Destination folder path
destination_folder = "X:/sp500_data"

# Check if the file exists in the copy_path
try: 
    if os.path.exists(source_folder):

        # Copy the entire contents of the source folder to the destination folder
        shutil.copy(source_folder, destination_folder)
        print(f"Contents of '{source_folder}' copied to '{destination_folder}' successfully.")

except shutil.Error as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")