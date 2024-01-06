import os
from utils import return_root_path


if __name__ == "__main__":
    root_path = return_root_path()
    logs_folder_path = os.path.join(root_path, "logs")
    logs_file_number = 0
    for log_file in os.listdir(logs_folder_path):
        logs_file_number += 1

    if logs_file_number > 10:
        for log_file in os.listdir(logs_folder_path):
            print(os.path.join(logs_folder_path, log_file))
            os.remove(os.path.join(logs_folder_path, log_file))
