import os


def return_root_path():
    folder_path = os.path.dirname(os.path.abspath(__file__))
    return folder_path
