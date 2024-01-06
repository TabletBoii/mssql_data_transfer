import os


def get_data(key: str) -> any:
    return os.environ.get(key)
