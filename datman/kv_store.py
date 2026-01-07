##### Utility functions for key-value file handling
from pathlib import Path
from typing import Union

def load_kv(file_path : Union[str,Path], int_key: bool = False) -> dict:
    data = {}
    try:
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    key, value = line.split(":", 1)
                    if int_key:
                        key = int(key)
                    data[key] = value
    except Exception as e:
        raise RuntimeError(f"Error reading key-value file {file_path}: {e}")

    return data


def save_kv(file_path : Union[str,Path], data : dict) -> None:
    with open(file_path, "w") as f:
        for key, value in data.items():
            f.write(f"{key}:{value}\n")