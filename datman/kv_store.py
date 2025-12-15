##### Utility functions for key-value file handling

def load_kv(file_path:str) -> dict:
    data = {}
    try:
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    key, value = line.split(":", 1)
                    data[key] = value
    except Exception as e:
        raise RuntimeError(f"Error reading key-value file {file_path}: {e}")

    return data


def save_kv(file_path:str, data:dict) -> None:
    with open(file_path, "w") as f:
        for key, value in data.items():
            f.write(f"{key}:{value}\n")