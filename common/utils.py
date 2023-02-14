import json
from pathlib import Path


def read_json(file_path: str | Path) -> dict:
    with open(file_path, "r", encoding="utf-8") as json_file:
        return json.load(json_file)


def string_to_bytes(size):
    units = {"b": 1, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
    size = size.lower().strip()
    num = float(size[:-1])
    unit = size[-1]
    value_in_bytes = int(num * units[unit])
    return value_in_bytes
