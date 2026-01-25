import pandas as pd
from pathlib import Path


def extract_data(raw_path: str) -> dict:
    """
    Read all CSV files from the raw data folder
    and return them as a dictionary of DataFrames.
    """
    data = {}

    for file in Path(raw_path).glob("*.csv"):
        data[file.stem] = pd.read_csv(
            file, 
            encoding="utf-8", 
            on_bad_lines='skip'
        )

    return data


if __name__ == "__main__":
    data = extract_data("../data/raw")