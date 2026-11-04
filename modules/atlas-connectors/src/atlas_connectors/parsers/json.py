import pandas as pd


def parse_json(path: str) -> pd.DataFrame:
    return pd.read_json(path)