import pandas as pd


def parse_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)