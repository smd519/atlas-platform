import pandas as pd


def parse_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)