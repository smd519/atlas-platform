import pandas as pd


def write_parquet(
    dataframe: pd.DataFrame,
    path: str,
) -> None:
    dataframe.to_parquet(path, index=False)


def read_parquet(
    path: str,
) -> pd.DataFrame:
    return pd.read_parquet(path)