import pandas as pd


def write_json(
    dataframe: pd.DataFrame,
    path: str,
) -> None:
    dataframe.to_json(path, orient="records")


def read_json(
    path: str,
) -> pd.DataFrame:
    return pd.read_json(path)