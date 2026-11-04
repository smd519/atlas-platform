import pandas as pd


def write_csv(
    dataframe: pd.DataFrame,
    path: str,
) -> None:
    dataframe.to_csv(path, index=False)


def read_csv(
    path: str,
) -> pd.DataFrame:
    return pd.read_csv(path)