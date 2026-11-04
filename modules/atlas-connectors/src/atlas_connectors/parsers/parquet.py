"""Parquet file parser."""

from __future__ import annotations

import pandas as pd  # type: ignore[import-untyped]

from atlas_connectors.exceptions import ParserError


def parse_parquet(path: str) -> pd.DataFrame:
    """Parse a Parquet file at ``path`` into a :class:`pandas.DataFrame`."""
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        raise ParserError(f"Failed to parse Parquet file '{path}': {exc}") from exc


__all__ = ["parse_parquet"]
