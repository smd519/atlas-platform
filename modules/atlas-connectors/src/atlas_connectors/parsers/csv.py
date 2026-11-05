"""CSV file parser."""

from __future__ import annotations

import pandas as pd  # type: ignore[import-untyped]

from atlas_connectors.exceptions import ParserError


def parse_csv(path: str) -> pd.DataFrame:
    """Parse a CSV file at ``path`` into a :class:`pandas.DataFrame`."""
    try:
        return pd.read_csv(path)
    except Exception as exc:
        raise ParserError(f"Failed to parse CSV file '{path}': {exc}") from exc


__all__ = ["parse_csv"]
