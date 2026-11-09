"""CSV format serialization and persistence helpers."""

from __future__ import annotations

import io
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from atlas_storage.exceptions import SerializationError


def _coerce_dataframe(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        return pd.DataFrame([data])
    raise SerializationError(f"Cannot serialize object of type '{type(data).__name__}' as CSV.")


def serialize_csv(data: Any) -> bytes:
    """Serialize ``data`` into UTF-8 encoded CSV bytes."""
    if data is None:
        raise SerializationError("Cannot serialize None payload as CSV.")
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8")
    try:
        dataframe = _coerce_dataframe(data)
        csv_text = dataframe.to_csv(index=False)
        return (csv_text or "").encode("utf-8")
    except SerializationError:
        raise
    except Exception as exc:
        raise SerializationError(f"Failed to serialize data to CSV: {exc}") from exc


def deserialize_csv(payload: bytes) -> pd.DataFrame:
    """Deserialize CSV ``payload`` bytes into a :class:`pandas.DataFrame`."""
    try:
        return pd.read_csv(io.BytesIO(payload))
    except Exception as exc:
        raise SerializationError(f"Failed to deserialize CSV bytes: {exc}") from exc


def write_csv(
    dataframe: pd.DataFrame,
    path: str,
) -> None:
    """Write ``dataframe`` directly to ``path`` as CSV."""
    try:
        dataframe.to_csv(path, index=False)
    except Exception as exc:
        raise SerializationError(f"Failed to write CSV to '{path}': {exc}") from exc


def read_csv(
    path: str,
) -> pd.DataFrame:
    """Read a CSV file at ``path`` into a :class:`pandas.DataFrame`."""
    try:
        return pd.read_csv(path)
    except Exception as exc:
        raise SerializationError(f"Failed to read CSV from '{path}': {exc}") from exc


__all__ = [
    "deserialize_csv",
    "read_csv",
    "serialize_csv",
    "write_csv",
]
