"""Parquet format serialization and persistence helpers."""

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
    raise SerializationError(f"Cannot serialize object of type '{type(data).__name__}' as Parquet.")


def serialize_parquet(data: Any) -> bytes:
    """Serialize ``data`` into Parquet binary bytes."""
    if data is None:
        raise SerializationError("Cannot serialize None payload as Parquet.")
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    try:
        dataframe = _coerce_dataframe(data)
        buffer = io.BytesIO()
        dataframe.to_parquet(buffer, index=False)
        return buffer.getvalue()
    except SerializationError:
        raise
    except Exception as exc:
        raise SerializationError(f"Failed to serialize data to Parquet: {exc}") from exc


def deserialize_parquet(payload: bytes) -> pd.DataFrame:
    """Deserialize Parquet ``payload`` bytes into a :class:`pandas.DataFrame`."""
    try:
        return pd.read_parquet(io.BytesIO(payload))
    except Exception as exc:
        raise SerializationError(f"Failed to deserialize Parquet bytes: {exc}") from exc


def write_parquet(
    dataframe: pd.DataFrame,
    path: str,
) -> None:
    """Write ``dataframe`` directly to ``path`` as Parquet."""
    try:
        dataframe.to_parquet(path, index=False)
    except Exception as exc:
        raise SerializationError(f"Failed to write Parquet to '{path}': {exc}") from exc


def read_parquet(
    path: str,
) -> pd.DataFrame:
    """Read a Parquet file at ``path`` into a :class:`pandas.DataFrame`."""
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        raise SerializationError(f"Failed to read Parquet from '{path}': {exc}") from exc


__all__ = [
    "deserialize_parquet",
    "read_parquet",
    "serialize_parquet",
    "write_parquet",
]
