"""JSON format serialization and persistence helpers."""

from __future__ import annotations

import io
import json
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from atlas_storage.exceptions import SerializationError


def serialize_json(data: Any) -> bytes:
    """Serialize ``data`` into UTF-8 encoded JSON bytes."""
    if data is None:
        raise SerializationError("Cannot serialize None payload as JSON.")
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8")
    try:
        if isinstance(data, pd.DataFrame):
            json_str = data.to_json(orient="records")
            return (json_str or "[]").encode("utf-8")
        if isinstance(data, (list, dict)):
            return json.dumps(data).encode("utf-8")
        raise TypeError(f"Unsupported type '{type(data).__name__}' for JSON serialization.")
    except SerializationError:
        raise
    except Exception as exc:
        raise SerializationError(f"Failed to serialize data to JSON: {exc}") from exc


def deserialize_json(payload: bytes) -> pd.DataFrame:
    """Deserialize JSON ``payload`` bytes into a :class:`pandas.DataFrame`."""
    try:
        parsed = json.loads(payload.decode("utf-8"))
        if isinstance(parsed, dict):
            return pd.DataFrame([parsed])
        if isinstance(parsed, list):
            return pd.DataFrame(parsed)
        return pd.read_json(io.BytesIO(payload))
    except Exception as exc:
        raise SerializationError(f"Failed to deserialize JSON bytes: {exc}") from exc


def write_json(
    dataframe: pd.DataFrame,
    path: str,
) -> None:
    """Write ``dataframe`` directly to ``path`` as record-oriented JSON."""
    try:
        dataframe.to_json(path, orient="records")
    except Exception as exc:
        raise SerializationError(f"Failed to write JSON to '{path}': {exc}") from exc


def read_json(
    path: str,
) -> pd.DataFrame:
    """Read a JSON file at ``path`` into a :class:`pandas.DataFrame`."""
    try:
        return pd.read_json(path)
    except Exception as exc:
        raise SerializationError(f"Failed to read JSON from '{path}': {exc}") from exc


__all__ = [
    "deserialize_json",
    "read_json",
    "serialize_json",
    "write_json",
]
