"""Format serializers and deserializers for atlas-storage."""

from __future__ import annotations

from typing import Any

from atlas_storage.exceptions import SerializationError
from atlas_storage.formats.csv import (
    deserialize_csv,
    read_csv,
    serialize_csv,
    write_csv,
)
from atlas_storage.formats.json import (
    deserialize_json,
    read_json,
    serialize_json,
    write_json,
)
from atlas_storage.formats.parquet import (
    deserialize_parquet,
    read_parquet,
    serialize_parquet,
    write_parquet,
)

SUPPORTED_STORAGE_FORMATS: frozenset[str] = frozenset({"csv", "json", "parquet", "raw", "bytes"})


def serialize_data(data: Any, fmt: str) -> bytes:
    """Serialize ``data`` into ``bytes`` according to storage format ``fmt``."""
    if data is None:
        raise SerializationError("Cannot serialize None data payload.")

    normalized = fmt.strip().lower() if isinstance(fmt, str) else ""
    if normalized == "csv":
        return serialize_csv(data)
    if normalized == "json":
        return serialize_json(data)
    if normalized == "parquet":
        return serialize_parquet(data)
    if normalized in {"raw", "bytes"}:
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        if isinstance(data, str):
            return data.encode("utf-8")
        raise SerializationError(
            f"Raw byte storage format requires bytes or str, got '{type(data).__name__}'."
        )

    raise SerializationError(
        f"Unsupported storage format '{fmt}'. "
        f"Supported formats: {sorted(SUPPORTED_STORAGE_FORMATS)}"
    )


def deserialize_data(payload: bytes, fmt: str) -> Any:
    """Deserialize raw ``payload`` bytes into the domain structure for ``fmt``."""
    if not isinstance(payload, (bytes, bytearray)):
        raise SerializationError(
            f"Expected bytes payload from storage backend, got '{type(payload).__name__}'."
        )

    normalized = fmt.strip().lower() if isinstance(fmt, str) else ""
    raw = bytes(payload)
    if normalized == "csv":
        return deserialize_csv(raw)
    if normalized == "json":
        return deserialize_json(raw)
    if normalized == "parquet":
        return deserialize_parquet(raw)
    if normalized in {"raw", "bytes"}:
        return raw

    raise SerializationError(
        f"Unsupported storage format '{fmt}'. "
        f"Supported formats: {sorted(SUPPORTED_STORAGE_FORMATS)}"
    )


__all__ = [
    "SUPPORTED_STORAGE_FORMATS",
    "deserialize_csv",
    "deserialize_data",
    "deserialize_json",
    "deserialize_parquet",
    "read_csv",
    "read_json",
    "read_parquet",
    "serialize_csv",
    "serialize_data",
    "serialize_json",
    "serialize_parquet",
    "write_csv",
    "write_json",
    "write_parquet",
]
