"""Unit tests for atlas-storage serializers, manager, registry, and error handling."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd  # type: ignore[import-untyped]
import pytest
from atlas_core import StorageProvider
from atlas_storage import (
    FileSystemBackend,
    ObjectStorageBackend,
    PartitioningError,
    PartitionStrategy,
    SerializationError,
    StorageConfigurationError,
    StorageLocation,
    StorageManager,
    StorageReadError,
    StorageRegistry,
    StorageWriteError,
)


class _InMemoryBackend(StorageProvider):
    def __init__(self, *, fail_save: bool = False, fail_load: bool = False) -> None:
        self.blobs: dict[str, bytes] = {}
        self._fail_save = fail_save
        self._fail_load = fail_load

    def save(self, data: Any, path: str) -> None:
        if self._fail_save:
            raise OSError("disk I/O failure")
        assert isinstance(data, bytes)
        self.blobs[path] = data

    def load(self, path: str) -> bytes:
        if self._fail_load:
            raise OSError("read I/O failure")
        if path not in self.blobs:
            raise StorageReadError(f"Missing key: {path}")
        return self.blobs[path]

    def exists(self, path: str) -> bool:
        return path in self.blobs


def test_storage_manager_dataframe_to_csv_and_symmetrical_load() -> None:
    backend = _InMemoryBackend()
    manager = StorageManager(backend=backend)
    df = pd.DataFrame([{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}])
    loc = StorageLocation(layer="bronze", path="mem/items.csv", format="csv")

    res = manager.save(df, loc)
    assert res.success is True
    assert res.row_count == 2
    assert manager.exists(loc) is True

    loaded = manager.load(loc)
    assert isinstance(loaded, pd.DataFrame)
    assert len(loaded) == 2
    assert list(loaded["name"]) == ["alpha", "beta"]


def test_storage_manager_dataframe_to_parquet_and_symmetrical_load() -> None:
    backend = _InMemoryBackend()
    manager = StorageManager(backend=backend)
    df = pd.DataFrame([{"station": "S1", "temp": 20.5}, {"station": "S2", "temp": 14.0}])
    loc = StorageLocation(layer="bronze", path="mem/weather.parquet", format="parquet")

    res = manager.save(df, loc)
    assert res.success is True
    assert res.row_count == 2

    loaded = manager.load(loc)
    assert isinstance(loaded, pd.DataFrame)
    assert len(loaded) == 2
    assert list(loaded["station"]) == ["S1", "S2"]


def test_storage_manager_dict_and_list_to_json_and_symmetrical_load() -> None:
    backend = _InMemoryBackend()
    manager = StorageManager(backend=backend)
    records = [{"k": "v1", "count": 10}, {"k": "v2", "count": 20}]
    loc = StorageLocation(layer="bronze", path="mem/records.json", format="json")

    res = manager.save(records, loc)
    assert res.success is True
    assert res.row_count == 2

    loaded = manager.load(loc)
    assert isinstance(loaded, pd.DataFrame)
    assert len(loaded) == 2
    assert list(loaded["k"]) == ["v1", "v2"]

    single_dict_loc = StorageLocation(layer="bronze", path="mem/single.json", format="json")
    manager.save({"k": "only", "count": 1}, single_dict_loc)
    loaded_single = manager.load(single_dict_loc)
    assert len(loaded_single) == 1
    assert loaded_single.iloc[0]["k"] == "only"


def test_storage_manager_raw_bytes_persistence_and_load() -> None:
    backend = _InMemoryBackend()
    manager = StorageManager(backend=backend)
    payload = b"\x00\x01\x02raw-binary-payload"
    loc = StorageLocation(layer="bronze", path="mem/blob.bin", format="raw")

    res = manager.save(payload, loc)
    assert res.success is True
    assert res.row_count == 1

    loaded = manager.load(loc)
    assert loaded == payload


def test_storage_manager_rejects_unsupported_formats() -> None:
    backend = _InMemoryBackend()
    manager = StorageManager(backend=backend)
    loc = StorageLocation(layer="bronze", path="mem/file.orc", format="orc")

    with pytest.raises(SerializationError, match="Unsupported storage format 'orc'"):
        manager.save([{"a": 1}], loc)


@pytest.mark.parametrize(
    "invalid_kwargs",
    [
        {"layer": "", "path": "data/file.csv", "format": "csv"},
        {"layer": "bronze", "path": "   ", "format": "csv"},
        {"layer": "bronze", "path": "data/file.csv", "format": ""},
    ],
)
def test_storage_location_rejects_invalid_configurations(
    invalid_kwargs: dict[str, str],
) -> None:
    with pytest.raises(StorageConfigurationError):
        StorageLocation(**invalid_kwargs)


def test_storage_manager_wraps_backend_failures() -> None:
    failing_save = StorageManager(backend=_InMemoryBackend(fail_save=True))
    loc = StorageLocation(layer="bronze", path="mem/fail.csv", format="csv")
    with pytest.raises(StorageWriteError, match="Failed to persist data") as exc_info:
        failing_save.save([{"a": 1}], loc)
    assert isinstance(exc_info.value.__cause__, OSError)

    failing_load = StorageManager(backend=_InMemoryBackend(fail_load=True))
    with pytest.raises(StorageReadError, match="Failed to read data") as load_exc:
        failing_load.load(loc)
    assert isinstance(load_exc.value.__cause__, OSError)


def test_storage_registry_and_partition_strategy() -> None:
    registry = StorageRegistry()
    registry.register("filesystem", FileSystemBackend)
    registry.register("object_storage", ObjectStorageBackend)

    assert registry.exists("filesystem") is True
    assert registry.get("filesystem") is FileSystemBackend
    assert set(registry.list_backends()) == {"filesystem", "object_storage"}

    with pytest.raises(StorageConfigurationError):
        registry.get("unknown")

    dt = datetime(2026, 11, 3, 12, 0, tzinfo=UTC)
    assert PartitionStrategy.by_date(dt) == "year=2026/month=11/day=03"

    with pytest.raises(PartitioningError):
        PartitionStrategy.by_date("2026-11-03")  # type: ignore[arg-type]
