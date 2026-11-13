"""Unit tests for atlas-storage format handlers, format registry, manager, and error handling."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd  # type: ignore[import-untyped]
import pytest
from atlas_core import StorageProvider
from atlas_storage import (
    CsvHandler,
    FileSystemBackend,
    FormatHandler,
    FormatRegistry,
    JsonHandler,
    ObjectStorageBackend,
    ParquetHandler,
    PartitioningError,
    PartitionStrategy,
    StorageConfigurationError,
    StorageLocation,
    StorageManager,
    StorageReadError,
    StorageRegistry,
    StorageWriteError,
    create_default_format_registry,
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


class _MetadataTableHandler:
    """Custom format handler that persists a metadata marker and checks logical existence via it."""

    def save(
        self,
        data: Any,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> int | None:
        payload = repr(data).encode("utf-8")
        backend.save(data=payload, path=f"{location.path}/part-00000.dat")
        backend.save(data=b"v1", path=f"{location.path}/_metadata")
        return len(data) if hasattr(data, "__len__") else 1

    def load(
        self,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> str:
        raw = backend.load(path=f"{location.path}/part-00000.dat")
        return bytes(raw).decode("utf-8")

    def exists(
        self,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> bool:
        return backend.exists(path=f"{location.path}/_metadata")


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

    with pytest.raises(StorageConfigurationError, match="Unsupported storage format 'orc'"):
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


def test_format_registry_builtin_and_custom_registration_and_normalization() -> None:
    default_registry = create_default_format_registry()
    assert {"csv", "json", "parquet"}.issubset(set(default_registry.list_formats()))
    assert default_registry.exists("csv") is True
    assert default_registry.exists("  CSV \n") is True
    assert default_registry.exists("unknown") is False

    first_csv = default_registry.get("csv")
    second_csv = default_registry.get("  CSV ")
    assert isinstance(first_csv, CsvHandler)
    assert isinstance(second_csv, CsvHandler)
    assert isinstance(first_csv, FormatHandler)
    assert first_csv is not second_csv

    custom_registry = FormatRegistry()
    custom_registry.register("  MetaTable ", _MetadataTableHandler)
    assert custom_registry.exists("metatable") is True
    assert custom_registry.exists("METATABLE") is True
    assert custom_registry.list_formats() == ["metatable"]
    handler = custom_registry.get("MetaTable")
    assert isinstance(handler, _MetadataTableHandler)


def test_format_registry_rejects_duplicate_and_invalid_registrations() -> None:
    registry = create_default_format_registry()

    with pytest.raises(StorageConfigurationError, match="already registered"):
        registry.register("CSV", CsvHandler)

    with pytest.raises(StorageConfigurationError, match="Unsupported storage format 'delta'"):
        registry.get("delta")

    with pytest.raises(StorageConfigurationError, match="cannot be empty"):
        registry.register("   ", CsvHandler)

    class _IncompleteHandler:
        def save(self, data: Any, location: StorageLocation, backend: StorageProvider) -> int:
            return 0

    with pytest.raises(StorageConfigurationError, match="missing required FormatHandler methods"):
        registry.register("incomplete", _IncompleteHandler)  # type: ignore[arg-type]


def test_builtin_format_handlers_direct_execution_and_error_semantics() -> None:
    backend = _InMemoryBackend()

    csv_handler = CsvHandler()
    csv_loc = StorageLocation(layer="bronze", path="direct/items.csv", format="csv")
    assert csv_handler.save([{"id": 1, "val": "x"}], csv_loc, backend) == 1
    assert csv_handler.exists(csv_loc, backend) is True
    loaded_csv = csv_handler.load(csv_loc, backend)
    assert list(loaded_csv["val"]) == ["x"]

    with pytest.raises(StorageWriteError, match="Failed to serialize data to CSV"):
        csv_handler.save(object(), csv_loc, backend)

    json_handler = JsonHandler()
    json_loc = StorageLocation(layer="bronze", path="direct/items.json", format="json")
    assert json_handler.save([{"id": 2, "val": "y"}], json_loc, backend) == 1
    assert json_handler.exists(json_loc, backend) is True
    loaded_json = json_handler.load(json_loc, backend)
    assert list(loaded_json["val"]) == ["y"]

    with pytest.raises(StorageWriteError, match="Failed to serialize data to JSON"):
        json_handler.save(object(), json_loc, backend)

    backend.blobs[json_loc.path] = b"{not-valid-json"
    with pytest.raises(StorageReadError, match="Failed to deserialize JSON bytes"):
        json_handler.load(json_loc, backend)

    parquet_handler = ParquetHandler()
    parquet_loc = StorageLocation(layer="bronze", path="direct/items.parquet", format="parquet")
    assert parquet_handler.save([{"id": 3, "val": "z"}], parquet_loc, backend) == 1
    assert parquet_handler.exists(parquet_loc, backend) is True
    loaded_parquet = parquet_handler.load(parquet_loc, backend)
    assert list(loaded_parquet["val"]) == ["z"]

    with pytest.raises(StorageWriteError, match="Failed to serialize data to Parquet"):
        parquet_handler.save(object(), parquet_loc, backend)

    backend.blobs[parquet_loc.path] = b"corrupt-parquet-bytes"
    with pytest.raises(StorageReadError, match="Failed to deserialize Parquet bytes"):
        parquet_handler.load(parquet_loc, backend)


def test_storage_manager_delegates_save_load_and_logical_exists_to_custom_handler() -> None:
    backend = _InMemoryBackend()
    registry = FormatRegistry()
    registry.register("metatable", _MetadataTableHandler)
    manager = StorageManager(backend=backend, format_registry=registry)

    loc = StorageLocation(layer="silver", path="tables/orders", format="metatable")
    assert manager.exists(loc) is False

    # Base path or data file alone does not satisfy logical existence without _metadata
    backend.blobs["tables/orders"] = b"directory-placeholder"
    backend.blobs["tables/orders/part-00000.dat"] = b"partial-data"
    assert manager.exists(loc) is False

    result = manager.save([{"order_id": 10}, {"order_id": 20}], loc)
    assert result.success is True
    assert result.path == "tables/orders"
    assert result.row_count == 2
    assert "tables/orders/_metadata" in backend.blobs
    assert manager.exists(loc) is True

    loaded = manager.load(loc)
    assert loaded == "[{'order_id': 10}, {'order_id': 20}]"
