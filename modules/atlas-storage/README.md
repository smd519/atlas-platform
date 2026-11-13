# atlas-storage

Format-agnostic storage orchestration, pluggable format handlers, physical storage backends, and dataset partitioning for the Atlas Platform.

---

## Architecture

`atlas-storage` separates **logical format handling** (`FormatHandler` & `FormatRegistry`) from **physical byte persistence** (`StorageProvider` & `StorageRegistry`):

```text
StorageManager
      |
      v
FormatRegistry  --->  FormatHandler (CsvHandler / JsonHandler / ParquetHandler / RawBytesHandler / Custom)
                            |
                            v
                     StorageProvider (FileSystemBackend / ObjectStorageBackend)
```

- **`StorageManager`**: Format-agnostic coordinator that validates `StorageLocation`, resolves the appropriate `FormatHandler` from `FormatRegistry`, and delegates `save()`, `load()`, and `exists()`.
- **`FormatRegistry` (`handlers/registry.py`)**: Registers and instantiates `FormatHandler` classes by normalized format name (`csv`, `json`, `parquet`, `raw`, `bytes`, or custom table/file formats).
- **`FormatHandler` (`handlers/protocol.py`)**: Structural protocol defining `save(data, location, backend)`, `load(location, backend)`, and `exists(location, backend)`. Delegating `exists()` to the format handler enables both single-file formats (checking `location.path`) and future multi-file or metadata-driven table formats (checking e.g. `_delta_log` or `_metadata`).
- **`StorageRegistry` (`registry.py`)**: Registers and resolves physical `StorageProvider` backend classes (`FileSystemBackend`, `ObjectStorageBackend`).

---

## Components

| Component | Responsibility |
| --- | --- |
| `manager.py` | Format-agnostic `StorageManager` coordinating `FormatRegistry` and `StorageProvider`. |
| `handlers/protocol.py` | Runtime-checkable `FormatHandler` protocol (`save`, `load`, `exists`). |
| `handlers/registry.py` | `FormatRegistry` and `create_default_format_registry()` for format handler registration and lookup. |
| `handlers/files/csv.py` | `CsvHandler` for UTF-8 CSV serialization, deserialization, and persistence. |
| `handlers/files/json.py` | `JsonHandler` for UTF-8 JSON serialization, deserialization, and persistence. |
| `handlers/files/parquet.py` | `ParquetHandler` for Apache Parquet serialization, deserialization, and persistence. |
| `handlers/files/raw.py` | `RawBytesHandler` for passthrough binary/raw payload persistence. |
| `backends/filesystem.py` | `FileSystemBackend` implementing local filesystem byte persistence (`StorageProvider`). |
| `backends/object_storage.py` | `ObjectStorageBackend` contract/skeleton for object storage providers. |
| `registry.py` | `StorageRegistry` for registering and resolving `StorageProvider` backend classes. |
| `models/storage_location.py` | `StorageLocation` model defining target `layer`, `path`, and `format`. |
| `models/storage_result.py` | `StorageResult` model returned from `StorageManager.save()`. |
| `partitioning/strategy.py` | `PartitionStrategy` for generating Hive-style date partitions (`year=YYYY/month=MM/day=DD`). |
| `formats/` | Backward-compatible procedural serialization and direct file I/O helpers (`serialize_data`, `deserialize_data`, `write_csv`, `write_json`, `write_parquet`, etc.). |
| `exceptions.py` | Storage exception hierarchy (`StorageError`, `StorageConfigurationError`, `StorageReadError`, `StorageWriteError`, `StorageBackendError`, `SerializationError`, `PartitioningError`). |

---

## Capabilities

- **Format-Agnostic `StorageManager`**:
  - Defaults to `create_default_format_registry()` with built-in handlers for `csv`, `json`, `parquet`, `raw`, and `bytes`.
  - Accepts an optional custom `FormatRegistry` for extending format support without modifying `StorageManager`.
- **Pluggable `FormatHandler` Protocol**:
  - `save(data, location, backend) -> int | None`: Serializes data (raising `StorageWriteError` on invalid input), persists via `backend.save()`, and returns the record count.
  - `load(location, backend) -> Any`: Loads raw bytes via `backend.load()` and deserializes the dataset (raising `StorageReadError` on corrupt payloads).
  - `exists(location, backend) -> bool`: Verifies logical dataset existence via `backend.exists()`.
- **Strict Registry Validation (`FormatRegistry`)**:
  - Normalizes format names (`name.strip().lower()`).
  - Rejects duplicate format registrations and invalid handler classes with `StorageConfigurationError`.
  - Instantiates a fresh `FormatHandler` instance on each `get(name)` call.
- **Physical Backend Abstraction (`StorageProvider` & `StorageRegistry`)**:
  - Decouples byte-level storage (`FileSystemBackend`, `ObjectStorageBackend`) from dataset formats.
- **Hive-Style Date Partitioning (`PartitionStrategy`)**:
  - Generates standardized `year=YYYY/month=MM/day=DD` partition paths.

---

## Usage Examples

### 1. Standard Usage with Built-in Format Handlers

```python
import pandas as pd
from atlas_storage import FileSystemBackend, StorageLocation, StorageManager

backend = FileSystemBackend()
storage = StorageManager(backend=backend)

dataframe = pd.DataFrame([
    {"station": "S1", "temp": 20.5},
    {"station": "S2", "temp": 14.0},
])

location = StorageLocation(
    layer="bronze",
    path="data/bronze/weather.parquet",
    format="parquet",
)

result = storage.save(dataframe, location)
assert result.success is True
assert result.row_count == 2
assert storage.exists(location) is True

loaded_df = storage.load(location)
```

### 2. Registering a Custom `FormatHandler`

```python
from typing import Any
from atlas_core import StorageProvider
from atlas_storage import (
     FileSystemBackend,
    StorageLocation,
    StorageManager,
    create_default_format_registry,
)


class CustomTableHandler:
    def save(
        self,
        data: Any,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> int | None:
        payload = repr(data).encode("utf-8")
        backend.save(data=payload, path=f"{location.path}/part-00000.dat")
        backend.save(data=b"v1", path=f"{location.path}/_metadata")
        return len(data)

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


registry = create_default_format_registry()
registry.register("custom_table", CustomTableHandler)

storage = StorageManager(
    backend=FileSystemBackend(),
    format_registry=registry,
)
```

---

## Testing

Run unit tests, integration tests, linting, and strict type checks from the repository root:

```bash
# Unit and integration tests
pytest modules/atlas-storage

# Linting and formatting
ruff check modules/atlas-storage
ruff format --check modules/atlas-storage

# Strict static type checking
mypy --strict modules/atlas-storage/src modules/atlas-storage/tests
```


---

## Testing

Run unit tests, integration tests, linting, and strict type checks from the repository root:

```bash
# Unit and integration tests
pytest modules/atlas-storage

# Linting and formatting
ruff check modules/atlas-storage
ruff format --check modules/atlas-storage

# Strict static type checking
mypy --strict modules/atlas-storage/src modules/atlas-storage/tests
```