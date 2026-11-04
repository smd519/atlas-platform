# atlas-storage

## Components

| Component | Responsibility |
|------------|----------------|
| `exceptions.py` | Storage-specific exception hierarchy. |
| `models/storage_location.py` | Defines physical storage locations and paths. |
| `models/storage_result.py` | Standard result returned from storage operations. |
| `formats/parquet.py` | Parquet serialization and deserialization. |
| `formats/csv.py` | CSV serialization and deserialization. |
| `formats/json.py` | JSON serialization and deserialization. |
| `backends/filesystem.py` | Local filesystem storage implementation. |
| `registry.py` | Registers and resolves storage backends. |
| `manager.py` | Orchestrates storage operations through registered backends. |
| `partitioning/strategy.py` | Generates and manages partition paths. |

## Initial Capabilities

- Filesystem-based storage
- CSV persistence
- JSON persistence
- Parquet persistence
- Backend registration and resolution
- Unified storage management
- Read/write operations
- Dataset partitioning
- Raw storage support
- Curated storage support


### manager

```python
# Backend selection occurs before manager usage

backend = FileSystemBackend()

storage = StorageManager(
    backend=backend,
)

storage.save(
    dataframe,
    StorageLocation(
        layer="bronze",
        path="data/bronze/weather.parquet",
        format="parquet",
    ),
)
```