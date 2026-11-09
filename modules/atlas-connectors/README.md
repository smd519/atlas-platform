# atlas-connectors

Source acquisition, HTTP streaming, authentication strategies, and file format parsers for the Atlas Platform.

---

## Components

| Component | Responsibility |
| --- | --- |
| `registry.py` | Registers and resolves available connector implementations (`ConnectorRegistry`). |
| `factory.py` | Creates connector instances from registered connector types or `SourceConfig` (`ConnectorFactory`). |
| `exceptions.py` | Connector-specific exception hierarchy (`ConnectorError`, `ConnectorConfigurationError`, `ConnectorConnectionError`, `ConnectorAuthenticationError`, `ConnectorExtractionError`, `ParserError`). |
| `models/source_config.py` | Standard configuration model for data sources (`SourceConfig`). |
| `models/extraction_result.py` | Standard extraction result returned by connectors (`ExtractionResult`). |
| `parsers/csv.py` | Parses CSV files into pandas DataFrames (`parse_csv`). |
| `parsers/json.py` | Parses JSON files into pandas DataFrames (`parse_json`). |
| `parsers/parquet.py` | Parses Parquet files into pandas DataFrames (`parse_parquet`). |
| `files/local.py` | Reads files from the local filesystem without copying or moving (`LocalFileConnector`). |
| `http/auth.py` | Authentication strategies for HTTP-based connectors (`AuthStrategy`, `ApiKeyAuth`, `BearerTokenAuth`). |
| `http/rest.py` | REST API connector (`RestConnector`) and adapter (`RestConnectorAdapter`) supporting both in-memory JSON extraction and memory-safe atomic file streaming. |

---

## Capabilities

- **Unified `Connector` Contract**:
  ```python
  def extract(
      self,
      *,
      destination: Path | None = None,
      **kwargs: Any,
  ) -> Any: ...
  ```
- **Local File Extraction (`LocalFileConnector`)**:
  - Validates that the configured local source file exists and returns its `Path`.
  - Accepts and ignores `destination` because local files already reside on disk (never copies or moves source files).
- **REST API Extraction (`RestConnector` & `RestConnectorAdapter`)**:
  - **In-Memory JSON Mode (`destination=None`)**: Performs an HTTP `GET` request and returns parsed JSON.
  - **Atomic Streaming Mode (`destination=Path(...)`)**:
    - Validates that `destination.parent` already exists (directory creation belongs to `atlas-ingestion`, not `atlas-connectors`).
    - Streams the HTTP response in chunks via `httpx.stream("GET", ...)` and `response.iter_bytes()` into `<destination>.part` on the same filesystem without loading the full payload into memory.
    - Atomically promotes `part_path.replace(destination)` only after the download completes and the file handle is closed, returning the completed `destination` `Path`.
    - Automatically cleans up any incomplete `.part` file on HTTP, timeout, connection, or filesystem errors without modifying any pre-existing `destination` file.
- **Format Parsers**: CSV, JSON, and Parquet parsers with standardized `ParserError` mapping.
- **Authentication**: API key (`ApiKeyAuth`) and Bearer token (`BearerTokenAuth`) header strategies.

---

## Usage Examples

### 1. Registry and Factory

```python
from atlas_connectors import (
    ConnectorFactory,
    ConnectorRegistry,
    LocalFileConnector,
    RestConnector,
)

registry = ConnectorRegistry()
registry.register("local_file", LocalFileConnector)
registry.register("rest", RestConnector)

factory = ConnectorFactory(registry)
local_connector = factory.create("local_file", path="drop/corporations.csv")
source_path = local_connector.extract()
```

### 2. REST In-Memory JSON vs. Atomic File Streaming

```python
from pathlib import Path
from atlas_connectors import BearerTokenAuth, RestConnector

connector = RestConnector(
    base_url="https://example.com",
    auth=BearerTokenAuth("secret-token"),
    endpoint="/downloads/corporations-active-cbca-en.csv",
    timeout=60.0,
)

# 1. In-memory JSON mode (when destination is omitted)
# payload = connector.extract(endpoint="/api/v1/status")

# 2. Memory-safe atomic streaming to an existing staging directory
staging_file = Path("drop/.staging/exec-001/corporations-active-cbca-en.csv")
staging_file.parent.mkdir(parents=True, exist_ok=True)
completed_path = connector.extract(destination=staging_file)
```
