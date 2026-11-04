# atlas-connectors
## Components

| Component | Responsibility |
|------------|----------------|
| `registry.py` | Registers and resolves available connector implementations. |
| `factory.py` | Creates connector instances from registered connector types. |
| `exceptions.py` | Connector-specific exception hierarchy. |
| `models/source_config.py` | Standard configuration model for data sources. |
| `models/extraction_result.py` | Standard extraction result returned by connectors. |
| `parsers/csv.py` | Parses CSV files into platform-supported structures. |
| `parsers/json.py` | Parses JSON files into platform-supported structures. |
| `parsers/parquet.py` | Parses Parquet files into platform-supported structures. |
| `files/local.py` | Reads files from the local filesystem. |
| `http/auth.py` | Authentication strategies for HTTP-based connectors. |
| `http/rest.py` | REST API connector implementation and request handling. |

## Initial Capabilities

- Local file ingestion
- CSV parsing
- JSON parsing
- Parquet parsing
- REST API ingestion
- API key authentication
- Bearer token authentication
- Connector registration
- Connector instantiation through factories
- Standardized extraction results
- Consistent connector exception handling


### Registry Example

```python
from atlas_connectors.files.local import LocalFileConnector
from atlas_connectors.http.rest import RestConnector
from atlas_connectors.registry import ConnectorRegistry

registry = ConnectorRegistry()

registry.register(
    "local_file",
    LocalFileConnector,
)

registry.register(
    "rest",
    RestConnector,
)
```

Resolve a connector:

```python
connector_class = registry.get("local_file")

connector = connector_class(
    path="data/source.csv",
)
```

Using the factory:

```python
from atlas_connectors.factory import ConnectorFactory

factory = ConnectorFactory(registry)

connector = factory.create(
    "local_file",
    path="data/source.csv",
)
```