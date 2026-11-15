# atlas-metadata

`atlas-metadata` provides the dataset catalog, versioned schema registry, physical artifact provenance tracking, and typed URN identification for the **Atlas Platform** modular monorepo.

---

## 1. Scope & Architectural Responsibilities

- **Centralized Validated URNs (`URN`)**:
  - Immutable Pydantic-backed `urn:atlas:{entity_type}:{namespace}:{entity_name}` identifier model supporting parsing, validation, hashing, equality, and JSON string serialization.
- **Metadata Domain Models**:
  - `DatasetMetadata`: Dataset catalog record with validated URN, `layer`, `format`, `location_uri`, `source_id`, `owner`, normalized `tags`, normalized `upstream_urns: list[str]` (for lineage tracking), and JSON-compatible `custom_properties`.
  - `ColumnMetadata` & `SchemaMetadata`: Strictly immutable (`frozen=True`) versioned schema definitions.
  - `ArtifactMetadata`: Immutable physical artifact metadata record tracking `dataset_id`, physical `uri`, `format`, `layer`, `execution_id: str | None` (execution provenance), `row_count`, `byte_size`, `tags`, and `custom_properties`.
- **Thread-Safe Repository & Atomicity**:
  - `MetadataRepository` contracts (`DatasetRepository`, `SchemaRepository`, `ArtifactRepository`).
  - `InMemoryMetadataRepository` synchronized with `threading.RLock`, enforcing atomic uniqueness on `(dataset_id, version)` so concurrent registrations of the same schema version never overwrite existing schemas and raise `SchemaConflictError`.
- **Dataset Catalog & Schema Registry**:
  - `DatasetCatalog`: Dataset registration, discovery, tag/property updates, and artifact linkage.
  - `SchemaRegistry`: Immutable schema versioning, retrieval of specific or latest versions, and automatic synchronization of `latest_schema_version` on the dataset.
- **`MetadataService` Facade**:
  - Implements `atlas_core.MetadataProvider` (`register_dataset`, `get_dataset`, `register_dataset_metadata`, `register_artifact`) and exposes rich catalog, schema, and artifact query operations.
- **Ingestion Integration (Phase 2)**:
  - Injecting `MetadataService` (or any `MetadataProvider`) into `atlas-ingestion`'s `IngestionService` / `IngestionPipeline` automatically registers the Bronze `DatasetMetadata` and `ArtifactMetadata` (linked by `dataset_id`, physical `uri`, and `execution_id`) when data lands in Bronze.

### Dependency Rules

- **Allowed dependencies**: `atlas-core`, `pydantic`, `sqlalchemy` (used only by the optional persistence backend in `repositories/sqlalchemy/`)
- **Forbidden dependencies**: `atlas-connectors`, `atlas-storage`, `atlas-ingestion`, `atlas-processing`, `atlas-quality`, `atlas-orchestration`, `atlas-spatial`, and all services under `services/`.

---

## 2. Package Structure

```text
modules/atlas-metadata/
├── pyproject.toml
├── README.md
├── src/
│   └── atlas_metadata/
│       ├── __init__.py
│       ├── py.typed
│       ├── exceptions.py
│       ├── service.py
│       ├── identifiers/
│       │   ├── __init__.py
│       │   └── urn.py
│       ├── models/
│       │   ├── __init__.py
│       │   ├── dataset.py
│       │   ├── schema.py
│       │   └── artifact.py
│       ├── repositories/
│       │   ├── __init__.py
│       │   ├── contracts.py
│       │   ├── memory.py
│       │   └── sqlalchemy/        # optional persistent backend (SQLite first)
│       │       ├── base.py        # declarative Base, session plumbing, error translation
│       │       ├── database.py    # engine, session factory, create_all, SQLite settings
│       │       ├── models.py      # ORM records (infrastructure only)
│       │       ├── mappings.py    # domain <-> ORM translation
│       │       ├── dataset.py
│       │       ├── schema.py
│       │       ├── artifact.py
│       │       └── repository.py  # unified SqlAlchemyMetadataRepository
│       ├── catalog/
│       │   ├── __init__.py
│       │   └── dataset_catalog.py
│       └── schema/
│           ├── __init__.py
│           └── schema_registry.py
└── tests/
    ├── unit/
    └── integration/
```

---

## 3. Usage Examples

### 3.1 Catalog, Lineage (`upstream_urns`), Schema Versioning, and Artifacts

```python
from atlas_metadata import (
    ArtifactMetadata,
    ColumnMetadata,
    DatasetMetadata,
    MetadataService,
    URN,
)

service = MetadataService()

# 1. Register dataset with URN, upstream lineage URNs, tags, and custom properties
dataset = DatasetMetadata(
    dataset_id="orders_silver",
    urn=URN.for_dataset("orders_silver", namespace="commerce"),
    name="Curated Orders",
    layer="silver",
    format="parquet",
    owner="data-engineering",
    tags=["orders", "commerce"],
    upstream_urns=["urn:atlas:dataset:commerce:orders_bronze"],
    custom_properties={"pii": False, "retention_days": 365},
)
service.register_dataset_metadata(dataset, overwrite=True)

# 2. Register immutable schema version 1
schema_v1 = service.register_schema(
    "orders_silver",
    namespace="commerce",
    version=1,
    columns=[
        ColumnMetadata(name="order_id", data_type="string", primary_key=True),
        ColumnMetadata(name="amount", data_type="float", nullable=False),
    ],
)

# 3. Register physical artifact with execution provenance
artifact = ArtifactMetadata(
    artifact_id="art-orders-001",
    dataset_id="orders_silver",
    namespace="commerce",
    uri="data/silver/orders.parquet",
    format="parquet",
    layer="silver",
    execution_id="exec-20261108-001",
    schema_version=schema_v1.version,
    row_count=500,
)
service.register_artifact(artifact, overwrite=True)
```

### 3.2 Automatic Bronze Registration with `atlas-ingestion`

```python
from atlas_connectors import SourceConfig
from atlas_core import BRONZE, DatasetId
from atlas_ingestion import IngestionRequest, IngestionService
from atlas_metadata import MetadataService
from atlas_storage import StorageLocation

metadata_service = MetadataService()
ingestion_service = IngestionService(metadata_provider=metadata_service)

request = IngestionRequest(
    dataset_id=DatasetId("corporations_cbca"),
    source=SourceConfig(
        source_id="src-cbca",
        source_name="CBCA Source",
        source_type="local_file",
        config={"path": "drop/corporations.csv"},
    ),
    destination=StorageLocation(
        layer=BRONZE,
        path="data/bronze/corporations.parquet",
        format="parquet",
    ),
    source_format="csv",
    tags=["raw", "corporations"],
)

result = ingestion_service.ingest(request, execution_id="exec-ingest-001")

# Dataset and physical artifact are automatically registered in MetadataService
ds_meta = metadata_service.get_dataset_metadata("corporations_cbca")
artifacts = metadata_service.list_artifacts(
    dataset_id="corporations_cbca",
    execution_id="exec-ingest-001",
)
```

---

### 3.3 Persistent Catalog with SQLAlchemy (SQLite)

`MetadataService` accepts any `MetadataRepository`, so persistence is a drop-in swap. The in-memory repository remains the default.

```python
from atlas_metadata import MetadataService
from atlas_metadata.repositories.sqlalchemy import SqlAlchemyMetadataRepository

# The URL is supplied by the caller (for example from the customer workspace); nothing is hardcoded.
repository = SqlAlchemyMetadataRepository.from_url(
    "sqlite:////absolute/workspace/path/metadata/catalog.db"
)
service = MetadataService(repository=repository)

# ... register datasets, schemas, artifacts; restart the process; read them back ...
repository.dispose()  # release pooled connections on shutdown
```

- **Initialization** (`initialize=True` by default) runs `create_all`: it creates missing tables, creates the parent directory of a file-based SQLite database, and never drops, recreates, or alters existing data. There is no migration tooling yet; schema changes in this phase are handled by deliberately recreating the database file.
- **Isolation**: Pydantic domain models never import SQLAlchemy. ORM records live in `repositories/sqlalchemy/models.py` and are translated by `mappings.py`. `import atlas_metadata` does not import SQLAlchemy.
- **Sessions and transactions**: every repository call opens its own short-lived `Session` (`factory.begin()` for writes), so sessions are never shared across threads and connections always return to the pool.
- **Schema version allocation**: `register_next_schema` computes `max(version) + 1` inside a transaction, backed by a unique constraint on `(namespace, dataset_id, version)`. A concurrent writer winning the same version triggers a bounded retry (`max_allocation_attempts`, default 10); other integrity errors are not retried.
- **SQLite settings** (isolated in `database.py`): WAL journal mode, `busy_timeout`/connect timeout (`sqlite_timeout`, default 30s), foreign keys on, and `check_same_thread=False`. A private `sqlite://` in-memory database uses a single shared connection (tests/single-threaded use only).
- **SQLite limitation**: SQLite permits one writer at a time. WAL lets readers proceed during a write and the timeout makes a second writer wait, but write throughput is serialized.
- **PostgreSQL later**: pass a PostgreSQL URL (for example `postgresql+psycopg://user:password@host:5432/atlas`) and install the driver; no repository code changes are needed. ORM types are portable (`String`, `Text`, `Integer`, `BigInteger`, `Boolean`, `JSON`), and dataset tag filtering is done in Python because JSON-array containment is not portable SQL.
- **Contract notes**: repository lookups take only `dataset_id` (no namespace), so `dataset_id` is treated as unique catalog-wide even though the database constraint is `(namespace, dataset_id)`. Schemas and artifacts have no foreign key to datasets because the contracts allow registering them independently of a dataset record.

---

## 4. Testing & Verification

```bash
# Unit and integration tests
pytest modules/atlas-metadata/tests -v

# Linting and formatting
ruff check modules/atlas-metadata
ruff format --check modules/atlas-metadata

# Strict static type checking
mypy --strict modules/atlas-metadata/src modules/atlas-metadata/tests
```
