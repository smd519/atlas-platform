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
│       │   └── sqlalchemy/        # optional persistent backend (SQLite & PostgreSQL)
│       │       ├── base.py        # declarative Base, repository base, session helpers
│       │       ├── session.py     # MetadataSession (scoped Session subclass)
│       │       ├── events.py      # before_flush workspace ownership validation
│       │       ├── database.py    # engine, session factory, create_all, SQLite settings
│       │       ├── models.py      # ORM records & TenantOwnedMixin (infrastructure only)
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

# The URL and workspace_id are supplied by the caller (for example from the customer workspace).
repository = SqlAlchemyMetadataRepository.from_url(
    "sqlite:////absolute/workspace/path/metadata/catalog.db",
    workspace_id="customer_001",
)
service = MetadataService(repository=repository)

# ... register datasets, schemas, artifacts; restart the process; read them back ...
repository.dispose()  # release pooled connections on shutdown
```

- **Workspace-Scoped Multi-Tenancy (`workspace_id`)**: Every tenant-owned ORM record (`atlas_datasets`, `atlas_schemas`, `atlas_schema_columns`, `atlas_artifacts`) inherits from `TenantOwnedMixin` (`workspace_id`) and uses composite primary keys, composite foreign keys (`(workspace_id, schema_id)`), and workspace-scoped unique constraints. `SqlAlchemyMetadataRepository` requires a non-empty `workspace_id` and scopes all reads and writes to that workspace while keeping Pydantic domain models and repository method signatures completely agnostic of tenancy.
- **Explicit Repository Filtering (No Magic Reads)**: Repository queries intentionally use explicit `.where(Model.workspace_id == self._workspace_id)` predicates rather than `do_orm_execute`, `with_loader_criteria`, or hidden query rewriting so tenant isolation is immediately visible at every query site.
- **Scoped Session (`MetadataSession`) & `before_flush` Write-Path Ownership Validation**:
  - `create_session_factory()` binds a `sessionmaker` using `class_=MetadataSession` and registers `validate_workspace_ownership` on the metadata `sessionmaker` (never on the global `sqlalchemy.orm.Session` class).
  - Repository methods open short-lived sessions via `_read_session()` and `_write_session()`, passing `info={"workspace_id": self._workspace_id}` from the repository's authoritative `_workspace_id`.
  - On every ORM flush (`before_flush`), only entities inheriting from `TenantOwnedMixin` are validated (platform-global entities not inheriting from `TenantOwnedMixin` are ignored and do not require `workspace_id`):
    - **Missing workspace identity**: fails closed (`ValueError`) if `session.info.get("workspace_id")` is missing or empty.
    - **Insertions (`session.new`)**: rejects flushing if `entity.workspace_id != session.info["workspace_id"]`.
    - **Modifications (`session.dirty`)**: inspects `inspect(entity).attrs.workspace_id.history` and raises `ValueError("Cannot mutate workspace_id of a persistent entity")` if `workspace_id` was modified; also rejects modifications where `entity.workspace_id != session.info["workspace_id"]`.
    - **Deletions (`session.deleted`)**: inspects attribute history to verify the original persisted `workspace_id` matches `session.info["workspace_id"]`, rejecting cross-workspace deletions.
  - **Limitations of Application-Level Enforcement**: `before_flush` only intercepts SQLAlchemy ORM unit-of-work flushes. It does **not** intercept or protect arbitrary raw SQL (`session.execute(text(...))`) or SQLAlchemy Core statements (`insert()`, `update()`, `delete()`), and it does not replace database-level security. PostgreSQL Row-Level Security (RLS) remains a separate future security milestone.
- **Initialization & Local Development Schema Reset**: `initialize=True` (the default) runs `Base.metadata.create_all(engine)`: it creates missing tables, creates the parent directory of a file-based SQLite database, and never drops, recreates, or alters existing tables. Because automatic legacy SQLite migrations are intentionally omitted, **developers must manually delete any pre-existing local `catalog.db` file** (for example `metadata/catalog.db` inside a local workspace) when upgrading to the composite-key schema so `create_all()` can recreate it cleanly.
- **Deterministic Ordering**: `list_datasets()` orders by `(created_at, dataset_id)` and `list_artifacts()` orders by `(created_at, artifact_id)`.
- **Isolation**: Pydantic domain models never import SQLAlchemy. ORM records live in `repositories/sqlalchemy/models.py` and are translated by `mappings.py`. `import atlas_metadata` does not import SQLAlchemy.
- **Sessions and transactions**: every repository call opens its own short-lived `Session` (`factory.begin()` for writes), so sessions are never shared across threads and connections always return to the pool.
- **Schema version allocation**: `register_next_schema` computes `max(version) + 1` inside a transaction, backed by a unique constraint on `(workspace_id, namespace, dataset_id, version)`. A concurrent writer winning the same version triggers a bounded retry (`max_allocation_attempts`, default 10); other integrity errors are not retried.
- **SQLite settings** (isolated in `database.py`): WAL journal mode, `busy_timeout`/connect timeout (`sqlite_timeout`, default 30s), foreign keys on, and `check_same_thread=False`. A private `sqlite://` in-memory database uses a single shared connection (tests/single-threaded use only).
- **SQLite limitation**: SQLite permits one writer at a time. WAL lets readers proceed during a write and the timeout makes a second writer wait, but write throughput is serialized.
- **PostgreSQL later**: supported by the SQLAlchemy implementation.
Future production deployments may use shared or dedicated PostgreSQL databases without changing metadata domain models or repository contracts. pass a PostgreSQL URL (for example `postgresql+psycopg://user:password@host:5432/atlas`) and install the driver; no repository code changes are needed. ORM types are portable (`String`, `Text`, `Integer`, `BigInteger`, `Boolean`, `JSON`), and dataset tag filtering is done in Python because JSON-array containment is not portable SQL.
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
