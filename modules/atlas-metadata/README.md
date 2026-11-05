# atlas-metadata

`atlas-metadata` provides the **Phase 1: Domain Foundation** for dataset catalog, schema registry, artifact metadata, and typed URN identification within the **Atlas Platform** modular monorepo.

---

## 1. Scope & Architectural Responsibilities (Phase 1)

- **Centralized Validated URNs (`URN`)**: Immutable Pydantic-backed `urn:atlas:{entity_type}:{namespace}:{entity_name}` identifier model supporting parsing, validation, hashing, equality, and JSON string serialization.
- **Metadata Domain Models**:
  - `DatasetMetadata`: Dataset catalog record with validated URN, layer, format, owner, tags, and JSON-compatible `custom_properties`.
  - `ColumnMetadata` & `SchemaMetadata`: Strictly immutable (`frozen=True`) versioned schema definitions.
  - `ArtifactMetadata`: Immutable physical artifact metadata record.
- **Thread-Safe Repository & Atomicity**:
  - `MetadataRepository` contracts (`DatasetRepository`, `SchemaRepository`, `ArtifactRepository`).
  - `InMemoryMetadataRepository` synchronized with `threading.RLock`, enforcing atomic uniqueness on `(dataset_id, version)` so concurrent registrations of the same schema version never overwrite existing schemas and raise `SchemaConflictError`.
- **Dataset Catalog & Schema Registry**:
  - `DatasetCatalog`: Dataset registration, discovery, tag/property updates, and artifact linkage.
  - `SchemaRegistry`: Immutable schema versioning, retrieval of specific or latest versions, and automatic synchronization of `latest_schema_version` on the dataset.
- **`MetadataService` Foundation**:
  - Implements `atlas_core.MetadataProvider` (`register_dataset`, `get_dataset`) and exposes rich catalog, schema, and artifact operations.

### Dependency Rules

- **Allowed dependencies**: `atlas-core`, `pydantic`
- **Forbidden dependencies**: `atlas-connectors`, `atlas-storage`, `atlas-ingestion`, `atlas-processing`, `atlas-quality`, `atlas-orchestration`, `atlas-geospatial`, and all services under `services/`.

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
│       │   └── memory.py
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

## 3. Usage Example

```python
from atlas_metadata import (
    ColumnMetadata,
    DatasetMetadata,
    MetadataService,
    URN,
)

service = MetadataService()

# 1. Register dataset with validated URN, tags, and JSON custom properties
dataset = DatasetMetadata(
    dataset_id="orders_silver",
    urn=URN.for_dataset("orders_silver", namespace="commerce"),
    name="Curated Orders",
    layer="silver",
    format="parquet",
    owner="data-engineering",
    tags=["orders", "commerce"],
    custom_properties={"pii": False, "retention_days": 365},
)
service.register_dataset_metadata(dataset)

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
```

---

## 4. Testing

```bash
pytest modules/atlas-metadata/tests -v
```
