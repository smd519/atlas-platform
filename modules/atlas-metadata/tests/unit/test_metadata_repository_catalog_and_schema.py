"""Unit tests for InMemoryMetadataRepository, DatasetCatalog, SchemaRegistry, and concurrency."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from atlas_metadata import (
    ColumnMetadata,
    DatasetCatalog,
    DatasetMetadata,
    DatasetNotFoundError,
    DuplicateDatasetError,
    InMemoryMetadataRepository,
    SchemaConflictError,
    SchemaMetadata,
    SchemaNotFoundError,
    SchemaRegistry,
)


def test_dataset_catalog_registration_duplicates_updates_and_filtering() -> None:
    """DatasetCatalog registers, retrieves, updates, filters, and prevents accidental overwrites."""
    repo = InMemoryMetadataRepository()
    catalog = DatasetCatalog(repository=repo)

    ds1 = DatasetMetadata(
        dataset_id="orders_bronze",
        namespace="commerce",
        name="Raw Orders",
        layer="bronze",
        owner="ingestion-team",
        tags=["raw", "orders"],
        custom_properties={"source": "erp"},
    )
    stored = catalog.register_dataset(ds1)
    assert stored.dataset_id == "orders_bronze"

    # Duplicate registration without overwrite raises DuplicateDatasetError
    with pytest.raises(DuplicateDatasetError):
        catalog.register_dataset(ds1, overwrite=False)

    # Lookup by ID and URN
    assert catalog.get_dataset("orders_bronze") is not None
    assert catalog.get_dataset_by_urn("urn:atlas:dataset:commerce:orders_bronze") is not None
    with pytest.raises(DatasetNotFoundError):
        catalog.get_dataset("unknown_ds", raise_if_missing=True)

    # Update tags and custom properties
    updated = catalog.update_dataset(
        "orders_bronze",
        owner="platform-team",
        tags=["raw", "orders", "verified"],
        custom_properties={"sla_hours": 4},
        merge_properties=True,
    )
    assert updated.owner == "platform-team"
    assert updated.tags == ["raw", "orders", "verified"]
    assert updated.custom_properties == {"source": "erp", "sla_hours": 4}

    # Filtering
    ds2 = DatasetMetadata(
        dataset_id="orders_silver",
        namespace="commerce",
        name="Curated Orders",
        layer="silver",
        owner="analytics-team",
        tags=["curated", "orders"],
    )
    catalog.register_dataset(ds2)

    assert len(catalog.list_datasets(layer="silver")) == 1
    assert len(catalog.list_datasets(tag="orders")) == 2
    assert len(catalog.list_datasets(owner="analytics-team")) == 1

    assert catalog.delete_dataset("orders_bronze") is True
    assert catalog.get_dataset("orders_bronze") is None


def test_schema_registry_versioning_and_dataset_sync() -> None:
    """SchemaRegistry tracks immutable versions and updates dataset latest_schema_version."""
    repo = InMemoryMetadataRepository()
    catalog = DatasetCatalog(repository=repo)
    registry = SchemaRegistry(repository=repo)

    catalog.register_dataset(
        DatasetMetadata(dataset_id="events", namespace="telemetry", name="Events")
    )

    s1 = registry.register_schema(
        "events",
        namespace="telemetry",
        version=1,
        columns=[ColumnMetadata(name="event_id", data_type="string", primary_key=True)],
    )
    assert s1.version == 1

    # Auto-increment version (version=None -> v2)
    s2 = registry.register_schema(
        "events",
        namespace="telemetry",
        columns=[
            ColumnMetadata(name="event_id", data_type="string", primary_key=True),
            ColumnMetadata(name="timestamp", data_type="datetime"),
        ],
    )
    assert s2.version == 2

    # Attempting to re-register version 1 raises SchemaConflictError
    with pytest.raises(SchemaConflictError):
        registry.register_schema(
            "events",
            namespace="telemetry",
            version=1,
            columns=[ColumnMetadata(name="event_id", data_type="string")],
        )

    assert registry.get_schema("events", version=1) == s1
    assert registry.get_latest_schema("events") == s2
    assert registry.get_schema_by_urn(s1.urn) == s1
    assert [s.version for s in registry.list_schemas("events")] == [1, 2]

    # Dataset latest_schema_version is synchronized
    ds_meta = catalog.require_dataset("events")
    assert ds_meta.latest_schema_version == 2

    with pytest.raises(SchemaNotFoundError):
        registry.get_schema("events", version=99, raise_if_missing=True)


def test_concurrent_same_version_schema_registration_atomicity() -> None:
    """Concurrent attempts to register the same (dataset_id, version) succeed exactly once."""
    repo = InMemoryMetadataRepository()
    registry = SchemaRegistry(repository=repo)
    num_workers = 16
    barrier = threading.Barrier(num_workers)

    successes: list[SchemaMetadata] = []
    conflicts: list[SchemaConflictError] = []
    unexpected_errors: list[Exception] = []
    result_lock = threading.Lock()

    def worker(worker_id: int) -> None:
        schema = SchemaMetadata(
            dataset_id="concurrent_ds",
            version=1,
            columns=(
                ColumnMetadata(name="id", data_type="string", primary_key=True),
                ColumnMetadata(name=f"col_{worker_id}", data_type="int"),
            ),
        )
        barrier.wait()
        try:
            saved = registry.register_schema(schema)
            with result_lock:
                successes.append(saved)
        except SchemaConflictError as exc:
            with result_lock:
                conflicts.append(exc)
        except Exception as exc:
            with result_lock:
                unexpected_errors.append(exc)

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = [pool.submit(worker, i) for i in range(num_workers)]
        for f in futures:
            f.result()

    assert unexpected_errors == []
    assert len(successes) == 1
    assert len(conflicts) == num_workers - 1
    stored_schema = registry.require_schema("concurrent_ds", version=1)
    assert stored_schema == successes[0]


def test_concurrent_auto_version_schema_registration_thread_safety() -> None:
    """Concurrent auto-versioned registrations allocate distinct versions 1..N atomically."""
    repo = InMemoryMetadataRepository()
    registry = SchemaRegistry(repository=repo)
    num_workers = 12
    barrier = threading.Barrier(num_workers)

    def register_next(worker_id: int) -> int:
        barrier.wait()
        schema = registry.register_schema(
            "stream_ds",
            version=None,
            columns=[
                ColumnMetadata(name="id", data_type="int"),
                ColumnMetadata(name=f"v_{worker_id}", data_type="string"),
            ],
        )
        return schema.version

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = [pool.submit(register_next, i) for i in range(num_workers)]
        versions = [f.result(timeout=10) for f in futures]

    assert sorted(versions) == list(range(1, num_workers + 1))
    assert len(registry.list_schemas("stream_ds")) == num_workers
