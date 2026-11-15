"""Repository contract tests run against both in-memory and SQLAlchemy implementations.

Running the same assertions against both backends proves the SQLAlchemy adapter honors the
existing repository contracts and exception semantics.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from atlas_metadata import (
    URN,
    ArtifactMetadata,
    ColumnMetadata,
    DatasetMetadata,
    DuplicateArtifactError,
    DuplicateDatasetError,
    InMemoryMetadataRepository,
    InvalidURNError,
    MetadataRepository,
    SchemaConflictError,
    SchemaMetadata,
)
from atlas_metadata.repositories.sqlalchemy import SqlAlchemyMetadataRepository
from sqlalchemy.engine import URL


@pytest.fixture(params=["memory", "sqlalchemy"])
def repo(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[MetadataRepository]:
    if request.param == "memory":
        yield InMemoryMetadataRepository()
        return
    sql_repo = SqlAlchemyMetadataRepository.from_url(
        URL.create("sqlite", database=str(tmp_path / "catalog.db"))
    )
    try:
        yield sql_repo
    finally:
        sql_repo.dispose()


def _dataset(dataset_id: str = "orders", **overrides: object) -> DatasetMetadata:
    fields: dict[str, object] = {"dataset_id": dataset_id, "name": dataset_id.title()}
    fields.update(overrides)
    return DatasetMetadata(**fields)


def _columns(*names: str) -> tuple[ColumnMetadata, ...]:
    return tuple(ColumnMetadata(name=name, data_type="string") for name in names)


def _artifact(artifact_id: str, **overrides: object) -> ArtifactMetadata:
    fields: dict[str, object] = {
        "artifact_id": artifact_id,
        "dataset_id": "orders",
        "uri": f"/lake/{artifact_id}.parquet",
        "format": "parquet",
    }
    fields.update(overrides)
    return ArtifactMetadata(**fields)


# ----------------------------------------------------------------------
# Datasets
# ----------------------------------------------------------------------


def test_dataset_save_get_and_duplicate_rules(repo: MetadataRepository) -> None:
    saved = repo.save_dataset(_dataset(namespace="commerce", tags=["raw"]))
    assert saved.dataset_id == "orders"
    assert repo.get_dataset("orders") == saved
    assert repo.get_dataset("  orders  ") == saved
    assert repo.get_dataset("missing") is None

    with pytest.raises(DuplicateDatasetError):
        repo.save_dataset(_dataset(namespace="commerce"))
    # The same dataset_id in another namespace is also a duplicate (lookups are by dataset_id).
    with pytest.raises(DuplicateDatasetError):
        repo.save_dataset(_dataset(namespace="finance"))


def test_dataset_lookup_by_urn(repo: MetadataRepository) -> None:
    saved = repo.save_dataset(_dataset(namespace="commerce"))

    assert repo.get_dataset_by_urn("urn:atlas:dataset:commerce:orders") == saved
    assert repo.get_dataset_by_urn(URN.for_dataset("orders", namespace="commerce")) == saved
    assert repo.get_dataset_by_urn("urn:atlas:dataset:commerce:other") is None
    with pytest.raises(InvalidURNError):
        repo.get_dataset_by_urn("not-a-urn")


def test_dataset_overwrite_preserves_created_at_and_refreshes_updated_at(
    repo: MetadataRepository,
) -> None:
    original = repo.save_dataset(_dataset(owner="alice"))

    replacement = _dataset(owner="bob", description="changed")
    updated = repo.save_dataset(replacement, overwrite=True)

    assert updated.owner == "bob"
    assert updated.description == "changed"
    assert updated.created_at == original.created_at
    assert updated.updated_at >= original.updated_at
    assert len(repo.list_datasets()) == 1
    assert repo.get_dataset("orders") == updated


def test_dataset_filters(repo: MetadataRepository) -> None:
    repo.save_dataset(
        _dataset(
            "orders_bronze", layer="bronze", namespace="commerce", owner="ingest", tags=["raw"]
        )
    )
    repo.save_dataset(
        _dataset(
            "orders_silver",
            layer="silver",
            namespace="commerce",
            owner="analytics",
            tags=["curated", "orders"],
        )
    )
    repo.save_dataset(
        _dataset("events", layer="bronze", namespace="telemetry", owner="ingest", tags=["orders"])
    )

    assert [d.dataset_id for d in repo.list_datasets()] == [
        "orders_bronze",
        "orders_silver",
        "events",
    ]
    assert [d.dataset_id for d in repo.list_datasets(layer=" Bronze ")] == [
        "orders_bronze",
        "events",
    ]
    assert [d.dataset_id for d in repo.list_datasets(namespace="TELEMETRY")] == ["events"]
    assert [d.dataset_id for d in repo.list_datasets(owner="analytics")] == ["orders_silver"]
    assert [d.dataset_id for d in repo.list_datasets(tag="orders")] == ["orders_silver", "events"]
    assert [d.dataset_id for d in repo.list_datasets(layer="bronze", tag="orders")] == ["events"]
    assert repo.list_datasets(owner="nobody") == []


def test_dataset_delete(repo: MetadataRepository) -> None:
    repo.save_dataset(_dataset())
    assert repo.delete_dataset("orders") is True
    assert repo.delete_dataset("orders") is False
    assert repo.get_dataset("orders") is None
    assert repo.get_dataset_by_urn("urn:atlas:dataset:default:orders") is None
    # A deleted dataset can be registered again.
    repo.save_dataset(_dataset())


# ----------------------------------------------------------------------
# Schemas
# ----------------------------------------------------------------------


def test_schema_versions_conflicts_and_lookups(repo: MetadataRepository) -> None:
    v1 = repo.save_schema(SchemaMetadata(dataset_id="events", version=1, columns=_columns("a")))
    v2 = repo.register_next_schema("events", _columns("a", "b"), description="second")
    assert v2.version == 2

    with pytest.raises(SchemaConflictError):
        repo.save_schema(SchemaMetadata(dataset_id="events", version=1, columns=_columns("z")))

    assert repo.get_schema("events", 1) == v1
    assert repo.get_schema("events", 2) == v2
    assert repo.get_schema("events") == v2
    assert repo.get_schema("events", 99) is None
    assert repo.get_schema("unknown") is None
    assert repo.get_schema_by_urn(v1.urn) == v1
    assert repo.get_schema_by_urn("urn:atlas:schema:default:events.v2") == v2
    assert repo.get_schema_by_urn("urn:atlas:schema:default:events.v9") is None
    assert [s.version for s in repo.list_schemas("events")] == [1, 2]
    assert repo.list_schemas("unknown") == []


def test_register_next_schema_starts_at_one_and_honors_options(repo: MetadataRepository) -> None:
    schema = repo.register_next_schema(
        "fresh",
        _columns("id"),
        namespace="Analytics",
        schema_format="Avro",
        description="d",
        tags=["x", "x", "y"],
        custom_properties={"k": [1, 2]},
    )
    assert schema.version == 1
    assert schema.namespace == "analytics"
    assert schema.schema_format == "avro"
    assert schema.tags == ["x", "y"]
    assert schema.custom_properties == {"k": [1, 2]}
    assert repo.get_schema("fresh") == schema


def test_saving_a_schema_syncs_dataset_latest_schema_version(repo: MetadataRepository) -> None:
    repo.save_dataset(_dataset("events"))
    before = repo.get_dataset("events")
    assert before is not None
    assert before.latest_schema_version is None

    repo.register_next_schema("events", _columns("a"))
    repo.save_schema(SchemaMetadata(dataset_id="events", version=5, columns=_columns("a")))
    repo.save_schema(SchemaMetadata(dataset_id="events", version=3, columns=_columns("a")))

    dataset = repo.get_dataset("events")
    assert dataset is not None
    assert dataset.latest_schema_version == 5

    # Schemas may be registered for datasets that are not in the catalog.
    repo.register_next_schema("not_in_catalog", _columns("a"))
    assert repo.get_dataset("not_in_catalog") is None


# ----------------------------------------------------------------------
# Artifacts
# ----------------------------------------------------------------------


def test_artifact_save_get_urn_and_duplicate_rules(repo: MetadataRepository) -> None:
    saved = repo.save_artifact(_artifact("a1", execution_id="exec-1", row_count=10))
    assert repo.get_artifact("a1") == saved
    assert repo.get_artifact("missing") is None
    assert repo.get_artifact_by_urn("urn:atlas:artifact:default:a1") == saved
    assert repo.get_artifact_by_urn(saved.urn) == saved
    assert repo.get_artifact_by_urn("urn:atlas:artifact:default:nope") is None

    with pytest.raises(DuplicateArtifactError):
        repo.save_artifact(_artifact("a1"))


def test_artifact_overwrite_replaces_only_the_same_artifact(repo: MetadataRepository) -> None:
    repo.save_artifact(_artifact("a1", execution_id="exec-1", row_count=1))
    repo.save_artifact(_artifact("a2", execution_id="exec-2", row_count=2))

    replaced = repo.save_artifact(
        _artifact("a1", execution_id="exec-1", row_count=99), overwrite=True
    )

    assert replaced.row_count == 99
    assert repo.get_artifact("a1") == replaced
    second = repo.get_artifact("a2")
    assert second is not None and second.row_count == 2
    assert len(repo.list_artifacts()) == 2


def test_artifact_listing_filters(repo: MetadataRepository) -> None:
    repo.save_artifact(_artifact("a1", dataset_id="orders", execution_id="exec-1"))
    repo.save_artifact(_artifact("a2", dataset_id="orders", execution_id="exec-2"))
    repo.save_artifact(_artifact("a3", dataset_id="events", execution_id="exec-1"))

    assert [a.artifact_id for a in repo.list_artifacts()] == ["a1", "a2", "a3"]
    assert [a.artifact_id for a in repo.list_artifacts(dataset_id="orders")] == ["a1", "a2"]
    assert [a.artifact_id for a in repo.list_artifacts(execution_id="exec-1")] == ["a1", "a3"]
    assert [
        a.artifact_id for a in repo.list_artifacts(dataset_id="orders", execution_id="exec-1")
    ] == ["a1"]
    assert repo.list_artifacts(dataset_id="nobody") == []
