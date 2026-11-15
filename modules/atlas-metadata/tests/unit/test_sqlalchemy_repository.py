"""SQLAlchemy-specific tests: round trips, persistence, constraints, and concurrency."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from atlas_core import Dataset, MetadataProvider
from atlas_metadata import (
    URN,
    ArtifactMetadata,
    ColumnMetadata,
    DatasetMetadata,
    MetadataRepositoryError,
    MetadataService,
    SchemaConflictError,
    SchemaMetadata,
)
from atlas_metadata.repositories.sqlalchemy import (
    SqlAlchemyMetadataRepository,
    SqlAlchemySchemaRepository,
)
from atlas_metadata.repositories.sqlalchemy.models import (
    ArtifactRecord,
    DatasetRecord,
    SchemaRecord,
)
from sqlalchemy import select, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.pool import QueuePool


def _url(path: Path) -> URL:
    return URL.create("sqlite", database=str(path))


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "workspace" / "metadata" / "catalog.db"


@pytest.fixture
def repo(db_path: Path) -> Iterator[SqlAlchemyMetadataRepository]:
    repository = SqlAlchemyMetadataRepository.from_url(_url(db_path))
    try:
        yield repository
    finally:
        repository.dispose()


def _full_dataset() -> DatasetMetadata:
    return DatasetMetadata(
        dataset_id="orders_silver",
        urn=URN.for_dataset("orders_silver", namespace="commerce"),
        name="Curated Orders",
        layer="silver",
        source_id="src-erp",
        format="parquet",
        location_uri="/lake/silver/orders",
        description="Curated orders",
        owner="analytics",
        tags=["orders", "curated"],
        upstream_urns=["urn:atlas:dataset:commerce:orders_bronze", "urn:atlas:dataset:commerce:x"],
        custom_properties={
            "pii": False,
            "retention_days": 365,
            "ratio": 0.25,
            "nested": {"list": [1, "two", None, {"k": True}]},
            "latest_quality_assessment": {"overall_status": "PASSED", "quality_score": 100.0},
        },
        latest_schema_version=3,
        created_at=datetime(2024, 1, 2, 3, 4, 5, 678901, tzinfo=UTC),
        updated_at=datetime(2024, 2, 3, 4, 5, 6, 789012, tzinfo=UTC),
    )


def _full_schema() -> SchemaMetadata:
    return SchemaMetadata(
        dataset_id="orders_silver",
        namespace="commerce",
        version=3,
        columns=(
            ColumnMetadata(name="zeta", data_type="STRING", description="last alphabetically"),
            ColumnMetadata(
                name="alpha",
                data_type="int",
                nullable=False,
                primary_key=True,
                default_value=0,
                tags=["key"],
                custom_properties={"unit": "count"},
            ),
            ColumnMetadata(name="mid", data_type="json", default_value={"a": [1, 2]}),
            ColumnMetadata(name="nodefault", data_type="string", default_value=None),
        ),
        schema_format="parquet",
        description="v3",
        tags=["curated"],
        custom_properties={"owner": {"team": "data"}},
        created_at=datetime(2024, 3, 4, 5, 6, 7, 890123, tzinfo=UTC),
    )


def _full_artifact() -> ArtifactMetadata:
    return ArtifactMetadata(
        artifact_id="art-1",
        dataset_id="orders_silver",
        namespace="commerce",
        uri="/lake/silver/orders/part-0.parquet",
        format="parquet",
        layer="silver",
        execution_id="exec-42",
        schema_version=3,
        row_count=123456789012,
        byte_size=987654321098,
        checksum="sha256:abc",
        tags=["part"],
        custom_properties={"rows_by_day": {"2024-01-01": 5}},
        created_at=datetime(2024, 4, 5, 6, 7, 8, 901234, tzinfo=UTC),
    )


# ----------------------------------------------------------------------
# Domain round-trip integrity
# ----------------------------------------------------------------------


def test_dataset_round_trip_preserves_every_field(repo: SqlAlchemyMetadataRepository) -> None:
    original = _full_dataset()
    saved = repo.save_dataset(original)
    fetched = repo.get_dataset("orders_silver")

    assert saved == original
    assert fetched == original
    assert fetched is not None
    assert fetched.created_at.tzinfo is not None
    assert fetched.created_at.utcoffset() == timedelta(0)
    assert fetched.custom_properties["nested"]["list"] == [1, "two", None, {"k": True}]
    assert repo.get_dataset_by_urn("urn:atlas:dataset:commerce:orders_silver") == original


def test_schema_round_trip_preserves_fields_and_column_order(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    original = _full_schema()
    repo.save_schema(original)
    fetched = repo.get_schema("orders_silver", 3)

    assert fetched == original
    assert fetched is not None
    assert [c.name for c in fetched.columns] == ["zeta", "alpha", "mid", "nodefault"]
    assert fetched.get_column("alpha") == original.get_column("alpha")
    no_default = fetched.get_column("nodefault")
    assert no_default is not None
    assert no_default.default_value is None
    assert fetched.urn == URN.for_schema("orders_silver", 3, namespace="commerce")
    assert repo.get_schema_by_urn(original.urn) == original


def test_artifact_round_trip_preserves_every_field(repo: SqlAlchemyMetadataRepository) -> None:
    original = _full_artifact()
    repo.save_artifact(original)

    assert repo.get_artifact("art-1") == original
    assert repo.get_artifact_by_urn(original.urn) == original
    assert repo.list_artifacts(execution_id="exec-42") == [original]


def test_non_utc_timestamps_are_normalized_to_utc(repo: SqlAlchemyMetadataRepository) -> None:
    eastern = timezone(timedelta(hours=-5))
    local = datetime(2024, 6, 1, 12, 0, 0, tzinfo=eastern)
    repo.save_artifact(
        ArtifactMetadata(artifact_id="tz", dataset_id="d", uri="/x", format="csv", created_at=local)
    )

    fetched = repo.get_artifact("tz")
    assert fetched is not None
    assert fetched.created_at == local
    assert fetched.created_at.utcoffset() == timedelta(0)
    assert fetched.created_at.hour == 17


def test_returned_models_do_not_alias_stored_state(repo: SqlAlchemyMetadataRepository) -> None:
    repo.save_dataset(_full_dataset())
    first = repo.get_dataset("orders_silver")
    assert first is not None
    first.custom_properties["nested"]["list"].append("mutated")
    first.tags.append("mutated")

    second = repo.get_dataset("orders_silver")
    assert second == _full_dataset()


# ----------------------------------------------------------------------
# Persistence across restarts
# ----------------------------------------------------------------------


def test_metadata_survives_process_restart_through_metadata_service(db_path: Path) -> None:
    first_repo = SqlAlchemyMetadataRepository.from_url(_url(db_path))
    service = MetadataService(repository=first_repo)
    assert isinstance(service, MetadataProvider)
    service.register_dataset_metadata(_full_dataset())
    service.register_schema(
        "orders_silver",
        [ColumnMetadata(name="id", data_type="int", primary_key=True)],
        namespace="commerce",
    )
    service.register_artifact(_full_artifact())
    first_repo.dispose()  # simulate process exit

    second_repo = SqlAlchemyMetadataRepository.from_url(_url(db_path))
    try:
        restarted = MetadataService(repository=second_repo)
        dataset = restarted.get_dataset_metadata("orders_silver")
        assert dataset is not None
        assert dataset.custom_properties == _full_dataset().custom_properties
        assert dataset.upstream_urns == _full_dataset().upstream_urns

        schema = restarted.get_latest_schema("orders_silver")
        assert schema is not None and schema.version == 1
        assert [c.name for c in schema.columns] == ["id"]

        assert restarted.get_artifact("art-1") == _full_artifact()
        assert restarted.get_dataset("orders_silver") is not None  # atlas_core contract
    finally:
        second_repo.dispose()


def test_core_dataset_update_round_trip_through_service_preserves_catalog_fields(
    db_path: Path,
) -> None:
    """get_dataset -> edit metadata -> register_dataset_metadata(overwrite) over SQLAlchemy."""
    repo = SqlAlchemyMetadataRepository.from_url(_url(db_path))
    try:
        service = MetadataService(repository=repo)
        service.register_dataset_metadata(_full_dataset())
        service.register_schema(
            "orders_silver", [ColumnMetadata(name="id", data_type="int")], namespace="commerce"
        )
        before = service.get_dataset_metadata("orders_silver")
        assert before is not None

        core = service.get_dataset("orders_silver")
        assert isinstance(core, Dataset)
        props = dict(core.metadata)
        props["latest_quality_assessment"] = {"overall_status": "FAILED"}
        service.register_dataset_metadata(
            core.model_copy(update={"metadata": props}), overwrite=True
        )

        after = service.get_dataset_metadata("orders_silver")
        assert after is not None
        assert after.latest_schema_version == before.latest_schema_version
        assert after.created_at == before.created_at
        assert after.custom_properties["latest_quality_assessment"] == {"overall_status": "FAILED"}
        assert after.upstream_urns == before.upstream_urns
    finally:
        repo.dispose()


def test_service_update_dataset_and_artifact_history_over_sqlalchemy(db_path: Path) -> None:
    repo = SqlAlchemyMetadataRepository.from_url(_url(db_path))
    try:
        service = MetadataService(repository=repo)
        service.register_dataset_metadata(DatasetMetadata(dataset_id="d", name="D"))
        updated = service.update_dataset("d", owner="bob", custom_properties={"a": 1})
        assert updated.owner == "bob"

        for index in range(3):
            service.register_artifact(
                ArtifactMetadata(
                    artifact_id=f"art-{index}",
                    dataset_id="d",
                    uri=f"/x/{index}",
                    format="csv",
                    execution_id=f"exec-{index}",
                ),
                overwrite=True,
            )
        assert [a.artifact_id for a in service.list_artifacts(dataset_id="d")] == [
            "art-0",
            "art-1",
            "art-2",
        ]
    finally:
        repo.dispose()


# ----------------------------------------------------------------------
# Database-level constraints
# ----------------------------------------------------------------------


def _dataset_record(dataset_id: str, namespace: str, urn: str) -> DatasetRecord:
    now = datetime.now(UTC)
    return DatasetRecord(
        dataset_id=dataset_id,
        urn=urn,
        name=dataset_id,
        namespace=namespace,
        layer="bronze",
        tags=[],
        upstream_urns=[],
        custom_properties={},
        created_at=now,
        updated_at=now,
    )


def test_database_enforces_dataset_uniqueness_including_namespace(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    assert repo.engine is not None
    with Session(repo.engine) as session:
        session.add(_dataset_record("d", "ns1", "urn:atlas:dataset:ns1:d"))
        session.add(
            _dataset_record("d", "ns2", "urn:atlas:dataset:ns2:d")
        )  # different namespace OK
        session.commit()

    with Session(repo.engine) as session:
        session.add(_dataset_record("d", "ns1", "urn:atlas:dataset:ns1:d-other"))
        with pytest.raises(IntegrityError):
            session.commit()

    with Session(repo.engine) as session:
        session.add(_dataset_record("e", "ns3", "urn:atlas:dataset:ns1:d"))  # duplicate URN
        with pytest.raises(IntegrityError):
            session.commit()


def test_database_enforces_schema_and_artifact_uniqueness(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    repo.save_schema(_full_schema())
    repo.save_artifact(_full_artifact())
    assert repo.engine is not None

    with Session(repo.engine) as session:
        existing = session.scalars(select(SchemaRecord)).one()
        session.add(
            SchemaRecord(
                schema_id="other",
                urn="urn:atlas:schema:commerce:other.v3",
                dataset_id=existing.dataset_id,
                namespace=existing.namespace,
                version=existing.version,  # (namespace, dataset_id, version) duplicate
                schema_format="tabular",
                tags=[],
                custom_properties={},
                created_at=datetime.now(UTC),
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()

    with Session(repo.engine) as session:
        existing_artifact = session.scalars(select(ArtifactRecord)).one()
        session.add(
            ArtifactRecord(
                artifact_id=existing_artifact.artifact_id,
                urn="urn:atlas:artifact:commerce:different",
                dataset_id="x",
                namespace=existing_artifact.namespace,  # (namespace, artifact_id) duplicate
                uri="/y",
                format="csv",
                tags=[],
                custom_properties={},
                created_at=datetime.now(UTC),
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()


def test_schema_columns_are_cascade_deleted_with_their_schema(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    repo.save_schema(_full_schema())
    assert repo.engine is not None
    with Session(repo.engine) as session:
        record = session.scalars(select(SchemaRecord)).one()
        assert len(record.columns) == 4
        session.delete(record)
        session.commit()
    with repo.engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM atlas_schema_columns")).scalar() == 0


# ----------------------------------------------------------------------
# Concurrency and bounded retries
# ----------------------------------------------------------------------


def test_concurrent_register_next_schema_allocates_distinct_versions(db_path: Path) -> None:
    repo = SqlAlchemyMetadataRepository.from_url(_url(db_path), max_allocation_attempts=25)
    try:
        repo.save_dataset(DatasetMetadata(dataset_id="stream_ds", name="Stream"))
        workers = 8
        barrier = threading.Barrier(workers)

        def register(worker_id: int) -> int:
            barrier.wait()
            schema = repo.register_next_schema(
                "stream_ds",
                (
                    ColumnMetadata(name="id", data_type="int"),
                    ColumnMetadata(name=f"v_{worker_id}", data_type="string"),
                ),
            )
            return schema.version

        with ThreadPoolExecutor(max_workers=workers) as pool:
            versions = [
                f.result(timeout=60) for f in [pool.submit(register, i) for i in range(workers)]
            ]

        assert sorted(versions) == list(range(1, workers + 1))
        assert [s.version for s in repo.list_schemas("stream_ds")] == list(range(1, workers + 1))
        dataset = repo.get_dataset("stream_ds")
        assert dataset is not None and dataset.latest_schema_version == workers
        assert repo.engine is not None
        with repo.engine.connect() as connection:
            columns = connection.execute(text("SELECT COUNT(*) FROM atlas_schema_columns")).scalar()
        assert columns == workers * 2  # no orphaned or partial rows from retried attempts
    finally:
        repo.dispose()


def test_concurrent_same_version_save_schema_succeeds_exactly_once(db_path: Path) -> None:
    repo = SqlAlchemyMetadataRepository.from_url(_url(db_path))
    try:
        workers = 8
        barrier = threading.Barrier(workers)
        successes: list[SchemaMetadata] = []
        conflicts: list[SchemaConflictError] = []
        unexpected: list[Exception] = []
        lock = threading.Lock()

        def save(worker_id: int) -> None:
            schema = SchemaMetadata(
                dataset_id="same_version",
                version=1,
                columns=(ColumnMetadata(name=f"c_{worker_id}", data_type="int"),),
            )
            barrier.wait()
            try:
                saved = repo.save_schema(schema)
                with lock:
                    successes.append(saved)
            except SchemaConflictError as exc:
                with lock:
                    conflicts.append(exc)
            except Exception as exc:
                with lock:
                    unexpected.append(exc)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for future in [pool.submit(save, i) for i in range(workers)]:
                future.result(timeout=60)

        assert unexpected == []
        assert len(successes) == 1
        assert len(conflicts) == workers - 1
        assert repo.get_schema("same_version", 1) == successes[0]
    finally:
        repo.dispose()


def test_allocation_conflicts_are_retried_only_a_bounded_number_of_times(
    db_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = SqlAlchemyMetadataRepository.from_url(_url(db_path), max_allocation_attempts=3)
    try:
        calls = 0

        def always_conflict(*_args: object, **_kwargs: object) -> None:
            nonlocal calls
            calls += 1
            raise SchemaConflictError("simulated allocation conflict", dataset_id="d")

        monkeypatch.setattr(SqlAlchemySchemaRepository, "_insert_schema", always_conflict)
        with pytest.raises(SchemaConflictError, match="after 3 attempts"):
            repo.register_next_schema("d", (ColumnMetadata(name="id", data_type="int"),))
        assert calls == 3
    finally:
        repo.dispose()


def test_unrecognized_integrity_errors_are_not_retried(
    db_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = SqlAlchemyMetadataRepository.from_url(_url(db_path), max_allocation_attempts=5)
    try:
        calls = 0

        def unrelated_integrity_error(*_args: object, **_kwargs: object) -> None:
            nonlocal calls
            calls += 1
            raise IntegrityError("INSERT ...", None, Exception("unrelated constraint"))

        monkeypatch.setattr(SqlAlchemySchemaRepository, "_insert_schema", unrelated_integrity_error)
        with pytest.raises(MetadataRepositoryError):
            repo.register_next_schema("d", (ColumnMetadata(name="id", data_type="int"),))
        assert calls == 1
    finally:
        repo.dispose()


def test_unexpected_database_failures_surface_as_repository_errors(
    db_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = SqlAlchemyMetadataRepository.from_url(_url(db_path))
    try:

        def broken(*_args: object, **_kwargs: object) -> None:
            raise OperationalError("SELECT 1", None, Exception("disk I/O error"))

        monkeypatch.setattr(Session, "scalar", broken)
        with pytest.raises(MetadataRepositoryError, match="get_dataset"):
            repo.get_dataset("anything")
        monkeypatch.undo()

        assert repo.engine is not None
        pool = repo.engine.pool
        assert isinstance(pool, QueuePool)
        assert pool.checkedout() == 0
    finally:
        repo.dispose()


def test_invalid_max_allocation_attempts_is_rejected(db_path: Path) -> None:
    with pytest.raises(ValueError, match="max_allocation_attempts"):
        SqlAlchemyMetadataRepository.from_url(_url(db_path), max_allocation_attempts=0)
