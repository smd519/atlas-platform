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
    Base,
    MetadataSession,
    SqlAlchemyMetadataRepository,
    SqlAlchemySchemaRepository,
    TenantOwnedMixin,
    create_session_factory,
    validate_workspace_ownership,
)
from atlas_metadata.repositories.sqlalchemy.models import (
    ArtifactRecord,
    DatasetRecord,
    SchemaColumnRecord,
    SchemaRecord,
)
from sqlalchemy import String, event, inspect, select, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.pool import QueuePool


def _url(path: Path) -> URL:
    return URL.create("sqlite", database=str(path))


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "workspace" / "metadata" / "catalog.db"


@pytest.fixture
def repo(db_path: Path) -> Iterator[SqlAlchemyMetadataRepository]:
    repository = SqlAlchemyMetadataRepository.from_url(_url(db_path), workspace_id="ws_1")
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
    first_repo = SqlAlchemyMetadataRepository.from_url(_url(db_path), workspace_id="ws_1")
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

    second_repo = SqlAlchemyMetadataRepository.from_url(_url(db_path), workspace_id="ws_1")
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
    repo = SqlAlchemyMetadataRepository.from_url(_url(db_path), workspace_id="ws_1")
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
    repo = SqlAlchemyMetadataRepository.from_url(_url(db_path), workspace_id="ws_1")
    try:
        service = MetadataService(repository=repo)
        service.register_dataset_metadata(DatasetMetadata(dataset_id="d", name="D"))
        updated = service.update_dataset("d", owner="bob", custom_properties={"a": 1})
        assert updated.owner == "bob"

        t0 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        for index in range(3):
            service.register_artifact(
                ArtifactMetadata(
                    artifact_id=f"art-{index}",
                    dataset_id="d",
                    uri=f"/x/{index}",
                    format="csv",
                    execution_id=f"exec-{index}",
                    created_at=t0 + timedelta(seconds=index),
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


def _dataset_record(
    dataset_id: str,
    namespace: str,
    urn: str,
    *,
    workspace_id: str = "ws_1",
) -> DatasetRecord:
    now = datetime.now(UTC)
    return DatasetRecord(
        workspace_id=workspace_id,
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


def test_database_enforces_dataset_uniqueness_within_workspace_and_allows_cross_workspace(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    assert repo.engine is not None
    with Session(repo.engine) as session:
        session.add(_dataset_record("d", "ns1", "urn:atlas:dataset:ns1:d", workspace_id="ws_1"))
        session.add(
            _dataset_record("d", "ns1", "urn:atlas:dataset:ns1:d", workspace_id="ws_2")
        )  # same dataset_id, namespace, and URN in another workspace is allowed
        session.commit()

    with Session(repo.engine) as session:
        session.add(
            _dataset_record("d", "ns1", "urn:atlas:dataset:ns1:d-other", workspace_id="ws_1")
        )
        with pytest.raises(IntegrityError):
            session.commit()

    with Session(repo.engine) as session:
        session.add(
            _dataset_record("e", "ns3", "urn:atlas:dataset:ns1:d", workspace_id="ws_1")
        )  # duplicate (workspace_id, urn)
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
                workspace_id=existing.workspace_id,
                schema_id="other",
                urn="urn:atlas:schema:commerce:other.v3",
                dataset_id=existing.dataset_id,
                namespace=existing.namespace,
                version=existing.version,  # (workspace_id, namespace, dataset_id, version) dup
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
        session.expunge(existing_artifact)
        session.add(
            ArtifactRecord(
                workspace_id=existing_artifact.workspace_id,
                artifact_id=existing_artifact.artifact_id,
                urn="urn:atlas:artifact:commerce:different",
                dataset_id="x",
                namespace=existing_artifact.namespace,  # (workspace_id, artifact_id) PK duplicate
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
    repo = SqlAlchemyMetadataRepository.from_url(
        _url(db_path), workspace_id="ws_1", max_allocation_attempts=25
    )
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
    repo = SqlAlchemyMetadataRepository.from_url(_url(db_path), workspace_id="ws_1")
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
    repo = SqlAlchemyMetadataRepository.from_url(
        _url(db_path), workspace_id="ws_1", max_allocation_attempts=3
    )
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
    repo = SqlAlchemyMetadataRepository.from_url(
        _url(db_path), workspace_id="ws_1", max_allocation_attempts=5
    )
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
    repo = SqlAlchemyMetadataRepository.from_url(_url(db_path), workspace_id="ws_1")
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
        SqlAlchemyMetadataRepository.from_url(
            _url(db_path), workspace_id="ws_1", max_allocation_attempts=0
        )


def test_invalid_workspace_id_is_rejected(db_path: Path) -> None:
    with pytest.raises(ValueError, match="workspace_id"):
        SqlAlchemyMetadataRepository.from_url(_url(db_path), workspace_id="")
    with pytest.raises(ValueError, match="workspace_id"):
        SqlAlchemyMetadataRepository.from_url(_url(db_path), workspace_id="   ")


# ----------------------------------------------------------------------
# Multi-tenancy schema structure, referential integrity, and isolation
# ----------------------------------------------------------------------


def test_tenant_owned_models_composite_keys_and_constraints(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    """Verify TenantOwnedMixin, composite PKs, composite FKs, and absence of surrogate IDs."""
    tables = Base.metadata.tables
    for model_cls in (DatasetRecord, SchemaRecord, SchemaColumnRecord, ArtifactRecord):
        assert issubclass(model_cls, TenantOwnedMixin)
        table = tables[model_cls.__tablename__]
        assert "id" not in table.c
        assert "schema_pk" not in table.c
        ws_col = table.c["workspace_id"]
        assert ws_col.nullable is False
        assert isinstance(ws_col.type, String)
        assert ws_col.type.length == 128

    assert [c.name for c in tables["atlas_datasets"].primary_key] == [
        "workspace_id",
        "dataset_id",
    ]
    assert [c.name for c in tables["atlas_schemas"].primary_key] == [
        "workspace_id",
        "schema_id",
    ]
    assert [c.name for c in tables["atlas_schema_columns"].primary_key] == [
        "workspace_id",
        "schema_id",
        "position",
    ]
    assert [c.name for c in tables["atlas_artifacts"].primary_key] == [
        "workspace_id",
        "artifact_id",
    ]

    # Verify composite foreign key on atlas_schema_columns and no single-column FKs anywhere
    fk_constraints = list(tables["atlas_schema_columns"].foreign_key_constraints)
    assert len(fk_constraints) == 1
    fk = fk_constraints[0]
    assert [c.name for c in fk.columns] == ["workspace_id", "schema_id"]
    assert [el.target_fullname for el in fk.elements] == [
        "atlas_schemas.workspace_id",
        "atlas_schemas.schema_id",
    ]
    assert fk.ondelete == "CASCADE"

    for table in tables.values():
        for fkc in table.foreign_key_constraints:
            assert len(fkc.columns) >= 2
            assert fkc.columns.keys()[0] == "workspace_id"

    assert repo.engine is not None
    inspector = inspect(repo.engine)
    assert inspector.get_pk_constraint("atlas_datasets")["constrained_columns"] == [
        "workspace_id",
        "dataset_id",
    ]
    assert inspector.get_pk_constraint("atlas_schemas")["constrained_columns"] == [
        "workspace_id",
        "schema_id",
    ]
    assert inspector.get_pk_constraint("atlas_schema_columns")["constrained_columns"] == [
        "workspace_id",
        "schema_id",
        "position",
    ]
    assert inspector.get_pk_constraint("atlas_artifacts")["constrained_columns"] == [
        "workspace_id",
        "artifact_id",
    ]


def test_cross_workspace_foreign_key_reference_is_rejected_by_sqlite(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    """A SchemaColumnRecord in workspace_B cannot reference a SchemaRecord in workspace_A."""
    repo.save_schema(_full_schema())  # persisted under repo.workspace_id == "ws_1"
    assert repo.engine is not None

    with Session(repo.engine) as session:
        session.add(
            SchemaColumnRecord(
                workspace_id="ws_2",
                schema_id=_full_schema().schema_id,
                position=0,
                name="hacked_col",
                data_type="string",
                nullable=True,
                primary_key=False,
                description=None,
                default_value=None,
                tags=[],
                custom_properties={},
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()


def test_multi_workspace_coexistence_and_isolation_in_shared_database(db_path: Path) -> None:
    """Two workspaces sharing one SQLite DB can store identical IDs/URNs with full isolation."""
    url = _url(db_path)
    repo_a = SqlAlchemyMetadataRepository.from_url(url, workspace_id="workspace_A")
    repo_b = SqlAlchemyMetadataRepository.from_url(url, workspace_id="workspace_B")
    try:
        assert repo_a.workspace_id == "workspace_A"
        assert repo_b.workspace_id == "workspace_B"

        # 1. Identical dataset_id and URN in both workspaces
        ds_a = _full_dataset().model_copy(update={"owner": "owner_a", "description": "Dataset A"})
        ds_b = _full_dataset().model_copy(update={"owner": "owner_b", "description": "Dataset B"})
        repo_a.save_dataset(ds_a)
        repo_b.save_dataset(ds_b)

        assert repo_a.get_dataset("orders_silver") == ds_a
        assert repo_b.get_dataset("orders_silver") == ds_b
        assert repo_a.get_dataset_by_urn(ds_a.urn) == ds_a
        assert repo_b.get_dataset_by_urn(ds_b.urn) == ds_b
        assert repo_a.list_datasets() == [ds_a]
        assert repo_b.list_datasets() == [ds_b]

        # A dataset only in workspace_A is invisible to workspace_B
        only_a = DatasetMetadata(dataset_id="only_in_a", name="Only A")
        repo_a.save_dataset(only_a)
        assert repo_b.get_dataset("only_in_a") is None
        assert repo_b.get_dataset_by_urn(only_a.urn) is None
        assert repo_b.delete_dataset("only_in_a") is False
        assert repo_a.get_dataset("only_in_a") == only_a

        # Overwriting in workspace_B never mutates workspace_A
        ds_b_updated = ds_b.model_copy(update={"owner": "owner_b_v2"})
        repo_b.save_dataset(ds_b_updated, overwrite=True)
        assert repo_a.get_dataset("orders_silver") == ds_a
        assert (
            repo_b.get_dataset("orders_silver") is not None
            and repo_b.get_dataset("orders_silver").owner == "owner_b_v2"  # type: ignore[union-attr]
        )

        # 2. Independent schema version allocation and lookup per workspace
        s_a1 = repo_a.register_next_schema(
            "orders_silver",
            (ColumnMetadata(name="a1", data_type="int"),),
            namespace="commerce",
        )
        s_a2 = repo_a.register_next_schema(
            "orders_silver",
            (
                ColumnMetadata(name="a1", data_type="int"),
                ColumnMetadata(name="a2", data_type="str"),
            ),
            namespace="commerce",
        )
        s_b1 = repo_b.register_next_schema(
            "orders_silver",
            (ColumnMetadata(name="b1", data_type="string"),),
            namespace="commerce",
        )
        assert s_a1.version == 1
        assert s_a2.version == 2
        assert s_b1.version == 1
        assert s_a1.schema_id == s_b1.schema_id
        assert s_a1.urn == s_b1.urn

        assert repo_a.get_schema("orders_silver", 1) == s_a1
        assert repo_b.get_schema("orders_silver", 1) == s_b1
        assert repo_b.get_schema("orders_silver", 2) is None
        assert repo_a.get_schema_by_urn(s_a1.urn) == s_a1
        assert repo_b.get_schema_by_urn(s_b1.urn) == s_b1
        assert [s.version for s in repo_a.list_schemas("orders_silver")] == [1, 2]
        assert [s.version for s in repo_b.list_schemas("orders_silver")] == [1]

        # 3. Identical artifact_id and URN in both workspaces
        art_a = _full_artifact().model_copy(update={"row_count": 100})
        art_b = _full_artifact().model_copy(update={"row_count": 200})
        repo_a.save_artifact(art_a)
        repo_b.save_artifact(art_b)

        assert repo_a.get_artifact("art-1") == art_a
        assert repo_b.get_artifact("art-1") == art_b
        assert repo_a.get_artifact_by_urn(art_a.urn) == art_a
        assert repo_b.get_artifact_by_urn(art_b.urn) == art_b
        assert repo_a.list_artifacts() == [art_a]
        assert repo_b.list_artifacts() == [art_b]

        # Deleting dataset in workspace_B leaves workspace_A untouched
        assert repo_b.delete_dataset("orders_silver") is True
        assert repo_b.get_dataset("orders_silver") is None
        assert repo_a.get_dataset("orders_silver") is not None
    finally:
        repo_a.dispose()
        repo_b.dispose()


def test_deterministic_ordering_for_list_datasets_and_list_artifacts(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    """Verify deterministic ordering by (created_at, dataset_id) and (created_at, artifact_id)."""
    t1 = datetime(2024, 5, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2024, 5, 1, 12, 0, 1, tzinfo=UTC)

    # Insert out of order; tie on t1 is broken by dataset_id ascending
    repo.save_dataset(DatasetMetadata(dataset_id="ds_c", name="C", created_at=t2))
    repo.save_dataset(DatasetMetadata(dataset_id="ds_b", name="B", created_at=t1))
    repo.save_dataset(DatasetMetadata(dataset_id="ds_a", name="A", created_at=t1))

    assert [d.dataset_id for d in repo.list_datasets()] == ["ds_a", "ds_b", "ds_c"]

    # Insert artifacts out of order; tie on t1 is broken by artifact_id ascending
    repo.save_artifact(
        ArtifactMetadata(
            artifact_id="art_c", dataset_id="ds_a", uri="/c", format="csv", created_at=t2
        )
    )
    repo.save_artifact(
        ArtifactMetadata(
            artifact_id="art_b", dataset_id="ds_a", uri="/b", format="csv", created_at=t1
        )
    )
    repo.save_artifact(
        ArtifactMetadata(
            artifact_id="art_a", dataset_id="ds_a", uri="/a", format="csv", created_at=t1
        )
    )

    assert [a.artifact_id for a in repo.list_artifacts()] == ["art_a", "art_b", "art_c"]


# ----------------------------------------------------------------------
# Milestone 3: Write-path ownership validation (Tests A - G)
# ----------------------------------------------------------------------


class _GlobalTestBase(DeclarativeBase):
    pass


class _PlatformGlobalRecord(_GlobalTestBase):
    """Non-tenant entity used to verify global entities are unaffected by before_flush."""

    __tablename__ = "atlas_test_platform_global"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(String(128), nullable=False)


def test_ownership_validation_a_valid_insertion(repo: SqlAlchemyMetadataRepository) -> None:
    """Test A: Repository operating under workspace_a inserts an entity stamped with workspace_a."""
    assert repo.engine is not None
    factory = create_session_factory(repo.engine)

    with factory(info={"workspace_id": "workspace_a"}) as session, session.begin():
        assert isinstance(session, MetadataSession)
        session.add(
            _dataset_record(
                "valid_ds",
                "default",
                "urn:atlas:dataset:default:valid_ds",
                workspace_id="workspace_a",
            )
        )

    repo_a = SqlAlchemyMetadataRepository(factory, workspace_id="workspace_a")
    fetched = repo_a.get_dataset("valid_ds")
    assert fetched is not None
    assert fetched.dataset_id == "valid_ds"


def test_ownership_validation_b_mismatch_on_insertion_rejected(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    """Test B: Session associated with workspace_a rejects inserting an entity for workspace_b."""
    assert repo.engine is not None
    factory = create_session_factory(repo.engine)

    with pytest.raises(ValueError, match="Workspace ownership mismatch on insert"):
        with factory(info={"workspace_id": "workspace_a"}) as session, session.begin():
            session.add(
                _dataset_record(
                    "bad_ds",
                    "default",
                    "urn:atlas:dataset:default:bad_ds",
                    workspace_id="workspace_b",
                )
            )


def test_ownership_validation_c_mutation_of_persistent_workspace_id_rejected(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    """Test C: Mutating workspace_id of a persistent entity raises the exact ValueError."""
    assert repo.engine is not None
    factory = create_session_factory(repo.engine)

    with factory(info={"workspace_id": "workspace_a"}) as session, session.begin():
        session.add(
            _dataset_record(
                "immutable_ds",
                "default",
                "urn:atlas:dataset:default:immutable_ds",
                workspace_id="workspace_a",
            )
        )

    with pytest.raises(ValueError, match="^Cannot mutate workspace_id of a persistent entity$"):
        with factory(info={"workspace_id": "workspace_a"}) as session, session.begin():
            persisted = session.scalar(
                select(DatasetRecord).where(
                    DatasetRecord.workspace_id == "workspace_a",
                    DatasetRecord.dataset_id == "immutable_ds",
                )
            )
            assert persisted is not None
            persisted.workspace_id = "workspace_b"
            session.flush()


def test_ownership_validation_d_cross_workspace_modification_rejected(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    """Test D: Modifying an entity belonging to workspace_b in a workspace_a session is rejected."""
    assert repo.engine is not None
    factory = create_session_factory(repo.engine)

    with factory(info={"workspace_id": "workspace_b"}) as session, session.begin():
        session.add(
            _dataset_record(
                "b_ds",
                "default",
                "urn:atlas:dataset:default:b_ds",
                workspace_id="workspace_b",
            )
        )

    with pytest.raises(ValueError, match="Workspace ownership mismatch on update"):
        with factory(info={"workspace_id": "workspace_a"}) as session, session.begin():
            foreign_record = session.scalar(
                select(DatasetRecord).where(
                    DatasetRecord.workspace_id == "workspace_b",
                    DatasetRecord.dataset_id == "b_ds",
                )
            )
            assert foreign_record is not None
            foreign_record.owner = "tampered"
            session.flush()


def test_ownership_validation_e_cross_workspace_deletion_rejected(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    """Test E: Deleting an entity belonging to workspace_b in a workspace_a session is rejected."""
    assert repo.engine is not None
    factory = create_session_factory(repo.engine)

    with factory(info={"workspace_id": "workspace_b"}) as session, session.begin():
        session.add(
            _dataset_record(
                "b_del_ds",
                "default",
                "urn:atlas:dataset:default:b_del_ds",
                workspace_id="workspace_b",
            )
        )

    # Direct delete attempt under workspace_a session
    with pytest.raises(ValueError, match="Workspace ownership mismatch on delete"):
        with factory(info={"workspace_id": "workspace_a"}) as session, session.begin():
            foreign_record = session.scalar(
                select(DatasetRecord).where(
                    DatasetRecord.workspace_id == "workspace_b",
                    DatasetRecord.dataset_id == "b_del_ds",
                )
            )
            assert foreign_record is not None
            session.delete(foreign_record)
            session.flush()

    # Delete attempt after mutating workspace_id in memory to match session workspace_a
    with pytest.raises(ValueError, match="Workspace ownership mismatch on delete"):
        with factory(info={"workspace_id": "workspace_a"}) as session, session.begin():
            foreign_record = session.scalar(
                select(DatasetRecord).where(
                    DatasetRecord.workspace_id == "workspace_b",
                    DatasetRecord.dataset_id == "b_del_ds",
                )
            )
            assert foreign_record is not None
            foreign_record.workspace_id = "workspace_a"
            session.delete(foreign_record)
            session.flush()


def test_ownership_validation_f_missing_workspace_identity_fails_closed(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    """Test F: Flushing a tenant-owned entity without a valid session workspace_id fails closed."""
    assert repo.engine is not None
    factory = create_session_factory(repo.engine)

    for invalid_info in ({}, {"workspace_id": None}, {"workspace_id": "   "}):
        with pytest.raises(ValueError, match="without a valid 'workspace_id' in session.info"):
            with factory(info=invalid_info) as session, session.begin():
                session.add(
                    _dataset_record(
                        "unscoped_ds",
                        "default",
                        "urn:atlas:dataset:default:unscoped_ds",
                        workspace_id="workspace_a",
                    )
                )


def test_ownership_validation_g_global_entities_and_global_session_unaffected(
    repo: SqlAlchemyMetadataRepository,
) -> None:
    """Test G: Non-TenantOwnedMixin entities flush without workspace_id and Session is untouched."""
    assert repo.engine is not None
    assert not event.contains(Session, "before_flush", validate_workspace_ownership)

    _GlobalTestBase.metadata.create_all(repo.engine)
    factory = create_session_factory(repo.engine)

    # Flushing a platform-global entity through MetadataSession without workspace_id succeeds
    with factory() as session, session.begin():
        session.add(_PlatformGlobalRecord(key="version", value="1.0.0"))

    with factory() as session:
        loaded = session.get(_PlatformGlobalRecord, "version")
        assert loaded is not None
        assert loaded.value == "1.0.0"
