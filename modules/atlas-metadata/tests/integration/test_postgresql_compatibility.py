"""PostgreSQL integration tests for multi-tenant schema, constraints, and ownership validation.

All tests in this module are marked with ``@pytest.mark.postgresql`` and use distinct
``workspace_id`` prefixes per test invocation so that records coexist safely in a persistent
PostgreSQL database without destructive ``TRUNCATE`` or ``drop_all()`` operations.
"""

from __future__ import annotations

import os
import threading
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest
from atlas_metadata import (
    URN,
    ArtifactMetadata,
    ColumnMetadata,
    DatasetMetadata,
    DuplicateArtifactError,
    DuplicateDatasetError,
    SchemaConflictError,
    SchemaMetadata,
)
from atlas_metadata.repositories.sqlalchemy import (
    MetadataSession,
    SqlAlchemyMetadataRepository,
    create_metadata_engine,
    create_session_factory,
    initialize_database,
    validate_workspace_ownership,
)
from atlas_metadata.repositories.sqlalchemy.models import (
    DatasetRecord,
    SchemaColumnRecord,
)
from sqlalchemy import Engine, String, event, inspect, select, text
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

DEFAULT_POSTGRES_URL = "postgresql+psycopg://atlas:atlas@127.0.0.1:5432/atlas"

pytestmark = pytest.mark.postgresql


def _postgres_url() -> str:
    return (
        os.environ.get("ATLAS_TEST_DATABASE_URL", DEFAULT_POSTGRES_URL).strip()
        or DEFAULT_POSTGRES_URL
    )


@pytest.fixture(scope="module")
def pg_engine() -> Iterator[Engine]:
    """Connect to the configured PostgreSQL instance and ensure metadata tables exist."""
    url = _postgres_url()
    engine = create_metadata_engine(url, connect_args={"connect_timeout": 3})
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError as exc:
        engine.dispose()
        pytest.skip(f"PostgreSQL is not reachable at '{url}': {exc}")

    initialize_database(engine)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def ws_pair() -> tuple[str, str]:
    """Return a pair of unique workspace IDs for non-destructive test isolation."""
    suffix = uuid.uuid4().hex[:10]
    return f"test_ws_a_{suffix}", f"test_ws_b_{suffix}"


class _PgGlobalBase(DeclarativeBase):
    pass


class _PgGlobalConfigRecord(_PgGlobalBase):
    __tablename__ = "atlas_pg_test_global_config"

    config_key: Mapped[str] = mapped_column(String(128), primary_key=True)
    config_value: Mapped[str] = mapped_column(String(255), nullable=False)


def _make_dataset_record(dataset_id: str, workspace_id: str) -> DatasetRecord:
    now = datetime.now(UTC)
    return DatasetRecord(
        workspace_id=workspace_id,
        dataset_id=dataset_id,
        urn=f"urn:atlas:dataset:default:{dataset_id}",
        name=dataset_id,
        namespace="default",
        layer="bronze",
        tags=[],
        upstream_urns=[],
        custom_properties={},
        created_at=now,
        updated_at=now,
    )


def test_postgresql_composite_keys_and_constraints_introspection(pg_engine: Engine) -> None:
    """Verify composite PKs, composite FKs, and workspace-scoped unique constraints in Postgres."""
    inspector = inspect(pg_engine)
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

    fks = inspector.get_foreign_keys("atlas_schema_columns")
    assert len(fks) == 1
    assert fks[0]["constrained_columns"] == ["workspace_id", "schema_id"]
    assert fks[0]["referred_table"] == "atlas_schemas"
    assert fks[0]["referred_columns"] == ["workspace_id", "schema_id"]


def test_postgresql_multi_workspace_coexistence_crud_and_isolation(
    pg_engine: Engine,
    ws_pair: tuple[str, str],
) -> None:
    """Two workspaces independently register identical business IDs/URNs on PostgreSQL."""
    ws_a, ws_b = ws_pair
    factory = create_session_factory(pg_engine)
    repo_a = SqlAlchemyMetadataRepository(factory, workspace_id=ws_a)
    repo_b = SqlAlchemyMetadataRepository(factory, workspace_id=ws_b)

    # 1. Identical dataset_id ("customers") and URN in both workspaces
    ds_a = DatasetMetadata(
        dataset_id="customers",
        urn=URN.for_dataset("customers", namespace="crm"),
        name="Customers A",
        namespace="crm",
        layer="bronze",
        owner="team_a",
        tags=["crm", "bronze"],
    )
    ds_b = DatasetMetadata(
        dataset_id="customers",
        urn=URN.for_dataset("customers", namespace="crm"),
        name="Customers B",
        namespace="crm",
        layer="silver",
        owner="team_b",
        tags=["crm", "silver"],
    )
    repo_a.save_dataset(ds_a)
    repo_b.save_dataset(ds_b)

    with pytest.raises(DuplicateDatasetError):
        repo_a.save_dataset(ds_a)

    assert repo_a.get_dataset("customers") == ds_a
    assert repo_b.get_dataset("customers") == ds_b
    assert repo_a.get_dataset_by_urn(ds_a.urn) == ds_a
    assert repo_b.get_dataset_by_urn(ds_b.urn) == ds_b
    assert repo_a.list_datasets() == [ds_a]
    assert repo_b.list_datasets() == [ds_b]

    # 2. Identical schema v1 in both workspaces
    s_a1 = repo_a.register_next_schema(
        "customers",
        (
            ColumnMetadata(name="customer_id", data_type="int", primary_key=True, nullable=False),
            ColumnMetadata(name="email", data_type="string"),
        ),
        namespace="crm",
    )
    s_b1 = repo_b.register_next_schema(
        "customers",
        (ColumnMetadata(name="customer_id", data_type="string", primary_key=True, nullable=False),),
        namespace="crm",
    )
    assert s_a1.version == 1
    assert s_b1.version == 1
    assert s_a1.schema_id == s_b1.schema_id

    with pytest.raises(SchemaConflictError):
        repo_a.save_schema(
            SchemaMetadata(
                dataset_id="customers",
                namespace="crm",
                version=1,
                columns=(ColumnMetadata(name="x", data_type="int"),),
            )
        )

    assert repo_a.get_schema("customers", 1) == s_a1
    assert repo_b.get_schema("customers", 1) == s_b1
    assert [c.data_type for c in (repo_a.get_schema("customers") or s_a1).columns] == [
        "int",
        "string",
    ]
    assert [c.data_type for c in (repo_b.get_schema("customers") or s_b1).columns] == ["string"]

    # 3. Identical artifact_id in both workspaces
    art_a = ArtifactMetadata(
        artifact_id="art-customers-001",
        dataset_id="customers",
        namespace="crm",
        uri="s3://bucket-a/customers.parquet",
        format="parquet",
        row_count=10,
    )
    art_b = ArtifactMetadata(
        artifact_id="art-customers-001",
        dataset_id="customers",
        namespace="crm",
        uri="s3://bucket-b/customers.parquet",
        format="parquet",
        row_count=25,
    )
    repo_a.save_artifact(art_a)
    repo_b.save_artifact(art_b)

    with pytest.raises(DuplicateArtifactError):
        repo_a.save_artifact(art_a)

    assert repo_a.get_artifact("art-customers-001") == art_a
    assert repo_b.get_artifact("art-customers-001") == art_b
    assert repo_a.list_artifacts(dataset_id="customers") == [art_a]
    assert repo_b.list_artifacts(dataset_id="customers") == [art_b]

    # 4. Update & delete isolation
    updated_a = repo_a.save_dataset(ds_a.model_copy(update={"owner": "team_a_v2"}), overwrite=True)
    assert updated_a.owner == "team_a_v2"
    fetched_b = repo_b.get_dataset("customers")
    assert fetched_b is not None and fetched_b.owner == "team_b"

    assert repo_a.delete_dataset("customers") is True
    assert repo_a.get_dataset("customers") is None
    assert repo_b.get_dataset("customers") is not None


def test_postgresql_composite_foreign_key_rejects_cross_workspace_column(
    pg_engine: Engine,
    ws_pair: tuple[str, str],
) -> None:
    """PostgreSQL composite FK prevents a SchemaColumnRecord from referencing another workspace."""
    ws_a, ws_b = ws_pair
    factory = create_session_factory(pg_engine)
    repo_a = SqlAlchemyMetadataRepository(factory, workspace_id=ws_a)

    schema_a = repo_a.register_next_schema(
        "fk_target_ds",
        (ColumnMetadata(name="id", data_type="int"),),
    )

    with pytest.raises(IntegrityError):
        with factory(info={"workspace_id": ws_b}) as session, session.begin():
            session.add(
                SchemaColumnRecord(
                    workspace_id=ws_b,
                    schema_id=schema_a.schema_id,
                    position=0,
                    name="cross_tenant_col",
                    data_type="string",
                    nullable=True,
                    primary_key=False,
                    description=None,
                    default_value=None,
                    tags=[],
                    custom_properties={},
                )
            )


def test_postgresql_concurrent_schema_version_allocation_across_two_workspaces(
    pg_engine: Engine,
    ws_pair: tuple[str, str],
) -> None:
    """Two workspaces concurrently allocate schema versions starting at v1 for the same dataset."""
    ws_a, ws_b = ws_pair
    factory = create_session_factory(pg_engine)
    repo_a = SqlAlchemyMetadataRepository(factory, workspace_id=ws_a, max_allocation_attempts=25)
    repo_b = SqlAlchemyMetadataRepository(factory, workspace_id=ws_b, max_allocation_attempts=25)

    repo_a.save_dataset(DatasetMetadata(dataset_id="shared_events", name="Shared Events A"))
    repo_b.save_dataset(DatasetMetadata(dataset_id="shared_events", name="Shared Events B"))

    workers_per_ws = 4
    total_workers = workers_per_ws * 2
    barrier = threading.Barrier(total_workers)

    def allocate(target_repo: SqlAlchemyMetadataRepository, idx: int) -> int:
        barrier.wait()
        schema = target_repo.register_next_schema(
            "shared_events",
            (
                ColumnMetadata(name="id", data_type="int"),
                ColumnMetadata(name=f"col_{idx}", data_type="string"),
            ),
        )
        return schema.version

    with ThreadPoolExecutor(max_workers=total_workers) as pool:
        futures_a = [pool.submit(allocate, repo_a, i) for i in range(workers_per_ws)]
        futures_b = [pool.submit(allocate, repo_b, i) for i in range(workers_per_ws)]
        versions_a = sorted(f.result(timeout=30) for f in futures_a)
        versions_b = sorted(f.result(timeout=30) for f in futures_b)

    expected = list(range(1, workers_per_ws + 1))
    assert versions_a == expected
    assert versions_b == expected
    assert [s.version for s in repo_a.list_schemas("shared_events")] == expected
    assert [s.version for s in repo_b.list_schemas("shared_events")] == expected


def test_postgresql_ownership_validation_tests_a_through_g(
    pg_engine: Engine,
    ws_pair: tuple[str, str],
) -> None:
    """Verify Tests A-G (write-path before_flush ownership invariants) on PostgreSQL."""
    ws_a, ws_b = ws_pair
    factory = create_session_factory(pg_engine)

    # Test A: Valid insertion
    with factory(info={"workspace_id": ws_a}) as session, session.begin():
        assert isinstance(session, MetadataSession)
        session.add(_make_dataset_record("pg_ds_a", ws_a))

    # Test B: Ownership mismatch on insertion
    with pytest.raises(ValueError, match="Workspace ownership mismatch on insert"):
        with factory(info={"workspace_id": ws_a}) as session, session.begin():
            session.add(_make_dataset_record("pg_ds_mismatch", ws_b))

    # Test C: Ownership mutation on persistent entity
    with pytest.raises(ValueError, match="^Cannot mutate workspace_id of a persistent entity$"):
        with factory(info={"workspace_id": ws_a}) as session, session.begin():
            rec = session.scalar(
                select(DatasetRecord).where(
                    DatasetRecord.workspace_id == ws_a,
                    DatasetRecord.dataset_id == "pg_ds_a",
                )
            )
            assert rec is not None
            rec.workspace_id = ws_b
            session.flush()

    # Persist an entity in ws_b for Tests D & E
    with factory(info={"workspace_id": ws_b}) as session, session.begin():
        session.add(_make_dataset_record("pg_ds_b", ws_b))

    # Test D: Cross-workspace modification
    with pytest.raises(ValueError, match="Workspace ownership mismatch on update"):
        with factory(info={"workspace_id": ws_a}) as session, session.begin():
            rec_b = session.scalar(
                select(DatasetRecord).where(
                    DatasetRecord.workspace_id == ws_b,
                    DatasetRecord.dataset_id == "pg_ds_b",
                )
            )
            assert rec_b is not None
            rec_b.owner = "hijacked"
            session.flush()

    # Test E: Cross-workspace deletion
    with pytest.raises(ValueError, match="Workspace ownership mismatch on delete"):
        with factory(info={"workspace_id": ws_a}) as session, session.begin():
            rec_b = session.scalar(
                select(DatasetRecord).where(
                    DatasetRecord.workspace_id == ws_b,
                    DatasetRecord.dataset_id == "pg_ds_b",
                )
            )
            assert rec_b is not None
            session.delete(rec_b)
            session.flush()

    # Test F: Missing workspace identity fails closed
    with pytest.raises(ValueError, match="without a valid 'workspace_id' in session.info"):
        with factory() as session, session.begin():
            session.add(_make_dataset_record("pg_ds_unscoped", ws_a))

    # Test G: Global entities and global Session class remain unaffected
    assert not event.contains(Session, "before_flush", validate_workspace_ownership)
    _PgGlobalBase.metadata.create_all(pg_engine)
    cfg_key = f"key_{ws_a}"
    with factory() as session, session.begin():
        session.add(_PgGlobalConfigRecord(config_key=cfg_key, config_value="enabled"))
    with factory() as session:
        loaded = session.get(_PgGlobalConfigRecord, cfg_key)
        assert loaded is not None and loaded.config_value == "enabled"

