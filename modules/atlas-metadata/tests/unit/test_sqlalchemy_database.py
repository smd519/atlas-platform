"""Tests for SQLAlchemy engine configuration, initialization, and session lifecycle."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from atlas_metadata import (
    ColumnMetadata,
    DatasetMetadata,
    DuplicateDatasetError,
    InMemoryMetadataRepository,
    MetadataRepository,
    SchemaMetadata,
)
from atlas_metadata.repositories.sqlalchemy import (
    Base,
    SqlAlchemyMetadataRepository,
    create_metadata_engine,
    initialize_database,
)
from sqlalchemy import inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.pool import QueuePool

EXPECTED_TABLES = {
    "atlas_datasets",
    "atlas_schemas",
    "atlas_schema_columns",
    "atlas_artifacts",
}


def _file_url(path: Path) -> URL:
    return URL.create("sqlite", database=str(path))


def test_initialize_database_creates_tables_and_is_repeatable(tmp_path: Path) -> None:
    engine = create_metadata_engine(_file_url(tmp_path / "catalog.db"))
    try:
        assert set(inspect(engine).get_table_names()) == set()
        initialize_database(engine)
        assert set(inspect(engine).get_table_names()) == EXPECTED_TABLES
        initialize_database(engine)  # idempotent
        assert set(inspect(engine).get_table_names()) == EXPECTED_TABLES
    finally:
        engine.dispose()


def test_initialize_never_resets_existing_catalog_data(tmp_path: Path) -> None:
    url = _file_url(tmp_path / "catalog.db")
    first = SqlAlchemyMetadataRepository.from_url(url)
    first.save_dataset(DatasetMetadata(dataset_id="keep_me", name="Keep Me"))
    first.dispose()

    second = SqlAlchemyMetadataRepository.from_url(url, initialize=True)
    try:
        assert second.get_dataset("keep_me") is not None
    finally:
        second.dispose()


def test_initialize_false_does_not_create_tables(tmp_path: Path) -> None:
    repo = SqlAlchemyMetadataRepository.from_url(
        _file_url(tmp_path / "catalog.db"), initialize=False
    )
    try:
        assert repo.engine is not None
        assert inspect(repo.engine).get_table_names() == []
    finally:
        repo.dispose()


def test_sqlite_parent_directory_is_created_for_file_database(tmp_path: Path) -> None:
    db_path = tmp_path / "nested" / "metadata" / "catalog.db"
    assert not db_path.parent.exists()

    repo = SqlAlchemyMetadataRepository.from_url(_file_url(db_path))
    try:
        repo.save_dataset(DatasetMetadata(dataset_id="d", name="D"))
        assert db_path.exists()
    finally:
        repo.dispose()


def test_in_memory_sqlite_creates_no_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    repo = SqlAlchemyMetadataRepository.from_url("sqlite://")
    try:
        repo.save_dataset(DatasetMetadata(dataset_id="d", name="D"))
        assert repo.get_dataset("d") is not None
    finally:
        repo.dispose()
    assert list(tmp_path.iterdir()) == []


def test_sqlite_connection_configuration(tmp_path: Path) -> None:
    engine = create_metadata_engine(_file_url(tmp_path / "catalog.db"), sqlite_timeout=7.5)
    try:
        with engine.connect() as connection:
            assert connection.execute(text("PRAGMA journal_mode")).scalar() == "wal"
            assert connection.execute(text("PRAGMA foreign_keys")).scalar() == 1
            assert connection.execute(text("PRAGMA busy_timeout")).scalar() == 7500
    finally:
        engine.dispose()


def test_non_sqlite_urls_do_not_apply_sqlite_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PostgreSQL URL is handed to SQLAlchemy untouched: no SQLite args, pool, or pragmas."""
    from atlas_metadata.repositories.sqlalchemy import database

    captured: dict[str, Any] = {}

    def fake_create_engine(url: URL, **kwargs: Any) -> str:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return "engine"

    monkeypatch.setattr(database, "create_engine", fake_create_engine)

    create_metadata_engine("postgresql+psycopg://user:pw@localhost:5432/atlas", pool_size=3)

    assert captured["url"].get_backend_name() == "postgresql"
    assert captured["kwargs"] == {"echo": False, "pool_size": 3}


def test_sessions_return_connections_to_pool_after_success_and_failure(tmp_path: Path) -> None:
    repo = SqlAlchemyMetadataRepository.from_url(_file_url(tmp_path / "catalog.db"))
    try:
        engine = repo.engine
        assert engine is not None
        pool = engine.pool
        assert isinstance(pool, QueuePool)

        repo.save_dataset(DatasetMetadata(dataset_id="d", name="D"))
        repo.get_dataset("d")
        repo.list_datasets()
        assert pool.checkedout() == 0

        with pytest.raises(DuplicateDatasetError):
            repo.save_dataset(DatasetMetadata(dataset_id="d", name="D"))
        assert pool.checkedout() == 0
    finally:
        repo.dispose()


def test_failed_transaction_rolls_back_all_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure after the schema rows are flushed leaves no partial schema or column rows."""
    repo = SqlAlchemyMetadataRepository.from_url(_file_url(tmp_path / "catalog.db"))
    try:
        repo.save_dataset(DatasetMetadata(dataset_id="d", name="D"))

        def boom(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("simulated failure after insert")

        monkeypatch.setattr(type(repo), "_sync_dataset_latest_version", boom)
        schema = SchemaMetadata(
            dataset_id="d",
            version=1,
            columns=(ColumnMetadata(name="id", data_type="int"),),
        )
        with pytest.raises(RuntimeError, match="simulated failure"):
            repo.save_schema(schema)

        monkeypatch.undo()
        assert repo.get_schema("d") is None
        assert repo.list_schemas("d") == []
        assert repo.engine is not None
        with repo.engine.connect() as connection:
            assert connection.execute(text("SELECT COUNT(*) FROM atlas_schemas")).scalar() == 0
            assert (
                connection.execute(text("SELECT COUNT(*) FROM atlas_schema_columns")).scalar() == 0
            )
        # The version is still free to register.
        assert repo.save_schema(schema) == schema
    finally:
        repo.dispose()


def test_repositories_satisfy_the_metadata_repository_contract(tmp_path: Path) -> None:
    sql_repo = SqlAlchemyMetadataRepository.from_url(_file_url(tmp_path / "catalog.db"))
    try:
        assert isinstance(sql_repo, MetadataRepository)
        assert isinstance(InMemoryMetadataRepository(), MetadataRepository)
    finally:
        sql_repo.dispose()


def test_orm_models_are_separate_from_domain_models() -> None:
    """Pydantic domain models do not derive from the SQLAlchemy base."""
    for model in (DatasetMetadata, SchemaMetadata, ColumnMetadata):
        assert Base not in model.__mro__
    assert all(table.startswith("atlas_") for table in Base.metadata.tables)


def test_importing_atlas_metadata_does_not_import_sqlalchemy() -> None:
    """The domain package and in-memory repository stay free of SQLAlchemy."""
    code = (
        "import sys, atlas_metadata; "
        "from atlas_metadata import InMemoryMetadataRepository, MetadataService; "
        "MetadataService(); "
        "assert not any(m == 'sqlalchemy' or m.startswith('sqlalchemy.') for m in sys.modules)"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
