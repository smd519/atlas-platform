"""Unit tests for atlas-runner dependency wiring and workspace-aware metadata configuration."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from atlas_core import Dataset, ExecutionStatus, MetadataProvider
from atlas_ingestion import IngestionService
from atlas_metadata import InMemoryMetadataRepository, MetadataService
from atlas_metadata.repositories.sqlalchemy import SqlAlchemyMetadataRepository
from atlas_processing import ProcessingService
from atlas_quality import QualityService
from atlas_runner import (
    AtlasRunner,
    JobDefinition,
    MetadataConfig,
    RunnerConfigurationError,
    WorkspaceConfig,
    WorkspaceIsolationError,
    build_default_dependencies,
    create_orchestration_service,
    load_job_definition,
    load_workspace_config,
    resolve_metadata_database_path,
    resolve_metadata_database_url,
)
from pydantic import ValidationError


def test_default_dependencies_instantiates_exactly_one_shared_metadata_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exactly one MetadataService must be instantiated and shared across all consumers."""
    instantiation_count = 0
    real_init = MetadataService.__init__

    def tracked_init(self: MetadataService, *args: Any, **kwargs: Any) -> None:
        nonlocal instantiation_count
        instantiation_count += 1
        real_init(self, *args, **kwargs)

    monkeypatch.setattr(MetadataService, "__init__", tracked_init)

    deps = build_default_dependencies()

    # 1. Verify instantiation count
    assert instantiation_count == 1, (
        f"Expected exactly 1 MetadataService instantiation, got {instantiation_count}"
    )

    # 2. Verify OrchestrationDependencies has the shared instance with in-memory repository
    shared_metadata = deps.metadata_provider
    assert shared_metadata is not None
    assert isinstance(shared_metadata, MetadataService)
    assert isinstance(shared_metadata.repository, InMemoryMetadataRepository)

    # 3. Verify Ingestion, Processing, and Quality receive the identical instance
    assert isinstance(deps.ingestion_service, IngestionService)
    assert isinstance(deps.processing_service, ProcessingService)
    assert isinstance(deps.quality_service, QualityService)

    assert deps.ingestion_service._pipeline._metadata is shared_metadata
    assert deps.processing_service._pipeline._metadata is shared_metadata
    assert deps.quality_service._pipeline._metadata is shared_metadata


def test_explicit_metadata_provider_is_propagated_to_all_services() -> None:
    """Explicit metadata_provider is propagated without creating a new MetadataService."""
    mock_provider = MagicMock(spec=MetadataProvider)

    deps = build_default_dependencies(metadata_provider=mock_provider)

    assert deps.metadata_provider is mock_provider
    assert isinstance(deps.ingestion_service, IngestionService)
    assert isinstance(deps.processing_service, ProcessingService)
    assert isinstance(deps.quality_service, QualityService)

    assert deps.ingestion_service._pipeline._metadata is mock_provider
    assert deps.processing_service._pipeline._metadata is mock_provider
    assert deps.quality_service._pipeline._metadata is mock_provider


def test_explicit_metadata_provider_takes_precedence_over_sqlalchemy_configuration(
    tmp_path: Path,
) -> None:
    """An explicitly injected MetadataProvider takes precedence over SQLAlchemy configuration."""
    mock_provider = MagicMock(spec=MetadataProvider)
    ws_dir = tmp_path / "customer_ws"
    ws_dir.mkdir(parents=True, exist_ok=True)

    meta_cfg = MetadataConfig(
        backend="sqlalchemy",
        database_url="sqlite:///{workspace}/metadata/catalog.db",
    )

    deps = build_default_dependencies(
        workspace=ws_dir,
        metadata_config=meta_cfg,
        metadata_provider=mock_provider,
    )

    assert deps.metadata_provider is mock_provider
    assert isinstance(deps.ingestion_service, IngestionService)
    assert isinstance(deps.processing_service, ProcessingService)
    assert isinstance(deps.quality_service, QualityService)
    assert deps.ingestion_service._pipeline._metadata is mock_provider
    assert deps.processing_service._pipeline._metadata is mock_provider
    assert deps.quality_service._pipeline._metadata is mock_provider
    # Database file should not be created when explicit provider overrides config
    assert not (ws_dir / "metadata" / "catalog.db").exists()


def test_falsy_injected_metadata_provider_is_not_replaced_by_default() -> None:
    """A deliberately injected metadata_provider that evaluates as falsy is preserved."""

    class FalsyMetadataProvider(MetadataProvider):
        def register_dataset(self, dataset: Any, *, overwrite: bool = False) -> Any:
            return None

        def get_dataset(self, dataset_id: str) -> Any:
            return None

        def __bool__(self) -> bool:
            return False

    falsy_provider = FalsyMetadataProvider()
    assert bool(falsy_provider) is False

    deps = build_default_dependencies(metadata_provider=falsy_provider)

    assert deps.metadata_provider is falsy_provider
    assert isinstance(deps.ingestion_service, IngestionService)
    assert isinstance(deps.processing_service, ProcessingService)
    assert isinstance(deps.quality_service, QualityService)

    assert deps.ingestion_service._pipeline._metadata is falsy_provider
    assert deps.processing_service._pipeline._metadata is falsy_provider
    assert deps.quality_service._pipeline._metadata is falsy_provider


def test_create_orchestration_service_wires_shared_metadata() -> None:
    """OrchestrationService created with default dependencies receives the shared provider."""
    service = create_orchestration_service()

    deps = service.dependencies
    shared_metadata = deps.metadata_provider
    assert shared_metadata is not None
    assert isinstance(shared_metadata, MetadataService)

    assert isinstance(deps.ingestion_service, IngestionService)
    assert isinstance(deps.processing_service, ProcessingService)
    assert isinstance(deps.quality_service, QualityService)

    assert deps.ingestion_service._pipeline._metadata is shared_metadata
    assert deps.processing_service._pipeline._metadata is shared_metadata
    assert deps.quality_service._pipeline._metadata is shared_metadata


def test_sqlalchemy_configuration_creates_persistent_sqlite_repository_and_shares_instance(
    tmp_path: Path,
) -> None:
    """SQLAlchemy backend creates one persistent SQLite repository shared across all consumers."""
    ws_dir = tmp_path / "customer_alpha"
    meta_cfg = MetadataConfig(
        backend="sqlalchemy",
        database_url="sqlite:///{workspace}/metadata/catalog.db",
    )

    deps = build_default_dependencies(workspace=ws_dir, metadata_config=meta_cfg)
    shared_metadata = deps.metadata_provider

    assert isinstance(shared_metadata, MetadataService)
    assert isinstance(shared_metadata.repository, SqlAlchemyMetadataRepository)

    assert isinstance(deps.ingestion_service, IngestionService)
    assert isinstance(deps.processing_service, ProcessingService)
    assert isinstance(deps.quality_service, QualityService)
    assert deps.ingestion_service._pipeline._metadata is shared_metadata
    assert deps.processing_service._pipeline._metadata is shared_metadata
    assert deps.quality_service._pipeline._metadata is shared_metadata

    expected_db = (ws_dir / "metadata" / "catalog.db").resolve()
    assert expected_db.parent.is_dir()

    # Register a dataset and verify persistence across a fresh MetadataService instance
    shared_metadata.register_dataset(
        Dataset(
            id="orders_bronze",
            name="Orders Bronze",
            layer="bronze",
            path=str(ws_dir / "data" / "bronze" / "orders.parquet"),
        )
    )
    assert expected_db.exists()
    shared_metadata.repository.dispose()

    deps_reopened = build_default_dependencies(workspace=ws_dir, metadata_config=meta_cfg)
    reopened_metadata = deps_reopened.metadata_provider
    assert isinstance(reopened_metadata, MetadataService)
    assert isinstance(reopened_metadata.repository, SqlAlchemyMetadataRepository)

    persisted = reopened_metadata.get_dataset("orders_bronze")
    assert persisted is not None
    assert persisted.dataset_id == "orders_bronze"
    assert persisted.name == "Orders Bronze"
    reopened_metadata.repository.dispose()


def test_workspace_placeholder_and_relative_sqlite_path_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """{workspace} and relative SQLite paths resolve against workspace root, never cwd."""
    managed_root = tmp_path / "managed_workspaces"
    ws_root = managed_root / "tenant_42"
    ws_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ATLAS_WORKSPACE", str(managed_root))

    initial_cwd = Path.cwd()

    # 1. {workspace} placeholder with logical workspace ID resolved via ATLAS_WORKSPACE
    resolved_path_1 = resolve_metadata_database_path(
        "sqlite:///{workspace}/metadata/catalog.db",
        workspace="tenant_42",
    )
    expected_db_path = (ws_root / "metadata" / "catalog.db").resolve()
    assert resolved_path_1 == expected_db_path
    assert expected_db_path.parent.is_dir()

    resolved_url_1 = resolve_metadata_database_url(
        "sqlite:///{workspace}/metadata/catalog.db",
        workspace="tenant_42",
    )
    assert resolved_url_1 == f"sqlite:///{expected_db_path.as_posix()}"

    # 2. Relative SQLite path (sqlite:///metadata/catalog.db) resolves against workspace root
    rel_ws = tmp_path / "relative_ws"
    resolved_path_2 = resolve_metadata_database_path(
        "sqlite:///metadata/nested/catalog.db",
        workspace=rel_ws,
    )
    expected_rel_db = (rel_ws / "metadata" / "nested" / "catalog.db").resolve()
    assert resolved_path_2 == expected_rel_db
    assert resolved_path_2 != (initial_cwd / "metadata" / "nested" / "catalog.db").resolve()
    assert expected_rel_db.parent.is_dir()

    # Process working directory must remain unchanged
    assert Path.cwd() == initial_cwd
    assert os.getcwd() == str(initial_cwd)


def test_separate_workspaces_receive_separate_database_files(tmp_path: Path) -> None:
    """Two workspaces using identical metadata configuration receive isolated SQLite files."""
    ws_a = tmp_path / "workspace_a"
    ws_b = tmp_path / "workspace_b"
    meta_cfg = MetadataConfig(
        backend="sqlalchemy",
        database_url="sqlite:///{workspace}/metadata/catalog.db",
    )

    deps_a = build_default_dependencies(workspace=ws_a, metadata_config=meta_cfg)
    deps_b = build_default_dependencies(workspace=ws_b, metadata_config=meta_cfg)

    meta_a = deps_a.metadata_provider
    meta_b = deps_b.metadata_provider
    assert isinstance(meta_a, MetadataService)
    assert isinstance(meta_b, MetadataService)
    assert meta_a is not meta_b

    meta_a.register_dataset(
        Dataset(
            id="only_in_a",
            name="Dataset A",
            layer="bronze",
            path=str(ws_a / "data" / "bronze" / "a.parquet"),
        )
    )
    meta_b.register_dataset(
        Dataset(
            id="only_in_b",
            name="Dataset B",
            layer="bronze",
            path=str(ws_b / "data" / "bronze" / "b.parquet"),
        )
    )

    db_a = (ws_a / "metadata" / "catalog.db").resolve()
    db_b = (ws_b / "metadata" / "catalog.db").resolve()
    assert db_a.exists()
    assert db_b.exists()
    assert db_a != db_b

    assert meta_a.get_dataset("only_in_a") is not None
    assert meta_a.get_dataset("only_in_b") is None
    assert meta_b.get_dataset("only_in_b") is not None
    assert meta_b.get_dataset("only_in_a") is None

    assert isinstance(meta_a.repository, SqlAlchemyMetadataRepository)
    assert isinstance(meta_b.repository, SqlAlchemyMetadataRepository)
    meta_a.repository.dispose()
    meta_b.repository.dispose()


def test_strict_pydantic_validation_for_metadata_and_workspace_configuration(
    tmp_path: Path,
) -> None:
    """Strict Pydantic models accept valid metadata blocks and reject invalid configs."""
    # 1. Valid WorkspaceConfig & JobDefinition with metadata block
    ws_cfg = WorkspaceConfig.model_validate(
        {
            "metadata": {
                "backend": "sqlalchemy",
                "database_url": "sqlite:///{workspace}/metadata/catalog.db",
            }
        }
    )
    assert ws_cfg.metadata.backend == "sqlalchemy"
    assert ws_cfg.metadata.database_url == "sqlite:///{workspace}/metadata/catalog.db"

    job_def = JobDefinition.model_validate(
        {
            "job_id": "morning_batch",
            "workflows": ["corporations_pipeline"],
            "metadata": {
                "backend": "sqlalchemy",
                "database_url": "sqlite:///{workspace}/metadata/catalog.db",
            },
        }
    )
    assert job_def.metadata.backend == "sqlalchemy"
    assert job_def.metadata.database_url == "sqlite:///{workspace}/metadata/catalog.db"

    # 2. Optional metadata block defaults to memory
    default_ws = WorkspaceConfig.model_validate({})
    assert default_ws.metadata.backend == "memory"
    assert default_ws.metadata.database_url is None

    default_job = JobDefinition.model_validate(
        {"job_id": "simple_job", "workflow_ref": "workflows/simple.yml"}
    )
    assert default_job.metadata.backend == "memory"
    assert default_job.metadata.database_url is None

    # 3. Unsupported backend is rejected
    with pytest.raises(ValidationError, match="Unsupported metadata backend"):
        MetadataConfig.model_validate({"backend": "postgresql"})

    with pytest.raises(ValidationError, match="Unsupported metadata backend"):
        WorkspaceConfig.model_validate({"metadata": {"backend": "duckdb"}})

    # 4. SQLAlchemy without database_url is rejected (never falls back to memory)
    with pytest.raises(ValidationError, match="database_url is required"):
        MetadataConfig.model_validate({"backend": "sqlalchemy"})

    with pytest.raises(ValidationError, match="database_url"):
        MetadataConfig.model_validate({"backend": "sqlalchemy", "database_url": "   "})

    # 5. Extra fields in MetadataConfig or WorkspaceConfig are rejected (extra="forbid")
    with pytest.raises(ValidationError):
        MetadataConfig.model_validate(
            {
                "backend": "memory",
                "unexpected_option": True,
            }
        )

    with pytest.raises(ValidationError):
        WorkspaceConfig.model_validate(
            {
                "metadata": {"backend": "memory"},
                "unknown_top_level": "rejected",
            }
        )

    # 6. Path traversal in database_url is rejected
    with pytest.raises(WorkspaceIsolationError):
        resolve_metadata_database_url(
            "sqlite:///{workspace}/../../outside/catalog.db",
            workspace=tmp_path,
        )

    with pytest.raises(WorkspaceIsolationError):
        resolve_metadata_database_url(
            "sqlite:///../../outside/catalog.db",
            workspace=tmp_path,
        )

    # 7. Misconfigured SQLAlchemy URL raises RunnerConfigurationError without fallback
    with pytest.raises(RunnerConfigurationError):
        build_default_dependencies(
            workspace=tmp_path,
            metadata_config={"backend": "sqlalchemy", "database_url": "postgresql://localhost/db"},
        )


def test_default_in_memory_execution_does_not_import_sqlalchemy() -> None:
    """Importing atlas_runner and building default dependencies must not load SQLAlchemy."""
    code = (
        "import sys; "
        "from atlas_runner.dependencies import build_default_dependencies; "
        "deps = build_default_dependencies(); "
        "assert 'sqlalchemy' not in sys.modules, 'sqlalchemy was imported!'; "
        "assert 'atlas_metadata.repositories.sqlalchemy' not in sys.modules, "
        "'sqlalchemy repository module was imported!'"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"Subprocess failed: {proc.stderr}"


def test_runner_end_to_end_persists_metadata_via_workspace_and_job_config(
    tmp_path: Path,
) -> None:
    """AtlasRunner executes a pipeline and persists catalog metadata to the workspace SQLite DB."""
    ws_dir = tmp_path / "customer_persistent"
    (ws_dir / "drop").mkdir(parents=True, exist_ok=True)
    (ws_dir / "jobs").mkdir(parents=True, exist_ok=True)
    (ws_dir / "workflows").mkdir(parents=True, exist_ok=True)

    (ws_dir / "drop" / "sensors.csv").write_text(
        "sensor_id,temperature\ns1,21.5\ns2,24.0\n",
        encoding="utf-8",
    )
    (ws_dir / "workspace.yml").write_text(
        """
metadata:
  backend: sqlalchemy
  database_url: "sqlite:///{workspace}/metadata/catalog.db"
""",
        encoding="utf-8",
    )
    (ws_dir / "jobs" / "sensors_job.yml").write_text(
        """
job_id: sensors_job
workflows:
  - sensors_pipeline
""",
        encoding="utf-8",
    )
    (ws_dir / "workflows" / "sensors_pipeline.yml").write_text(
        """
workflow_id: sensors_pipeline
name: Sensors Pipeline
version: 1
steps:
  - step_id: ingest
    module_target: atlas-ingestion
    action: ingest
    parameters:
      dataset_id: sensors_dataset
      source_format: csv
      source:
        source_id: sensors_csv
        source_name: Sensors CSV
        source_type: local_file
        config:
          path: drop/sensors.csv
      destination:
        layer: bronze
        path: data/bronze/sensors.parquet
        format: parquet
  - step_id: process
    module_target: atlas-processing
    action: transform
    depends_on:
      - ingest
    parameters:
      dataset_id: "${steps.ingest.dataset_id}"
      source: "${steps.ingest.destination}"
      destination:
        layer: silver
        path: data/silver/sensors.parquet
        format: parquet
      transformations:
        - transformation_id: filter
          name: require_temperature
          params:
            column: temperature
            operator: not_null
  - step_id: quality
    module_target: atlas-quality
    action: assess
    depends_on:
      - process
    parameters:
      dataset_id: "${steps.process.dataset_id}"
      storage_location: "${steps.process.destination}"
      rules:
        - rule_id: completeness.not_null
          name: sensor_id_not_null
          params:
            column: sensor_id
""",
        encoding="utf-8",
    )

    loaded_ws_cfg, cfg_path = load_workspace_config(ws_dir)
    assert cfg_path == (ws_dir / "workspace.yml").resolve()
    assert loaded_ws_cfg.metadata.backend == "sqlalchemy"

    loaded_job, _ = load_job_definition(ws_dir, "jobs/sensors_job.yml")
    assert loaded_job.job_id == "sensors_job"

    runner = AtlasRunner()
    result = runner.run(ws_dir, "jobs/sensors_job.yml")
    assert result.status == ExecutionStatus.SUCCEEDED

    catalog_db = ws_dir / "metadata" / "catalog.db"
    assert catalog_db.exists()

    assert runner.last_dependencies is not None
    run_meta = runner.last_dependencies.metadata_provider
    assert isinstance(run_meta, MetadataService)
    assert isinstance(run_meta.repository, SqlAlchemyMetadataRepository)
    run_meta.repository.dispose()

    # Open a new MetadataService against the workspace database to verify persisted records
    verify_deps = build_default_dependencies(workspace=ws_dir)
    verify_meta = verify_deps.metadata_provider
    assert isinstance(verify_meta, MetadataService)
    assert isinstance(verify_meta.repository, SqlAlchemyMetadataRepository)

    dataset = verify_meta.get_dataset("sensors_dataset")
    assert dataset is not None
    assert dataset.dataset_id == "sensors_dataset"
    artifacts = verify_meta.list_artifacts(dataset_id="sensors_dataset")
    assert len(artifacts) >= 2
    verify_meta.repository.dispose()

    # Verify job-level metadata configuration also works without workspace.yml
    ws_job_cfg_dir = tmp_path / "customer_job_cfg"
    (ws_job_cfg_dir / "drop").mkdir(parents=True, exist_ok=True)
    (ws_job_cfg_dir / "jobs").mkdir(parents=True, exist_ok=True)
    (ws_job_cfg_dir / "workflows").mkdir(parents=True, exist_ok=True)
    (ws_job_cfg_dir / "drop" / "sensors.csv").write_text(
        "sensor_id,temperature\ns1,21.5\n",
        encoding="utf-8",
    )
    (ws_job_cfg_dir / "jobs" / "sensors_job.yml").write_text(
        """
job_id: sensors_job
workflows:
  - sensors_pipeline
metadata:
  backend: sqlalchemy
  database_url: "sqlite:///{workspace}/metadata/catalog.db"
""",
        encoding="utf-8",
    )
    (ws_job_cfg_dir / "workflows" / "sensors_pipeline.yml").write_text(
        (ws_dir / "workflows" / "sensors_pipeline.yml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    res_job_cfg = runner.run(ws_job_cfg_dir, "jobs/sensors_job.yml")
    assert res_job_cfg.status == ExecutionStatus.SUCCEEDED
    assert (ws_job_cfg_dir / "metadata" / "catalog.db").exists()
    assert runner.last_dependencies is not None
    job_run_meta = runner.last_dependencies.metadata_provider
    assert isinstance(job_run_meta, MetadataService)
    assert isinstance(job_run_meta.repository, SqlAlchemyMetadataRepository)
    job_run_meta.repository.dispose()
