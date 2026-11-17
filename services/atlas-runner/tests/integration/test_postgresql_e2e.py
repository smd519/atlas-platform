"""End-to-End (E2E) PostgreSQL persistence test for AtlasRunner."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from atlas_core import ExecutionStatus
from atlas_metadata import ColumnMetadata, MetadataService
from atlas_metadata.repositories.sqlalchemy import (
    SqlAlchemyMetadataRepository,
    create_metadata_engine,
)
from atlas_runner import AtlasRunner
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

DEFAULT_POSTGRES_URL = "postgresql+psycopg://atlas:atlas@127.0.0.1:5432/atlas"

pytestmark = pytest.mark.postgresql


def _postgres_url() -> str:
    return (
        os.environ.get("ATLAS_TEST_DATABASE_URL", DEFAULT_POSTGRES_URL).strip()
        or DEFAULT_POSTGRES_URL
    )


def test_customer_001_runner_end_to_end_persists_to_postgresql(tmp_path: Path) -> None:
    """Execute AtlasRunner.run() for workspace customer_001 and verify PostgreSQL rows."""
    db_url = _postgres_url()
    probe_engine = create_metadata_engine(db_url, connect_args={"connect_timeout": 3})
    try:
        with probe_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError as exc:
        probe_engine.dispose()
        pytest.skip(f"PostgreSQL is not reachable at '{db_url}': {exc}")
    finally:
        probe_engine.dispose()

    # Build customer_001 workspace directory structure so workspace_id resolves to 'customer_001'
    ws_dir = tmp_path / "customer_001"
    (ws_dir / "drop").mkdir(parents=True, exist_ok=True)
    (ws_dir / "jobs").mkdir(parents=True, exist_ok=True)
    (ws_dir / "workflows").mkdir(parents=True, exist_ok=True)

    (ws_dir / "drop" / "sensors.csv").write_text(
        "sensor_id,temperature,city\ns1,21.5,Toronto\ns2,24.0,Montreal\ns3,18.5,Ottawa\n",
        encoding="utf-8",
    )

    (ws_dir / "workspace.yml").write_text(
        f"""
metadata:
  backend: sqlalchemy
  database_url: "{db_url}"
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
      dataset_id: customer_001_sensors
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

    runner = AtlasRunner()
    result = runner.run(ws_dir, "jobs/sensors_job.yml")
    assert result.status == ExecutionStatus.SUCCEEDED

    assert runner.last_dependencies is not None
    metadata_provider = runner.last_dependencies.metadata_provider
    assert isinstance(metadata_provider, MetadataService)
    assert isinstance(metadata_provider.repository, SqlAlchemyMetadataRepository)
    assert metadata_provider.repository.workspace_id == "customer_001"

    # Register schema version for the dataset through the runner's active MetadataService
    registered_schema = metadata_provider.register_schema(
        "customer_001_sensors",
        columns=[
            ColumnMetadata(name="sensor_id", data_type="string", primary_key=True, nullable=False),
            ColumnMetadata(name="temperature", data_type="float", nullable=False),
            ColumnMetadata(name="city", data_type="string", nullable=True),
        ],
    )
    metadata_provider.repository.dispose()

    # Query PostgreSQL directly and assert that datasets, schemas, and artifacts exist
    # with workspace_id = 'customer_001'
    verify_engine = create_metadata_engine(db_url)
    try:
        with verify_engine.connect() as conn:
            dataset_rows = conn.execute(
                text(
                    "SELECT workspace_id, dataset_id, layer "
                    "FROM atlas_datasets "
                    "WHERE workspace_id = :ws AND dataset_id = :ds"
                ),
                {"ws": "customer_001", "ds": "customer_001_sensors"},
            ).fetchall()
            assert len(dataset_rows) == 1
            assert all(row.workspace_id == "customer_001" for row in dataset_rows)
            assert dataset_rows[0].dataset_id == "customer_001_sensors"
            assert dataset_rows[0].layer == "silver"

            schema_rows = conn.execute(
                text(
                    "SELECT workspace_id, schema_id, dataset_id, version "
                    "FROM atlas_schemas "
                    "WHERE workspace_id = :ws AND dataset_id = :ds"
                ),
                {"ws": "customer_001", "ds": "customer_001_sensors"},
            ).fetchall()
            assert len(schema_rows) >= 1
            assert all(row.workspace_id == "customer_001" for row in schema_rows)
            assert any(row.schema_id == registered_schema.schema_id for row in schema_rows)

            column_rows = conn.execute(
                text(
                    "SELECT workspace_id, schema_id, name "
                    "FROM atlas_schema_columns "
                    "WHERE workspace_id = :ws AND schema_id = :sid "
                    "ORDER BY position"
                ),
                {"ws": "customer_001", "sid": registered_schema.schema_id},
            ).fetchall()
            assert len(column_rows) == 3
            assert all(row.workspace_id == "customer_001" for row in column_rows)
            assert [row.name for row in column_rows] == ["sensor_id", "temperature", "city"]

            artifact_rows = conn.execute(
                text(
                    "SELECT workspace_id, artifact_id, dataset_id, layer "
                    "FROM atlas_artifacts "
                    "WHERE workspace_id = :ws AND dataset_id = :ds"
                ),
                {"ws": "customer_001", "ds": "customer_001_sensors"},
            ).fetchall()
            assert len(artifact_rows) >= 2
            assert all(row.workspace_id == "customer_001" for row in artifact_rows)
            layers = {row.layer for row in artifact_rows}
            assert {"bronze", "silver"}.issubset(layers)
    finally:
        verify_engine.dispose()
