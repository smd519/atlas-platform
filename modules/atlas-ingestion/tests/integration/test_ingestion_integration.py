"""Integration tests for atlas-ingestion with atlas-connectors and atlas-storage."""

from __future__ import annotations

from pathlib import Path

import pandas as pd  # type: ignore[import-untyped]
import pytest
from atlas_connectors.files.local import LocalFileConnector
from atlas_connectors.models.source_config import SourceConfig
from atlas_connectors.registry import ConnectorRegistry
from atlas_core.constants import BRONZE
from atlas_core.enums import ExecutionStatus
from atlas_core.types import DatasetId
from atlas_ingestion import (
    ExecutionConfig,
    IngestionRequest,
    IngestionService,
    IngestionState,
)
from atlas_storage.backends.filesystem import FileSystemBackend
from atlas_storage.formats.csv import read_csv, write_csv
from atlas_storage.formats.json import read_json, write_json
from atlas_storage.formats.parquet import read_parquet, write_parquet
from atlas_storage.manager import StorageManager
from atlas_storage.models.storage_location import StorageLocation
from atlas_storage.registry import StorageRegistry


def _sample_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"station_id": "STN-01", "temperature_c": 21.5, "city": "Toronto"},
            {"station_id": "STN-02", "temperature_c": 18.0, "city": "Montreal"},
            {"station_id": "STN-03", "temperature_c": 15.2, "city": "Vancouver"},
        ]
    )


@pytest.mark.parametrize(
    ("source_format", "dest_format"),
    [
        ("csv", "parquet"),
        ("json", "csv"),
        ("parquet", "json"),
        ("csv", "csv"),
    ],
)
def test_end_to_end_local_file_ingestion_across_formats(
    tmp_path: Path,
    source_format: str,
    dest_format: str,
) -> None:
    df = _sample_dataframe()
    source_file = tmp_path / "source" / f"weather.{source_format}"
    source_file.parent.mkdir(parents=True, exist_ok=True)

    if source_format == "csv":
        write_csv(df, str(source_file))
    elif source_format == "json":
        write_json(df, str(source_file))
    elif source_format == "parquet":
        write_parquet(df, str(source_file))

    dest_file = tmp_path / "bronze" / f"weather_raw.{dest_format}"

    connector_registry = ConnectorRegistry()
    connector_registry.register("local_file", LocalFileConnector)

    storage_registry = StorageRegistry()
    storage_registry.register("filesystem", FileSystemBackend)

    service = IngestionService(
        connector_registry=connector_registry,
        storage_registry=storage_registry,
    )

    request = IngestionRequest(
        request_id=f"req-{source_format}-to-{dest_format}",
        dataset_id=DatasetId("weather_observations"),
        source=SourceConfig(
            source_id="env-canada-local",
            source_name="Environment Canada Sample",
            source_type="local_file",
            config={"path": str(source_file)},
        ),
        destination=StorageLocation(
            layer=BRONZE,
            path=str(dest_file),
            format=dest_format,
        ),
        source_format=source_format,
        execution_config=ExecutionConfig(
            validate_connection=True,
            partition_by_date=True,
        ),
    )

    result = service.ingest(request)

    assert result.success is True
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.lifecycle_state == IngestionState.COMPLETED
    assert result.row_count == 3
    assert len(result.artifacts) == 1

    persisted_path = Path(result.artifacts[0].path)
    assert persisted_path.exists()
    assert "year=" in str(persisted_path)

    if dest_format == "csv":
        loaded_df = read_csv(str(persisted_path))
    elif dest_format == "json":
        loaded_df = read_json(str(persisted_path))
    else:
        loaded_df = read_parquet(str(persisted_path))

    assert len(loaded_df) == 3
    assert list(loaded_df["station_id"]) == ["STN-01", "STN-02", "STN-03"]


def test_integration_missing_source_file_produces_failed_result(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing" / "does_not_exist.csv"
    dest_file = tmp_path / "bronze" / "should_not_exist.parquet"

    service = IngestionService(
        storage_manager=StorageManager(backend=FileSystemBackend()),
    )

    request = IngestionRequest(
        request_id="req-missing-file",
        dataset_id=DatasetId("missing_dataset"),
        source=SourceConfig(
            source_id="missing-src",
            source_name="Missing Local File",
            source_type="local_file",
            config={"path": str(missing_file)},
        ),
        destination=StorageLocation(
            layer=BRONZE,
            path=str(dest_file),
            format="parquet",
        ),
        source_format="csv",
    )

    result = service.ingest(request, execution_id="exec-missing-1")

    assert result.success is False
    assert result.status == ExecutionStatus.FAILED
    assert result.lifecycle_state == IngestionState.FAILED
    assert result.execution_id == "exec-missing-1"
    assert result.artifacts == []
    assert not dest_file.exists()

    tracked = service.get_execution("exec-missing-1")
    assert tracked.status == ExecutionStatus.FAILED
    assert tracked.lifecycle_state == IngestionState.FAILED
