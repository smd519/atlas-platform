"""Unit tests for ingestion request validation and result models."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from atlas_connectors import SourceConfig
from atlas_core import (
    BRONZE,
    Artifact,
    DatasetId,
    ExecutionStatus,
    Source,
    SourceId,
)
from atlas_ingestion.exceptions import InvalidIngestionRequestError
from atlas_ingestion.lifecycle import IngestionState
from atlas_ingestion.models.ingestion_request import ExecutionConfig, IngestionRequest
from atlas_ingestion.models.ingestion_result import IngestionResult
from atlas_storage import StorageLocation


def _valid_source() -> SourceConfig:
    return SourceConfig(
        source_id="src-001",
        source_name="weather_csv",
        source_type="local_file",
        config={"path": "data/sample.csv"},
    )


def _valid_destination() -> StorageLocation:
    return StorageLocation(
        layer=BRONZE,
        path="data/bronze/weather.parquet",
        format="parquet",
    )


def test_ingestion_request_valid_creation() -> None:
    request = IngestionRequest(
        request_id="req-100",
        dataset_id=DatasetId("ds-weather"),
        source=_valid_source(),
        destination=_valid_destination(),
        source_format="CSV",
        format_options={"delimiter": ","},
        execution_config=ExecutionConfig(validate_connection=True, partition_by_date=True),
        metadata={"owner": "data-eng"},
    )

    assert request.request_id == "req-100"
    assert request.dataset_id == DatasetId("ds-weather")
    assert request.source_format == "csv"
    assert request.destination.layer == "bronze"
    assert request.destination.format == "parquet"
    assert request.execution_config.partition_by_date is True
    assert request.metadata["owner"] == "data-eng"

    core_source = request.to_core_source()
    assert core_source == Source(
        id=SourceId("src-001"),
        name="weather_csv",
        source_type="local_file",
    )


@pytest.mark.parametrize(
    ("field_overrides", "expected_match"),
    [
        ({"request_id": "   "}, "request_id cannot be empty"),
        ({"dataset_id": DatasetId("")}, "dataset_id cannot be empty"),
        ({"source_format": ""}, "source_format cannot be empty"),
        ({"source_format": "xml"}, "Unsupported source_format 'xml'"),
        (
            {
                "source": SourceConfig(
                    source_id="",
                    source_name="name",
                    source_type="local_file",
                )
            },
            "source.source_id cannot be empty",
        ),
        (
            {
                "source": SourceConfig(
                    source_id="src-1",
                    source_name="  ",
                    source_type="local_file",
                )
            },
            "source.source_name cannot be empty",
        ),
        (
            {
                "source": SourceConfig(
                    source_id="src-1",
                    source_name="name",
                    source_type="",
                )
            },
            "source.source_type cannot be empty",
        ),
        (
            {
                "destination": StorageLocation(
                    layer="gold",
                    path="data/gold/out.parquet",
                    format="parquet",
                )
            },
            "Invalid destination layer 'gold'",
        ),
        (
            {
                "destination": StorageLocation.model_construct(
                    layer="bronze",
                    path="   ",
                    format="parquet",
                )
            },
            "cannot be empty",
        ),
        (
            {
                "destination": StorageLocation(
                    layer="bronze",
                    path="data/bronze/out.avro",
                    format="avro",
                )
            },
            "Unsupported destination format 'avro'",
        ),
    ],
)
def test_ingestion_request_rejects_invalid_configurations(
    field_overrides: dict[str, object],
    expected_match: str,
) -> None:
    base_kwargs: dict[str, object] = {
        "request_id": "req-1",
        "dataset_id": DatasetId("ds-1"),
        "source": _valid_source(),
        "destination": _valid_destination(),
        "source_format": "csv",
    }
    base_kwargs.update(field_overrides)

    with pytest.raises(InvalidIngestionRequestError, match=expected_match):
        IngestionRequest(**base_kwargs)  # type: ignore[arg-type]


def test_ingestion_request_rejects_missing_required_fields() -> None:
    with pytest.raises(InvalidIngestionRequestError):
        IngestionRequest(request_id="req-only")  # type: ignore[call-arg]


def test_ingestion_result_successful_construction_and_duration() -> None:
    start = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
    end = start + timedelta(seconds=12.5)
    artifact = Artifact(
        id="art-1",
        dataset_id="ds-weather",
        path="data/bronze/weather.parquet",
        format="parquet",
    )

    result = IngestionResult(
        request_id="req-100",
        execution_id="exec-200",
        status=ExecutionStatus.SUCCEEDED,
        lifecycle_state=IngestionState.COMPLETED,
        source=Source(id=SourceId("src-001"), name="weather_csv", source_type="local_file"),
        dataset_id=DatasetId("ds-weather"),
        destination=_valid_destination(),
        artifacts=[artifact],
        row_count=42,
        started_at=start,
        completed_at=end,
    )

    assert result.success is True
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.duration_seconds == pytest.approx(12.5)
    assert len(result.artifacts) == 1
    assert result.row_count == 42


def test_ingestion_result_failed_clears_partial_artifacts() -> None:
    start = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
    end = start + timedelta(seconds=3.0)
    partial_artifact = Artifact(
        id="art-partial",
        dataset_id="ds-weather",
        path="data/bronze/partial.parquet",
        format="parquet",
    )

    result = IngestionResult(
        request_id="req-100",
        execution_id="exec-201",
        status=ExecutionStatus.FAILED,
        lifecycle_state=IngestionState.FAILED,
        source=Source(id=SourceId("src-001"), name="weather_csv", source_type="local_file"),
        dataset_id=DatasetId("ds-weather"),
        destination=_valid_destination(),
        artifacts=[partial_artifact],
        started_at=start,
        completed_at=end,
        error_type="LoadingError",
        error_message="Disk write failed",
    )

    assert result.success is False
    assert result.artifacts == []
    assert result.message == "Disk write failed"
    assert result.duration_seconds == pytest.approx(3.0)
