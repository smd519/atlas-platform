"""Unit tests for atlas-core domain models, contracts, identifiers, and exports."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from atlas_core import (
    BRONZE,
    GOLD,
    SILVER,
    Artifact,
    Connector,
    Dataset,
    DatasetId,
    Execution,
    ExecutionManager,
    ExecutionStatus,
    JobId,
    MetadataProvider,
    Processor,
    QualityResult,
    QualityRule,
    Result,
    Source,
    SourceId,
    StorageProvider,
    ensure_directory,
    generate_dataset_id,
    generate_execution_id,
    require_not_empty,
)


class _DummyMetadataProvider(MetadataProvider):
    def __init__(self) -> None:
        self._datasets: dict[str, Dataset] = {}

    def register_dataset(self, dataset: Dataset) -> None:
        self._datasets[dataset.id] = dataset

    def get_dataset(self, dataset_id: str) -> Dataset | None:
        return self._datasets.get(dataset_id)


def test_dataset_model_and_metadata_provider_contract() -> None:
    ds = Dataset(
        id=generate_dataset_id(),
        name="weather_observations",
        layer=BRONZE,
        source_id="env-canada",
        format="parquet",
        path="data/bronze/weather.parquet",
        description="Hourly weather station readings",
        metadata={"region": "ON"},
    )
    assert ds.name == "weather_observations"
    assert ds.layer == "bronze"
    assert ds.metadata["region"] == "ON"

    provider = _DummyMetadataProvider()
    provider.register_dataset(ds)
    assert provider.get_dataset(ds.id) == ds
    assert provider.get_dataset("missing") is None


def test_execution_model_and_status_consistency() -> None:
    now = datetime.now(UTC)
    exec_minimal = Execution(id=generate_execution_id(), status=ExecutionStatus.PENDING)
    assert exec_minimal.status == ExecutionStatus.PENDING
    assert exec_minimal.started_at is None
    assert exec_minimal.completed_at is None

    exec_full = Execution(
        id="exec-1",
        status=ExecutionStatus.FAILED,
        started_at=now,
        completed_at=now,
        error_message="connection reset",
    )
    assert exec_full.status == ExecutionStatus.FAILED
    assert exec_full.started_at == now
    assert exec_full.error_message == "connection reset"


def test_shared_models_and_identifiers() -> None:
    artifact = Artifact(
        id="art-1",
        dataset_id=DatasetId("ds-1"),
        path="data/silver/clean.parquet",
        format="parquet",
    )
    source = Source(
        id=SourceId("src-1"),
        name="REST Feed",
        source_type="rest",
    )
    result = Result(success=True, message="ok")
    quality = QualityResult(passed=True, rule_name="not_null")
    job_id = JobId("job-1")

    assert artifact.dataset_id == "ds-1"
    assert source.source_type == "rest"
    assert result.success is True
    assert quality.passed is True
    assert job_id == "job-1"
    assert (BRONZE, SILVER, GOLD) == ("bronze", "silver", "gold")


def test_validation_and_paths(tmp_path: Path) -> None:
    require_not_empty("valid", "field")
    with pytest.raises(ValueError, match="field cannot be empty"):
        require_not_empty("", "field")

    nested = ensure_directory(tmp_path / "a" / "b")
    assert nested.exists()
    assert nested.is_dir()


def test_contracts_are_abstract() -> None:
    for contract_cls in (
        Connector,
        ExecutionManager,
        MetadataProvider,
        Processor,
        QualityRule,
        StorageProvider,
    ):
        with pytest.raises(TypeError):
            contract_cls()  # type: ignore[abstract]
