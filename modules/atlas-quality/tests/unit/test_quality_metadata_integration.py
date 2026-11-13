"""Unit tests for QualityPipeline/QualityService metadata catalog integration."""

from __future__ import annotations

import json
from typing import Any

from atlas_core import Artifact, Dataset, DatasetId, MetadataProvider
from atlas_quality import (
    QualityAssessmentRequest,
    QualityAssessmentStatus,
    QualityPipeline,
    QualityService,
)
from atlas_quality.pipeline import (
    LATEST_QUALITY_ASSESSMENT_KEY,
    MAX_QUALITY_ASSESSMENT_HISTORY,
    QUALITY_ASSESSMENT_HISTORY_KEY,
)
from atlas_storage import FileSystemBackend, StorageLocation, StorageManager

DATASET = [{"id": 1, "email": "a@example.com"}, {"id": 2, "email": None}]
RULES: list[Any] = [
    {"rule_id": "completeness.not_null", "params": {"column": "id"}},
    {"rule_id": "completeness.not_null", "params": {"column": "email"}},
]


class StubStorageManager(StorageManager):
    """StorageManager returning fixed in-memory data."""

    def __init__(self, data: Any) -> None:
        super().__init__(backend=FileSystemBackend())
        self._data = data

    def load(self, location: StorageLocation) -> Any:
        return self._data


class RecordingLogger:
    """Minimal structlog-compatible logger capturing warnings."""

    def __init__(self) -> None:
        self.warnings: list[tuple[str, dict[str, Any]]] = []

    def warning(self, event: str, **kwargs: Any) -> None:
        self.warnings.append((event, kwargs))

    def info(self, event: str, **kwargs: Any) -> None:
        pass

    def error(self, event: str, **kwargs: Any) -> None:
        pass


class FakeMetadataProvider(MetadataProvider):
    """In-memory MetadataProvider that records update calls."""

    def __init__(
        self,
        dataset: Dataset | None = None,
        *,
        get_error: Exception | None = None,
        update_error: Exception | None = None,
    ) -> None:
        self.dataset = dataset
        self.get_error = get_error
        self.update_error = update_error
        self.get_calls: list[str] = []
        self.update_calls: list[tuple[Dataset, bool]] = []

    def register_dataset(self, dataset: Any, *, overwrite: bool = False) -> None:
        if self.update_error is not None:
            raise self.update_error
        self.update_calls.append((dataset, overwrite))
        self.dataset = dataset

    def get_dataset(self, dataset_id: str) -> Dataset | None:
        self.get_calls.append(dataset_id)
        if self.get_error is not None:
            raise self.get_error
        return self.dataset


def _dataset(**metadata: Any) -> Dataset:
    return Dataset(id="orders", name="orders", metadata=dict(metadata))


def _request(
    assessment_id: str = "assess-1", artifact: Artifact | None = None
) -> QualityAssessmentRequest:
    return QualityAssessmentRequest(
        assessment_id=assessment_id,
        dataset_id="orders",
        dataset=DATASET,
        artifact=artifact,
        rules=RULES,
    )


def _pipeline(provider: MetadataProvider | None) -> tuple[QualityPipeline, RecordingLogger]:
    pipeline = QualityPipeline(metadata_provider=provider)
    logger = RecordingLogger()
    pipeline._logger = logger
    return pipeline, logger


def test_success_registers_artifact_aware_payload_and_preserves_properties() -> None:
    provider = FakeMetadataProvider(_dataset(team="data-eng"))
    pipeline, logger = _pipeline(provider)
    artifact = Artifact(id="art-1", dataset_id="orders", path="/tmp/orders.parquet", format="csv")

    report = pipeline.run(_request(artifact=artifact), execution_id="exec-1")

    assert provider.get_calls == ["orders"]
    assert len(provider.update_calls) == 1
    updated, overwrite = provider.update_calls[0]
    assert overwrite is True
    props = updated.metadata
    assert props["team"] == "data-eng"

    latest = props[LATEST_QUALITY_ASSESSMENT_KEY]
    assert latest["execution_id"] == "exec-1"
    assert latest["artifact_id"] == "art-1"
    assert latest["overall_status"] == str(report.overall_status)
    assert latest["total_assertions"] == report.total_rules == 2
    assert latest["passed_assertions"] == report.passed_rules == 1
    assert latest["failed_assertions"] == report.failed_rules == 1
    assert latest["quality_score"] == report.quality_score
    assert latest["assessment_timestamp"] == report.assessment_timestamp.isoformat()
    assert props[QUALITY_ASSESSMENT_HISTORY_KEY] == [latest]
    json.dumps(props)  # JSON-compatible
    assert logger.warnings == []


def test_payload_artifact_id_is_none_without_artifact_and_service_wiring() -> None:
    provider = FakeMetadataProvider(_dataset())
    service = QualityService(metadata_provider=provider)

    report = service.assess(_request())

    latest = provider.update_calls[0][0].metadata[LATEST_QUALITY_ASSESSMENT_KEY]
    assert latest["artifact_id"] is None
    assert latest["execution_id"] == report.execution_id


def test_history_is_bounded_and_evicts_oldest() -> None:
    provider = FakeMetadataProvider(_dataset())
    pipeline, _ = _pipeline(provider)

    total = MAX_QUALITY_ASSESSMENT_HISTORY + 3
    for i in range(total):
        pipeline.run(_request(f"assess-{i}"), execution_id=f"exec-{i}")

    assert MAX_QUALITY_ASSESSMENT_HISTORY == 10
    assert provider.dataset is not None
    history = provider.dataset.metadata[QUALITY_ASSESSMENT_HISTORY_KEY]
    assert len(history) == 10
    assert [h["execution_id"] for h in history] == [f"exec-{i}" for i in range(3, total)]
    assert provider.dataset.metadata[LATEST_QUALITY_ASSESSMENT_KEY]["execution_id"] == "exec-12"


def test_malformed_existing_history_is_reset() -> None:
    provider = FakeMetadataProvider(_dataset(**{QUALITY_ASSESSMENT_HISTORY_KEY: "garbage"}))
    pipeline, _ = _pipeline(provider)

    pipeline.run(_request(), execution_id="exec-1")

    assert provider.dataset is not None
    assert len(provider.dataset.metadata[QUALITY_ASSESSMENT_HISTORY_KEY]) == 1


def test_missing_dataset_warns_and_skips_update() -> None:
    provider = FakeMetadataProvider(None)
    pipeline, logger = _pipeline(provider)

    report = pipeline.run(_request(), execution_id="exec-1")

    assert report.overall_status != QualityAssessmentStatus.ERROR
    assert provider.update_calls == []
    assert len(logger.warnings) == 1
    event, fields = logger.warnings[0]
    assert event == "quality_metadata_dataset_not_found"
    assert fields["dataset_id"] == "orders"
    assert fields["execution_id"] == "exec-1"


def test_update_failure_is_isolated_and_logged() -> None:
    provider = FakeMetadataProvider(_dataset(), update_error=RuntimeError("catalog down"))
    pipeline, logger = _pipeline(provider)
    artifact = Artifact(id="art-9", dataset_id="orders", path="/tmp/o.csv", format="csv")

    report = pipeline.run(_request(artifact=artifact), execution_id="exec-1", raise_on_error=True)

    assert report.overall_status == QualityAssessmentStatus.FAILED
    assert len(logger.warnings) == 1
    event, fields = logger.warnings[0]
    assert event == "quality_metadata_registration_failed"
    assert fields["dataset_id"] == "orders"
    assert fields["artifact_id"] == "art-9"
    assert fields["execution_id"] == "exec-1"
    assert "catalog down" in fields["error"]
    assert pipeline.tracker.get_report("assess-1") == report


def test_get_failure_is_isolated_and_logged() -> None:
    provider = FakeMetadataProvider(_dataset(), get_error=RuntimeError("lookup failed"))
    pipeline, logger = _pipeline(provider)

    report = pipeline.run(_request(), execution_id="exec-1")

    assert report.overall_status != QualityAssessmentStatus.ERROR
    assert provider.update_calls == []
    assert logger.warnings[0][0] == "quality_metadata_registration_failed"


def test_no_provider_preserves_existing_behavior() -> None:
    pipeline, logger = _pipeline(None)

    report = pipeline.run(_request(), execution_id="exec-1")

    assert report.execution_id == "exec-1"
    assert logger.warnings == []


def test_storage_location_only_never_records_generated_artifact_id() -> None:
    """A pipeline-generated (unregistered) artifact reference must not leak into the catalog."""
    provider = FakeMetadataProvider(_dataset())
    pipeline = QualityPipeline(
        storage_manager=StubStorageManager(DATASET), metadata_provider=provider
    )
    request = QualityAssessmentRequest(
        assessment_id="assess-loc",
        dataset_id=DatasetId("orders"),
        storage_location=StorageLocation(layer="bronze", path="/tmp/orders.csv", format="csv"),
        rules=RULES,
    )

    report = pipeline.run(request, execution_id="exec-loc")

    assert report.artifact_reference is not None  # generated, unregistered id
    latest = provider.update_calls[0][0].metadata[LATEST_QUALITY_ASSESSMENT_KEY]
    assert latest["artifact_id"] is None
