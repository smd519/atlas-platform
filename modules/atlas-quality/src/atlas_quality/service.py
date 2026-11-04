"""Public quality assessment service and module entry point."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from atlas_core import (
    Artifact,
    DatasetId,
    ExecutionStatus,
    StorageProvider,
    generate_execution_id,
)
from atlas_storage import StorageLocation, StorageManager, StorageRegistry

from atlas_quality.engine import QualityEngine
from atlas_quality.models.quality_report import (
    QualityAssessmentRequest,
    QualityReport,
)
from atlas_quality.models.quality_rule import QualityRuleConfig
from atlas_quality.pipeline import QualityPipeline
from atlas_quality.profiling.profiler import DatasetProfile, DatasetProfiler
from atlas_quality.rules.registry import QualityRuleRegistry
from atlas_quality.tracking.execution_tracker import (
    ExecutionTracker,
    QualityExecutionRecord,
)


class QualityService:
    """Public dependency-injected facade for assessing dataset quality.

    Provides operations to assess datasets from storage or memory, retrieve
    immutable :class:`QualityReport` snapshots, and inspect tracked assessment
    executions.
    """

    def __init__(
        self,
        pipeline: QualityPipeline | None = None,
        *,
        engine: QualityEngine | None = None,
        rule_registry: QualityRuleRegistry | None = None,
        profiler: DatasetProfiler | None = None,
        tracker: ExecutionTracker | None = None,
        storage_manager: StorageManager | None = None,
        storage_provider: StorageProvider | None = None,
        storage_registry: StorageRegistry | None = None,
    ) -> None:
        if pipeline is not None:
            self._pipeline = pipeline
        else:
            resolved_engine = engine or QualityEngine(registry=rule_registry)
            self._pipeline = QualityPipeline(
                engine=resolved_engine,
                profiler=profiler,
                storage_manager=storage_manager,
                tracker=tracker,
                storage_provider=storage_provider,
                storage_registry=storage_registry,
            )

    def assess(
        self,
        request: QualityAssessmentRequest,
        *,
        execution_id: str | None = None,
        raise_on_error: bool | None = None,
    ) -> QualityReport:
        """Execute a :class:`QualityAssessmentRequest` and return a :class:`QualityReport`."""
        return self._pipeline.run(
            request,
            execution_id=execution_id,
            raise_on_error=raise_on_error,
        )

    def assess_dataset(
        self,
        dataset: Any,
        rules: Sequence[QualityRuleConfig | dict[str, Any]],
        *,
        dataset_id: DatasetId | str = "in_memory_dataset",
        assessment_id: str | None = None,
        storage_location: StorageLocation | None = None,
        artifact: Artifact | None = None,
        reference_datasets: dict[str, Any] | None = None,
        enable_profiling: bool = False,
        fail_fast: bool = False,
        expose_samples: bool = False,
        sample_limit: int = 5,
        raise_on_error: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> QualityReport:
        """Convenience method to assess an already-loaded dataset or storage location."""
        request = QualityAssessmentRequest(
            assessment_id=assessment_id or generate_execution_id(),
            dataset_id=DatasetId(str(dataset_id)),
            dataset=dataset,
            storage_location=storage_location,
            artifact=artifact,
            reference_datasets=dict(reference_datasets or {}),
            rules=list(rules),
            enable_profiling=enable_profiling,
            fail_fast=fail_fast,
            expose_samples=expose_samples,
            sample_limit=sample_limit,
            raise_on_error=raise_on_error,
            metadata=dict(metadata or {}),
        )
        return self.assess(request)

    def profile_dataset(self, dataset: Any) -> DatasetProfile:
        """Compute and return a standalone :class:`DatasetProfile` for ``dataset``."""
        return self._pipeline.profiler.profile(dataset)

    def get_report(self, assessment_id: str) -> QualityReport:
        """Retrieve a previously generated :class:`QualityReport` by ``assessment_id``."""
        return self._pipeline.tracker.get_report(assessment_id)

    def get_execution(self, execution_id: str) -> QualityExecutionRecord:
        """Retrieve the tracked :class:`QualityExecutionRecord` for ``execution_id``."""
        return self._pipeline.tracker.get_execution(execution_id)

    def list_executions(
        self,
        *,
        dataset_id: DatasetId | str | None = None,
        status: ExecutionStatus | None = None,
    ) -> list[QualityExecutionRecord]:
        """List tracked :class:`QualityExecutionRecord` entries with optional filtering."""
        return self._pipeline.tracker.list_executions(
            dataset_id=dataset_id,
            status=status,
        )


__all__ = ["QualityService"]
