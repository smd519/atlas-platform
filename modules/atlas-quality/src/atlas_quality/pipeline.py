"""Coordinates data quality assessment workflows, optional profiling, and reporting."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from atlas_core import (
    Artifact,
    MetadataProvider,
    StorageProvider,
    generate_execution_id,
    get_logger,
)
from atlas_storage import (
    FileSystemBackend,
    StorageError,
    StorageLocation,
    StorageManager,
    StorageRegistry,
)

from atlas_quality.engine import QualityEngine
from atlas_quality.exceptions import (
    InvalidQualityRequestError,
    QualityError,
    QualityExecutionTrackingError,
    QualityPipelineError,
)
from atlas_quality.models.quality_report import (
    QualityAssessmentRequest,
    QualityAssessmentStatus,
    QualityReport,
)
from atlas_quality.models.quality_result import QualityResult, RuleOutcomeStatus
from atlas_quality.models.quality_rule import (
    QualityRuleCategory,
    QualityRuleSeverity,
)
from atlas_quality.profiling.profiler import DatasetProfiler
from atlas_quality.tracking.execution_tracker import (
    ExecutionTracker,
    QualityAssessmentState,
    QualityExecutionRecord,
)

LATEST_QUALITY_ASSESSMENT_KEY = "latest_quality_assessment"
QUALITY_ASSESSMENT_HISTORY_KEY = "quality_assessment_history"
MAX_QUALITY_ASSESSMENT_HISTORY = 10


LATEST_QUALITY_ASSESSMENT_KEY = "latest_quality_assessment"
QUALITY_ASSESSMENT_HISTORY_KEY = "quality_assessment_history"
MAX_QUALITY_ASSESSMENT_HISTORY = 10


def create_default_storage_registry() -> StorageRegistry:
    """Create a :class:`StorageRegistry` populated with standard storage backends."""
    registry = StorageRegistry()
    registry.register("filesystem", FileSystemBackend)
    return registry


class QualityPipeline:
    """Coordinates dataset loading, rule execution, optional profiling, and report generation.

    Architectural guarantees:
    - Never moves or promotes datasets between Bronze, Silver, and Gold layers.
    - Never modifies the input dataset.
    - Supports both in-memory dataset inputs and datasets stored via :mod:`atlas_storage`.
    - Ensures tracking failures never mask the underlying assessment exception.
    """

    def __init__(
        self,
        engine: QualityEngine | None = None,
        profiler: DatasetProfiler | None = None,
        storage_manager: StorageManager | None = None,
        tracker: ExecutionTracker | None = None,
        *,
        storage_provider: StorageProvider | None = None,
        storage_registry: StorageRegistry | None = None,
        backend_name: str = "filesystem",
        metadata_provider: MetadataProvider | None = None,
    ) -> None:
        self._metadata = metadata_provider
        self._engine = engine or QualityEngine()
        self._profiler = profiler or DatasetProfiler()
        self._tracker = tracker or ExecutionTracker()
        self._logger = get_logger("atlas.quality.pipeline")

        if storage_manager is not None:
            self._storage_manager = storage_manager
        elif storage_provider is not None:
            self._storage_manager = StorageManager(backend=storage_provider)
        else:
            registry = storage_registry or create_default_storage_registry()
            backend_cls = registry.get(backend_name)
            self._storage_manager = StorageManager(backend=backend_cls())

    @property
    def engine(self) -> QualityEngine:
        """Return the pipeline's :class:`QualityEngine`."""
        return self._engine

    @property
    def profiler(self) -> DatasetProfiler:
        """Return the pipeline's :class:`DatasetProfiler`."""
        return self._profiler

    @property
    def tracker(self) -> ExecutionTracker:
        """Return the pipeline's :class:`ExecutionTracker`."""
        return self._tracker

    def run(
        self,
        request: QualityAssessmentRequest,
        *,
        execution_id: str | None = None,
        raise_on_error: bool | None = None,
    ) -> QualityReport:
        """Execute a quality assessment workflow and return an immutable :class:`QualityReport`."""
        if not isinstance(request, QualityAssessmentRequest):
            raise InvalidQualityRequestError(
                f"Expected QualityAssessmentRequest, got '{type(request).__name__}'."
            )

        should_raise = raise_on_error if raise_on_error is not None else request.raise_on_error

        record = self._tracker.create_execution(
            assessment_id=request.assessment_id,
            dataset_id=request.dataset_id,
            execution_id=execution_id,
            metadata=request.metadata,
        )
        exec_id = record.id
        artifact_ref = self._resolve_artifact_reference(request)

        self._logger.info(
            "quality_assessment_started",
            assessment_id=request.assessment_id,
            execution_id=exec_id,
            dataset_id=str(request.dataset_id),
            rule_count=len(request.rules),
            enable_profiling=request.enable_profiling,
        )

        try:
            self._tracker.start(exec_id)

            # 1. Load dataset (from memory, storage_location, or artifact)
            dataset = self._load_dataset(request, exec_id)

            # 2. Load any reference datasets specified as StorageLocation or Artifact
            resolved_references = self._load_reference_datasets(request, exec_id)

            # 3. Execute quality rules via QualityEngine
            self._tracker.record_transition(exec_id, QualityAssessmentState.EVALUATING)
            results, evaluated_rows = self._engine.execute(
                dataset,
                request.rules,
                reference_datasets=resolved_references,
                fail_fast=request.fail_fast,
            )

            # 4. Optionally profile dataset
            profile_dict: dict[str, Any] | None = None
            if request.enable_profiling:
                self._tracker.record_transition(exec_id, QualityAssessmentState.PROFILING)
                profile_obj = self._profiler.profile(dataset)
                profile_dict = profile_obj.model_dump()

            # 5. Build immutable QualityReport
            self._tracker.record_transition(exec_id, QualityAssessmentState.REPORTING)
            report = QualityReport(
                assessment_id=request.assessment_id,
                execution_id=exec_id,
                dataset_id=request.dataset_id,
                artifact_reference=artifact_ref,
                assessment_timestamp=datetime.now(UTC),
                total_evaluated_records=evaluated_rows,
                results=tuple(results),
                profile=profile_dict,
                metadata=dict(request.metadata),
            )

            # 6. Complete tracking
            self._tracker.complete(exec_id, report=report)

            self._logger.info(
                "quality_assessment_completed",
                assessment_id=request.assessment_id,
                execution_id=exec_id,
                overall_status=str(report.overall_status),
                quality_score=report.quality_score,
                passed_rules=report.passed_rules,
                failed_rules=report.failed_rules,
                execution_errors=report.execution_errors,
            )
        except Exception as exc:
            return self._handle_failure(
                request=request,
                initial_record=record,
                artifact_ref=artifact_ref,
                error=exc,
                should_raise=should_raise,
            )

        # Metadata publication is best-effort and must never alter the quality outcome.
        self._publish_quality_metadata(request, report)
        return report

    def _publish_quality_metadata(
        self,
        request: QualityAssessmentRequest,
        report: QualityReport,
    ) -> None:
        """Publish assessment results to the dataset catalog; never raises."""
        if self._metadata is None:
            return

        dataset_id = str(request.dataset_id)
        artifact_id = request.artifact.id if request.artifact is not None else None
        try:
            existing = self._metadata.get_dataset(dataset_id)
            if existing is None:
                self._logger.warning(
                    "quality_metadata_dataset_not_found",
                    dataset_id=dataset_id,
                    artifact_id=artifact_id,
                    execution_id=report.execution_id,
                    detail="Dataset is not registered; skipping quality metadata registration.",
                )
                return

            payload = self._build_assessment_payload(report, artifact_id)
            properties = dict(existing.metadata or {})
            raw_history = properties.get(QUALITY_ASSESSMENT_HISTORY_KEY)
            history = list(raw_history) if isinstance(raw_history, list) else []
            history.append(payload)
            history = history[-MAX_QUALITY_ASSESSMENT_HISTORY:]

            properties[LATEST_QUALITY_ASSESSMENT_KEY] = payload
            properties[QUALITY_ASSESSMENT_HISTORY_KEY] = history

            updated = existing.model_copy(update={"metadata": properties})
            self._metadata.register_dataset_metadata(updated, overwrite=True)
        except Exception as exc:
            self._logger.warning(
                "quality_metadata_registration_failed",
                dataset_id=dataset_id,
                artifact_id=artifact_id,
                execution_id=report.execution_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )

    @staticmethod
    def _build_assessment_payload(report: QualityReport, artifact_id: str | None) -> dict[str, Any]:
        """Build a JSON-compatible summary of ``report`` for catalog storage."""
        status = str(report.overall_status)
        return {
            "execution_id": report.execution_id,
            "assessment_id": report.assessment_id,
            "artifact_id": artifact_id,
            "assessment_timestamp": report.assessment_timestamp.isoformat(),
            "overall_status": status,
            "passed": report.overall_status
            in (QualityAssessmentStatus.PASSED, QualityAssessmentStatus.PASSED_WITH_WARNINGS),
            "total_assertions": report.total_rules,
            "passed_assertions": report.passed_rules,
            "failed_assertions": report.failed_rules,
            "execution_errors": report.execution_errors,
            "quality_score": report.quality_score,
        }

    def _load_dataset(
        self,
        request: QualityAssessmentRequest,
        execution_id: str,
    ) -> Any:
        if request.dataset is not None:
            return request.dataset

        self._tracker.record_transition(execution_id, QualityAssessmentState.LOADING)
        location = request.storage_location
        if location is None and request.artifact is not None:
            layer = str(request.metadata.get("layer", "bronze"))
            location = StorageLocation(
                layer=layer,
                path=request.artifact.path,
                format=request.artifact.format,
            )

        if location is None:
            raise InvalidQualityRequestError(
                "No dataset, storage_location, or artifact provided.",
                assessment_id=request.assessment_id,
                execution_id=execution_id,
            )

        try:
            return self._storage_manager.load(location)
        except StorageError as exc:
            raise QualityPipelineError(
                f"Failed to load dataset '{request.dataset_id}' from '{location.path}': {exc}",
                assessment_id=request.assessment_id,
                execution_id=execution_id,
            ) from exc
        except Exception as exc:
            raise QualityPipelineError(
                f"Unexpected error loading dataset '{request.dataset_id}' from "
                f"'{location.path}': {exc}",
                assessment_id=request.assessment_id,
                execution_id=execution_id,
            ) from exc

    def _load_reference_datasets(
        self,
        request: QualityAssessmentRequest,
        execution_id: str,
    ) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        for ref_name, ref_input in request.reference_datasets.items():
            if isinstance(ref_input, StorageLocation):
                try:
                    resolved[ref_name] = self._storage_manager.load(ref_input)
                except Exception as exc:
                    raise QualityPipelineError(
                        f"Failed to load reference dataset '{ref_name}' from "
                        f"'{ref_input.path}': {exc}",
                        assessment_id=request.assessment_id,
                        execution_id=execution_id,
                    ) from exc
            elif isinstance(ref_input, Artifact):
                loc = StorageLocation(
                    layer="bronze",
                    path=ref_input.path,
                    format=ref_input.format,
                )
                try:
                    resolved[ref_name] = self._storage_manager.load(loc)
                except Exception as exc:
                    raise QualityPipelineError(
                        f"Failed to load reference artifact '{ref_name}' from "
                        f"'{ref_input.path}': {exc}",
                        assessment_id=request.assessment_id,
                        execution_id=execution_id,
                    ) from exc
            else:
                resolved[ref_name] = ref_input
        return resolved

    @staticmethod
    def _resolve_artifact_reference(request: QualityAssessmentRequest) -> Artifact | None:
        if request.artifact is not None:
            return request.artifact
        if request.storage_location is not None:
            return Artifact(
                id=generate_execution_id(),
                dataset_id=str(request.dataset_id),
                path=request.storage_location.path,
                format=request.storage_location.format,
            )
        return None

    def _handle_failure(
        self,
        *,
        request: QualityAssessmentRequest,
        initial_record: QualityExecutionRecord,
        artifact_ref: Artifact | None,
        error: Exception,
        should_raise: bool,
    ) -> QualityReport:
        exec_id = initial_record.id
        if isinstance(error, QualityError):
            error.assessment_id = error.assessment_id or request.assessment_id
            error.execution_id = error.execution_id or exec_id
            quality_error: QualityError = error
        else:
            quality_error = QualityPipelineError(
                f"Unexpected quality assessment error: {error}",
                assessment_id=request.assessment_id,
                execution_id=exec_id,
            )
            quality_error.__cause__ = error

        metadata = dict(request.metadata)
        try:
            self._tracker.fail(
                exec_id,
                reason=str(quality_error),
                error_type=type(quality_error).__name__,
            )
        except QualityExecutionTrackingError as tracking_exc:
            # Never mask the original assessment exception if failure tracking fails.
            self._logger.error(
                "quality_failure_tracking_error",
                assessment_id=request.assessment_id,
                execution_id=exec_id,
                tracking_error=str(tracking_exc),
                original_error=str(quality_error),
            )
            metadata["tracking_error"] = str(tracking_exc)

        cause = quality_error.__cause__
        cause_str = f"{type(cause).__name__}: {cause}" if cause is not None else None

        error_result = QualityResult(
            rule_id=quality_error.rule_id or "pipeline.execution",
            rule_name=quality_error.rule_id or "pipeline.execution",
            category=QualityRuleCategory.SCHEMA,
            severity=QualityRuleSeverity.CRITICAL,
            passed=False,
            status=RuleOutcomeStatus.ERROR,
            evaluated_records=0,
            failed_records=0,
            success_percentage=0.0,
            is_execution_error=True,
            error_type=type(quality_error).__name__,
            error_message=str(quality_error),
            error_cause=cause_str,
            message=str(quality_error),
        )

        failed_report = QualityReport(
            assessment_id=request.assessment_id,
            execution_id=exec_id,
            dataset_id=request.dataset_id,
            artifact_reference=artifact_ref,
            overall_status=QualityAssessmentStatus.ERROR,
            quality_score=0.0,
            results=(error_result,),
            metadata=metadata,
        )
        try:
            self._tracker.store_report(failed_report)
        except Exception:
            pass

        if should_raise:
            raise quality_error

        return failed_report


__all__ = [
    "QualityPipeline",
    "create_default_storage_registry",
]
