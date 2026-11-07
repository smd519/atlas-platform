"""Concrete step executor adapter for atlas-quality."""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Any

from atlas_core import Artifact, DatasetId, ExecutionStatus
from atlas_quality import (
    QualityAssessmentRequest,
    QualityAssessmentStatus,
    QualityReport,
)
from atlas_storage import StorageLocation

from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.exceptions import StepExecutionError
from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.execution.executor import BaseStepExecutor
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow_step import WorkflowStep

_SUPPORTED_ACTIONS = frozenset({"assess", "evaluate", "check", "run"})


class QualityExecutor(BaseStepExecutor):
    """Step executor adapter invoking the public ``QualityService`` interface."""

    def __init__(self, quality_service: Any | None = None) -> None:
        self._quality_service = quality_service

    def execute(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
        dependencies: OrchestrationDependencies | None = None,
    ) -> StepExecution:
        """Execute a dataset quality assessment step via the injected ``QualityService``."""
        start_ts = datetime.now(UTC)
        action = step.action.strip().lower()
        if action not in _SUPPORTED_ACTIONS:
            raise StepExecutionError(
                f"Unsupported quality action '{step.action}' for step '{step.step_id}'. "
                f"Supported actions: {sorted(_SUPPORTED_ACTIONS)}",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
                retryable=False,
                error_code="UNSUPPORTED_ACTION",
            )

        service = self._resolve_service(step, context, dependencies)
        request = self._build_request(step, context)

        try:
            report = service.assess(request)
        except StepExecutionError:
            raise
        except Exception as exc:
            raise StepExecutionError(
                f"Quality assessment execution failed for step '{step.step_id}': {exc}",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
            ) from exc

        outputs, module_exec_ref = self._normalize_report(report, step=step, context=context)
        end_ts = max(datetime.now(UTC), start_ts)

        return StepExecution(
            execution_id=context.execution_id,
            step_id=step.step_id,
            module_target=step.module_target,
            status=ExecutionStatus.SUCCEEDED,
            start_time=start_ts,
            end_time=end_ts,
            module_execution_ref=module_exec_ref,
            output_references=outputs,
        )

    def _resolve_service(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
        dependencies: OrchestrationDependencies | None,
    ) -> Any:
        service = self._quality_service
        if service is None and dependencies is not None:
            service = dependencies.quality_service or dependencies.resolve_service(
                step.module_target
            )
        if service is None:
            raise StepExecutionError(
                f"No QualityService injected in OrchestrationDependencies "
                f"for step '{step.step_id}'.",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
                retryable=False,
                error_code="MISSING_DEPENDENCY",
            )
        return service

    @staticmethod
    def _build_request(step: WorkflowStep, context: ExecutionContext) -> QualityAssessmentRequest:
        params = copy.deepcopy(step.parameters)
        if "request" in params and isinstance(params["request"], QualityAssessmentRequest):
            return params["request"]
        if "request" in params and isinstance(params["request"], dict):
            params = dict(params["request"])

        try:
            dataset_id = DatasetId(str(params.get("dataset_id", step.step_id)))

            storage_loc: StorageLocation | None = None
            raw_loc = params.get("storage_location")
            if isinstance(raw_loc, dict):
                storage_loc = StorageLocation(
                    layer=str(raw_loc.get("layer", "silver")),
                    path=str(raw_loc["path"]),
                    format=str(raw_loc.get("format", "parquet")),
                )
            elif isinstance(raw_loc, StorageLocation):
                storage_loc = raw_loc
            elif "input_path" in params:
                storage_loc = StorageLocation(
                    layer=str(params.get("layer", "silver")),
                    path=str(params["input_path"]),
                    format=str(params.get("format", "parquet")),
                )

            artifact_obj: Artifact | None = None
            raw_artifact = params.get("artifact")
            if isinstance(raw_artifact, dict):
                artifact_obj = Artifact(
                    id=str(raw_artifact.get("id", step.step_id)),
                    dataset_id=DatasetId(str(raw_artifact.get("dataset_id", dataset_id))),
                    path=str(raw_artifact["path"]),
                    format=str(raw_artifact.get("format", "parquet")),
                )
            elif isinstance(raw_artifact, Artifact):
                artifact_obj = raw_artifact

            req_kwargs: dict[str, Any] = {
                "dataset_id": dataset_id,
                "dataset": params.get("dataset"),
                "storage_location": storage_loc,
                "artifact": artifact_obj,
                "reference_datasets": dict(params.get("reference_datasets") or {}),
                "rules": list(params.get("rules") or []),
                "enable_profiling": bool(params.get("enable_profiling", False)),
                "fail_fast": bool(params.get("fail_fast", False)),
                "expose_samples": bool(params.get("expose_samples", False)),
                "sample_limit": int(params.get("sample_limit", 5)),
                "raise_on_error": bool(params.get("raise_on_error", False)),
                "metadata": dict(params.get("metadata") or {}),
            }
            if "assessment_id" in params and params["assessment_id"]:
                req_kwargs["assessment_id"] = str(params["assessment_id"])

            return QualityAssessmentRequest(**req_kwargs)
        except Exception as exc:
            raise StepExecutionError(
                f"Invalid quality assessment parameters for step '{step.step_id}': {exc}",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
                retryable=False,
                error_code="INVALID_PARAMETERS",
            ) from exc

    @staticmethod
    def _normalize_report(
        report: Any,
        *,
        step: WorkflowStep,
        context: ExecutionContext,
    ) -> tuple[dict[str, Any], str | None]:
        if isinstance(report, QualityReport):
            if report.overall_status in (
                QualityAssessmentStatus.FAILED,
                QualityAssessmentStatus.ERROR,
            ):
                raise StepExecutionError(
                    f"Quality assessment failed for dataset '{report.dataset_id}' "
                    f"with status '{report.overall_status}' "
                    f"(failed_rules={report.failed_rules}, "
                    f"execution_errors={report.execution_errors}).",
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step.step_id,
                )
            outputs: dict[str, Any] = {
                "assessment_id": report.assessment_id,
                "execution_id": report.execution_id,
                "dataset_id": str(report.dataset_id),
                "overall_status": str(report.overall_status),
                "quality_score": report.quality_score,
                "total_rules": report.total_rules,
                "passed_rules": report.passed_rules,
                "failed_rules": report.failed_rules,
                "execution_errors": report.execution_errors,
                "total_evaluated_records": report.total_evaluated_records,
                "total_failed_records": report.total_failed_records,
            }
            return outputs, report.execution_id

        if isinstance(report, dict):
            status_val = str(report.get("overall_status", "PASSED")).upper()
            if status_val in ("FAILED", "ERROR"):
                raise StepExecutionError(
                    f"Quality assessment step '{step.step_id}' failed with status '{status_val}'.",
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step.step_id,
                )
            exec_ref = str(report["execution_id"]) if "execution_id" in report else None
            return copy.deepcopy(report), exec_ref

        raise StepExecutionError(
            f"Unexpected report type '{type(report).__name__}' from QualityService.",
            workflow_id=context.workflow_id,
            workflow_version=context.workflow_version,
            execution_id=context.execution_id,
            step_id=step.step_id,
        )


__all__ = ["QualityExecutor"]
