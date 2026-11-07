"""Concrete step executor adapter for atlas-ingestion."""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Any

from atlas_connectors import SourceConfig
from atlas_core import DatasetId, ExecutionStatus, SourceId
from atlas_ingestion import ExecutionConfig, IngestionRequest, IngestionResult
from atlas_storage import StorageLocation

from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.exceptions import StepExecutionError
from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.execution.executor import BaseStepExecutor
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow_step import WorkflowStep

_SUPPORTED_ACTIONS = frozenset({"ingest", "run"})


class IngestionExecutor(BaseStepExecutor):
    """Step executor adapter invoking the public ``IngestionService`` interface."""

    def __init__(self, ingestion_service: Any | None = None) -> None:
        self._ingestion_service = ingestion_service

    def execute(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
        dependencies: OrchestrationDependencies | None = None,
    ) -> StepExecution:
        """Execute an ingestion step via the injected ``IngestionService``."""
        start_ts = datetime.now(UTC)
        action = step.action.strip().lower()
        if action not in _SUPPORTED_ACTIONS:
            raise StepExecutionError(
                f"Unsupported ingestion action '{step.action}' for step '{step.step_id}'. "
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
            result = service.ingest(request)
        except StepExecutionError:
            raise
        except Exception as exc:
            raise StepExecutionError(
                f"Ingestion execution failed for step '{step.step_id}': {exc}",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
            ) from exc

        outputs, module_exec_ref = self._normalize_result(result, step=step, context=context)
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
        service = self._ingestion_service
        if service is None and dependencies is not None:
            service = dependencies.ingestion_service or dependencies.resolve_service(
                step.module_target
            )
        if service is None:
            raise StepExecutionError(
                f"No IngestionService injected in OrchestrationDependencies "
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
    def _build_request(step: WorkflowStep, context: ExecutionContext) -> IngestionRequest:
        params = copy.deepcopy(step.parameters)
        if "request" in params and isinstance(params["request"], IngestionRequest):
            return params["request"]
        if "request" in params and isinstance(params["request"], dict):
            params = dict(params["request"])

        try:
            raw_source = params.get("source")
            if isinstance(raw_source, dict):
                source_cfg = SourceConfig(
                    source_id=SourceId(str(raw_source["source_id"])),
                    source_name=str(raw_source["source_name"]),
                    source_type=str(raw_source["source_type"]),
                    config=dict(raw_source.get("config") or {}),
                )
            elif isinstance(raw_source, SourceConfig):
                source_cfg = raw_source
            else:
                raise ValueError("Missing or invalid 'source' configuration.")

            raw_dest = params.get("destination")
            if isinstance(raw_dest, dict):
                dest_loc = StorageLocation(
                    layer=str(raw_dest.get("layer", "bronze")),
                    path=str(raw_dest["path"]),
                    format=str(raw_dest.get("format", params.get("source_format", "parquet"))),
                )
            elif isinstance(raw_dest, StorageLocation):
                dest_loc = raw_dest
            else:
                raise ValueError("Missing or invalid 'destination' configuration.")

            raw_exec_cfg = params.get("execution_config")
            exec_cfg = (
                ExecutionConfig(**raw_exec_cfg)
                if isinstance(raw_exec_cfg, dict)
                else (
                    raw_exec_cfg if isinstance(raw_exec_cfg, ExecutionConfig) else ExecutionConfig()
                )
            )

            req_kwargs: dict[str, Any] = {
                "dataset_id": DatasetId(str(params["dataset_id"])),
                "source": source_cfg,
                "destination": dest_loc,
                "source_format": str(params.get("source_format", dest_loc.format)),
                "format_options": dict(params.get("format_options") or {}),
                "execution_config": exec_cfg,
                "metadata": dict(params.get("metadata") or {}),
            }
            if "request_id" in params and params["request_id"]:
                req_kwargs["request_id"] = str(params["request_id"])

            return IngestionRequest(**req_kwargs)
        except Exception as exc:
            raise StepExecutionError(
                f"Invalid ingestion parameters for step '{step.step_id}': {exc}",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
                retryable=False,
                error_code="INVALID_PARAMETERS",
            ) from exc

    @staticmethod
    def _normalize_result(
        result: Any,
        *,
        step: WorkflowStep,
        context: ExecutionContext,
    ) -> tuple[dict[str, Any], str | None]:
        if isinstance(result, IngestionResult):
            if not result.success or result.status == ExecutionStatus.FAILED:
                raise StepExecutionError(
                    result.error_message
                    or result.message
                    or f"Ingestion step '{step.step_id}' failed.",
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step.step_id,
                )
            artifact_path = (
                result.artifacts[0].path if result.artifacts else result.destination.path
            )
            outputs: dict[str, Any] = {
                "execution_id": result.execution_id,
                "request_id": result.request_id,
                "dataset_id": str(result.dataset_id),
                "status": str(result.status),
                "artifact_path": artifact_path,
                "source_name": result.source.name,
                "row_count": result.row_count,
                "destination": {
                    "layer": result.destination.layer,
                    "path": result.destination.path,
                    "format": result.destination.format,
                },
                "artifacts": [
                    {
                        "id": art.id,
                        "dataset_id": str(art.dataset_id),
                        "path": art.path,
                        "format": art.format,
                    }
                    for art in result.artifacts
                ],
            }
            return outputs, result.execution_id

        if isinstance(result, dict):
            exec_ref = str(result["execution_id"]) if "execution_id" in result else None
            return copy.deepcopy(result), exec_ref

        raise StepExecutionError(
            f"Unexpected result type '{type(result).__name__}' from IngestionService.",
            workflow_id=context.workflow_id,
            workflow_version=context.workflow_version,
            execution_id=context.execution_id,
            step_id=step.step_id,
        )


__all__ = ["IngestionExecutor"]
