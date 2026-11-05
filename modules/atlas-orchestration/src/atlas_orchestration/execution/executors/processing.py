"""Concrete step executor adapter for atlas-processing."""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Any

from atlas_core import DatasetId, ExecutionStatus
from atlas_processing import (
    ProcessingExecutionConfig,
    ProcessingRequest,
    ProcessingResult,
    TransformationStep,
)
from atlas_storage import StorageLocation

from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.exceptions import StepExecutionError
from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.execution.executor import BaseStepExecutor
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow_step import WorkflowStep

_SUPPORTED_ACTIONS = frozenset({"process", "transform", "run"})


class ProcessingExecutor(BaseStepExecutor):
    """Step executor adapter invoking the public ``ProcessingService`` interface."""

    def __init__(self, processing_service: Any | None = None) -> None:
        self._processing_service = processing_service

    def execute(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
        dependencies: OrchestrationDependencies | None = None,
    ) -> StepExecution:
        """Execute a dataset transformation step via the injected ``ProcessingService``."""
        start_ts = datetime.now(UTC)
        action = step.action.strip().lower()
        if action not in _SUPPORTED_ACTIONS:
            raise StepExecutionError(
                f"Unsupported processing action '{step.action}' for step '{step.step_id}'. "
                f"Supported actions: {sorted(_SUPPORTED_ACTIONS)}",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
            )

        service = self._resolve_service(step, context, dependencies)
        request = self._build_request(step, context)

        try:
            result = service.process(request)
        except StepExecutionError:
            raise
        except Exception as exc:
            raise StepExecutionError(
                f"Processing execution failed for step '{step.step_id}': {exc}",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
            ) from exc

        outputs, module_exec_ref = self._normalize_result(
            result,
            request=request,
            step=step,
            context=context,
        )
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
        service = self._processing_service
        if service is None and dependencies is not None:
            service = dependencies.processing_service or dependencies.resolve_service(
                step.module_target
            )
        if service is None:
            raise StepExecutionError(
                f"No ProcessingService injected in OrchestrationDependencies "
                f"for step '{step.step_id}'.",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
            )
        return service

    @staticmethod
    def _build_request(step: WorkflowStep, context: ExecutionContext) -> ProcessingRequest:
        params = copy.deepcopy(step.parameters)
        if "request" in params and isinstance(params["request"], ProcessingRequest):
            return params["request"]
        if "request" in params and isinstance(params["request"], dict):
            params = dict(params["request"])

        try:
            raw_source = params.get("source")
            if isinstance(raw_source, dict):
                source_loc = StorageLocation(
                    layer=str(raw_source.get("layer", "bronze")),
                    path=str(raw_source["path"]),
                    format=str(raw_source.get("format", "parquet")),
                )
            elif isinstance(raw_source, StorageLocation):
                source_loc = raw_source
            elif "input_path" in params:
                source_loc = StorageLocation(
                    layer=str(params.get("source_layer", "bronze")),
                    path=str(params["input_path"]),
                    format=str(params.get("source_format", "parquet")),
                )
            else:
                raise ValueError("Missing 'source' or 'input_path' in processing parameters.")

            raw_dest = params.get("destination")
            if isinstance(raw_dest, dict):
                dest_loc = StorageLocation(
                    layer=str(raw_dest.get("layer", "silver")),
                    path=str(raw_dest["path"]),
                    format=str(raw_dest.get("format", source_loc.format)),
                )
            elif isinstance(raw_dest, StorageLocation):
                dest_loc = raw_dest
            elif "output_path" in params:
                dest_loc = StorageLocation(
                    layer=str(params.get("destination_layer", "silver")),
                    path=str(params["output_path"]),
                    format=str(params.get("destination_format", source_loc.format)),
                )
            else:
                raise ValueError("Missing 'destination' or 'output_path' in processing parameters.")

            raw_transforms = params.get("transformations") or []
            transforms: list[TransformationStep] = []
            for idx, item in enumerate(raw_transforms):
                if isinstance(item, TransformationStep):
                    transforms.append(item)
                elif isinstance(item, dict):
                    transforms.append(TransformationStep(**item))
                else:
                    raise ValueError(f"Invalid transformation specification at index {idx}.")

            raw_exec_cfg = params.get("execution_config")
            exec_cfg = (
                ProcessingExecutionConfig(**raw_exec_cfg)
                if isinstance(raw_exec_cfg, dict)
                else (
                    raw_exec_cfg
                    if isinstance(raw_exec_cfg, ProcessingExecutionConfig)
                    else ProcessingExecutionConfig()
                )
            )

            req_kwargs: dict[str, Any] = {
                "dataset_id": DatasetId(str(params.get("dataset_id", step.step_id))),
                "source": source_loc,
                "destination": dest_loc,
                "transformations": transforms,
                "execution_config": exec_cfg,
                "metadata": dict(params.get("metadata") or {}),
            }
            if "request_id" in params and params["request_id"]:
                req_kwargs["request_id"] = str(params["request_id"])

            return ProcessingRequest(**req_kwargs)
        except Exception as exc:
            raise StepExecutionError(
                f"Invalid processing parameters for step '{step.step_id}': {exc}",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
            ) from exc

    @staticmethod
    def _normalize_result(
        result: Any,
        *,
        request: ProcessingRequest,
        step: WorkflowStep,
        context: ExecutionContext,
    ) -> tuple[dict[str, Any], str | None]:
        if isinstance(result, ProcessingResult):
            if not result.success or result.status == ExecutionStatus.FAILED:
                raise StepExecutionError(
                    result.error_message
                    or result.message
                    or f"Processing step '{step.step_id}' failed.",
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step.step_id,
                )
            artifact_path = (
                result.output_artifact.path
                if result.output_artifact is not None
                else request.destination.path
            )
            outputs: dict[str, Any] = {
                "execution_id": result.execution_id,
                "request_id": result.request_id,
                "dataset_id": str(result.dataset_id),
                "status": str(result.status),
                "artifact_path": artifact_path,
                "destination": {
                    "layer": request.destination.layer,
                    "path": request.destination.path,
                    "format": request.destination.format,
                },
                "output_artifact": (
                    {
                        "id": result.output_artifact.id,
                        "dataset_id": str(result.output_artifact.dataset_id),
                        "path": result.output_artifact.path,
                        "format": result.output_artifact.format,
                    }
                    if result.output_artifact is not None
                    else None
                ),
                "input_row_count": result.statistics.input_row_count,
                "output_row_count": result.statistics.output_row_count,
                "rows_delta": result.statistics.rows_delta,
                "transformations_applied": result.statistics.transformations_applied,
            }
            return outputs, result.execution_id

        if isinstance(result, dict):
            exec_ref = str(result["execution_id"]) if "execution_id" in result else None
            return copy.deepcopy(result), exec_ref

        raise StepExecutionError(
            f"Unexpected result type '{type(result).__name__}' from ProcessingService.",
            workflow_id=context.workflow_id,
            workflow_version=context.workflow_version,
            execution_id=context.execution_id,
            step_id=step.step_id,
        )


__all__ = ["ProcessingExecutor"]
