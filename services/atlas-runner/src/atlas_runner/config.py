"""YAML loading, strict workspace path isolation, parameter merging, and validation helpers."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from atlas_orchestration import (
    CyclicDependencyError,
    InvalidStepDefinitionError,
    InvalidWorkflowDependencyError,
    OrchestrationError,
    WorkflowDefinition,
    WorkflowValidationResult,
    WorkflowValidator,
)
from pydantic import ValidationError

from atlas_runner.exceptions import RunnerConfigurationError, WorkspaceIsolationError
from atlas_runner.models import JobDefinition


def resolve_workspace_path(
    workspace: str | Path,
    target: str | Path,
    *,
    config_file: str | Path | None = None,
    field: str | None = None,
) -> Path:
    """Resolve ``target`` against ``workspace`` and enforce strict workspace isolation.

    Raises
    ------
    RunnerConfigurationError
        If ``workspace`` or ``target`` is empty.
    WorkspaceIsolationError
        If the resolved path escapes ``workspace`` via relative traversal
        (e.g. ``../../etc/shadow``) or an external absolute path.
    """
    cfg_str = str(config_file) if config_file is not None else None
    ws_str = str(workspace).strip() if workspace is not None else ""
    if not ws_str:
        raise RunnerConfigurationError(
            "Workspace path must not be empty.",
            config_file=cfg_str,
            field=field or "workspace",
        )

    target_str = str(target).strip() if target is not None else ""
    if not target_str:
        raise RunnerConfigurationError(
            "Target path must not be empty.",
            config_file=cfg_str,
            field=field or "path",
        )

    workspace_root = Path(ws_str).resolve()
    target_path = Path(target_str)
    if target_path.is_absolute():
        resolved = target_path.resolve()
    else:
        resolved = (workspace_root / target_path).resolve()

    if not resolved.is_relative_to(workspace_root):
        field_label = field or "path"
        loc_info = (
            f" in '{cfg_str}' (field '{field_label}')" if cfg_str else f" (field '{field_label}')"
        )
        raise WorkspaceIsolationError(
            f"Workspace isolation violation{loc_info}: path '{target_str}' resolves to "
            f"'{resolved}', which escapes workspace boundary '{workspace_root}'.",
            config_file=cfg_str,
            field=field_label,
            offending_path=target_str,
            workspace=str(workspace_root),
        )

    return resolved


def load_yaml_file(path: str | Path) -> dict[str, Any]:
    """Load and parse a YAML file using ``yaml.safe_load()``.

    Raises
    ------
    FileNotFoundError
        If ``path`` does not exist or is not a file.
    RunnerConfigurationError
        If the file contains invalid YAML syntax or does not parse into a dictionary.
    """
    file_path = Path(path)
    cfg_str = str(file_path)
    if not file_path.exists() or not file_path.is_file():
        err = FileNotFoundError(f"YAML configuration file not found: {file_path}")
        err.add_note(f"config_file={cfg_str} | field=file_path")
        raise err

    try:
        raw_text = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RunnerConfigurationError(
            f"Failed to read YAML file '{file_path}': {exc}",
            config_file=cfg_str,
            field="file_read",
        ) from exc

    try:
        parsed = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise RunnerConfigurationError(
            f"Invalid YAML syntax in '{file_path}' (field 'yaml_syntax'): {exc}",
            config_file=cfg_str,
            field="yaml_syntax",
        ) from exc

    if not isinstance(parsed, dict):
        raise RunnerConfigurationError(
            f"YAML configuration '{file_path}' (field 'root') must be a top-level mapping, "
            f"got {type(parsed).__name__}.",
            config_file=cfg_str,
            field="root",
        )

    return parsed


def merge_parameters(
    workflow_parameters: Mapping[str, Any] | None,
    job_parameters: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Merge workflow default parameters with job runtime parameter overrides.

    Merge Strategy
    --------------
    - Workflow parameters provide defaults.
    - Job parameters override matching top-level parameters.
    - Unspecified workflow parameters remain unchanged.
    - Nested dictionaries are replaced at the top level (no recursive merging).
    """
    merged: dict[str, Any] = copy.deepcopy(dict(workflow_parameters or {}))
    if job_parameters:
        for key, value in job_parameters.items():
            merged[key] = copy.deepcopy(value)
    return merged


def _resolve_maybe_relative_path(
    workspace: str | Path,
    raw_value: Any,
    *,
    config_file: str | Path | None = None,
    field: str | None = None,
) -> Any:
    """Resolve and audit a string path against ``workspace`` unless it is a context reference."""
    if not isinstance(raw_value, str):
        return raw_value
    stripped = raw_value.strip()
    if not stripped:
        return raw_value
    if "${" in stripped:
        if ".." in Path(stripped).parts:
            cfg_str = str(config_file) if config_file is not None else None
            raise WorkspaceIsolationError(
                f"Workspace isolation violation: path traversal segment '..' is not allowed "
                f"in parameter '{field or 'path'}' ('{raw_value}').",
                config_file=cfg_str,
                field=field or "path",
                offending_path=raw_value,
                workspace=str(Path(str(workspace)).resolve()),
            )
        return raw_value
    return str(
        resolve_workspace_path(
            workspace,
            stripped,
            config_file=config_file,
            field=field,
        )
    )


def resolve_step_paths(
    step_parameters: Mapping[str, Any],
    workspace: str | Path,
    *,
    config_file: str | Path | None = None,
    step_id: str | None = None,
) -> dict[str, Any]:
    """Resolve and audit all filesystem paths within a step's ``parameters``."""
    params = copy.deepcopy(dict(step_parameters))
    prefix = f"steps.{step_id}.parameters" if step_id else "parameters"

    # 1. source.config.path (atlas-ingestion LocalFileConnector) or source.path (atlas-processing)
    raw_source = params.get("source")
    if isinstance(raw_source, dict):
        if "path" in raw_source:
            raw_source["path"] = _resolve_maybe_relative_path(
                workspace,
                raw_source["path"],
                config_file=config_file,
                field=f"{prefix}.source.path",
            )
        raw_cfg = raw_source.get("config")
        if isinstance(raw_cfg, dict) and "path" in raw_cfg:
            raw_cfg["path"] = _resolve_maybe_relative_path(
                workspace,
                raw_cfg["path"],
                config_file=config_file,
                field=f"{prefix}.source.config.path",
            )

    # 2. destination.path (atlas-ingestion / atlas-processing)
    raw_dest = params.get("destination")
    if isinstance(raw_dest, dict) and "path" in raw_dest:
        raw_dest["path"] = _resolve_maybe_relative_path(
            workspace,
            raw_dest["path"],
            config_file=config_file,
            field=f"{prefix}.destination.path",
        )

    # 3. storage_location.path (atlas-quality)
    raw_loc = params.get("storage_location")
    if isinstance(raw_loc, dict) and "path" in raw_loc:
        raw_loc["path"] = _resolve_maybe_relative_path(
            workspace,
            raw_loc["path"],
            config_file=config_file,
            field=f"{prefix}.storage_location.path",
        )

    # 4. artifact.path (atlas-quality)
    raw_artifact = params.get("artifact")
    if isinstance(raw_artifact, dict) and "path" in raw_artifact:
        raw_artifact["path"] = _resolve_maybe_relative_path(
            workspace,
            raw_artifact["path"],
            config_file=config_file,
            field=f"{prefix}.artifact.path",
        )

    # 5. Flat path keys (input_path, output_path, path, or any *_path parameter)
    for key in list(params.keys()):
        if key in ("input_path", "output_path", "path") or (
            isinstance(key, str) and key.endswith("_path")
        ):
            params[key] = _resolve_maybe_relative_path(
                workspace,
                params[key],
                config_file=config_file,
                field=f"{prefix}.{key}",
            )

    return params


def apply_job_parameters_to_workflow_data(
    workflow_data: Mapping[str, Any],
    job_parameters: Mapping[str, Any] | None,
    *,
    workspace: str | Path | None = None,
    config_file: str | Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Apply job-level parameter overrides and workspace path resolution/auditing.

    Returns a tuple of ``(prepared_workflow_dict, merged_runtime_parameters)``.
    """
    wf_copy: dict[str, Any] = copy.deepcopy(dict(workflow_data))
    cfg_str = str(config_file) if config_file is not None else None

    raw_wf_params = wf_copy.pop("parameters", None)
    if raw_wf_params is not None and not isinstance(raw_wf_params, dict):
        raise RunnerConfigurationError(
            "Workflow top-level 'parameters' must be a mapping, "
            f"got {type(raw_wf_params).__name__}.",
            config_file=cfg_str,
            field="parameters",
        )

    merged_runtime_params = merge_parameters(raw_wf_params, job_parameters)

    # Audit any top-level path parameters in merged_runtime_params if workspace is provided
    if workspace is not None:
        for key, value in list(merged_runtime_params.items()):
            if key in ("input_path", "output_path", "path") or (
                isinstance(key, str) and key.endswith("_path")
            ):
                merged_runtime_params[key] = _resolve_maybe_relative_path(
                    workspace,
                    value,
                    config_file=config_file,
                    field=f"parameters.{key}",
                )

    raw_steps = wf_copy.get("steps")
    if isinstance(raw_steps, list):
        updated_steps: list[Any] = []
        for idx, step in enumerate(raw_steps):
            if not isinstance(step, dict):
                updated_steps.append(step)
                continue

            step_copy = copy.deepcopy(step)
            raw_step_params = step_copy.get("parameters")
            if isinstance(raw_step_params, dict) or raw_step_params is None:
                step_params: dict[str, Any] = copy.deepcopy(raw_step_params or {})
                step_id = step_copy.get("step_id")
                step_label = str(step_id) if isinstance(step_id, str) and step_id else str(idx)

                # Apply step-scoped overrides if keyed by step_id
                if isinstance(step_id, str) and step_id in merged_runtime_params:
                    step_specific = merged_runtime_params[step_id]
                    if isinstance(step_specific, Mapping):
                        step_params = merge_parameters(step_params, step_specific)

                # Apply top-level parameter overrides to matching step parameters
                for key, value in merged_runtime_params.items():
                    if isinstance(step_id, str) and key == step_id and isinstance(value, Mapping):
                        continue
                    if key in step_params:
                        existing_val = step_params[key]
                        # Preserve downstream step output references (e.g. "${steps.ingest...}")
                        if (
                            isinstance(existing_val, str)
                            and existing_val.strip().startswith("${steps.")
                            and key != "dataset_id"
                        ):
                            continue
                        step_params[key] = copy.deepcopy(value)
                    elif key in ("dataset_id", "input_path", "output_path", "path") or (
                        isinstance(key, str) and key.endswith("_path")
                    ):
                        step_params[key] = copy.deepcopy(value)

                if workspace is not None:
                    step_params = resolve_step_paths(
                        step_params,
                        workspace,
                        config_file=config_file,
                        step_id=step_label,
                    )

                step_copy["parameters"] = step_params

            updated_steps.append(step_copy)
        wf_copy["steps"] = updated_steps

    return wf_copy, merged_runtime_params


def _format_pydantic_loc(loc: Sequence[Any], raw_data: Mapping[str, Any] | None = None) -> str:
    if not loc:
        return "root"
    parts = [str(item) for item in loc]
    if (
        len(parts) >= 2
        and parts[0] == "steps"
        and parts[1].isdigit()
        and isinstance(raw_data, Mapping)
    ):
        steps = raw_data.get("steps")
        idx = int(parts[1])
        if isinstance(steps, list) and 0 <= idx < len(steps) and isinstance(steps[idx], Mapping):
            step_id = steps[idx].get("step_id")
            if isinstance(step_id, str) and step_id.strip():
                parts[1] = step_id.strip()
    return ".".join(parts)


def _infer_workflow_error_field(
    exc: BaseException,
    raw_data: Mapping[str, Any] | None = None,
) -> str:
    if isinstance(exc, ValidationError) and exc.errors():
        return _format_pydantic_loc(exc.errors()[0].get("loc", ()), raw_data)

    cause = exc.__cause__
    if isinstance(cause, ValidationError) and cause.errors():
        return _format_pydantic_loc(cause.errors()[0].get("loc", ()), raw_data)

    if isinstance(exc, CyclicDependencyError):
        cycle = getattr(exc, "cycle", None)
        if cycle and len(cycle) > 0:
            return f"steps.{cycle[0]}.depends_on"
        return "steps.depends_on"

    step_id = getattr(exc, "step_id", None)
    if isinstance(exc, InvalidWorkflowDependencyError):
        return f"steps.{step_id}.depends_on" if step_id else "steps.depends_on"

    msg = str(exc)
    if isinstance(exc, InvalidStepDefinitionError):
        prefix = f"steps.{step_id}" if step_id else "steps"
        for candidate in (
            "module_target",
            "action",
            "step_id",
            "timeout_seconds",
            "depends_on",
            "parameters",
            "retry_policy",
        ):
            if candidate in msg:
                return f"{prefix}.{candidate}"
        return prefix

    for wf_field in ("workflow_id", "name", "version", "steps", "metadata", "description"):
        if f"'{wf_field}'" in msg or wf_field in msg.lower():
            return wf_field

    return "workflow"


def _annotate_exception(
    exc: BaseException,
    *,
    config_file: str | Path,
    field: str,
) -> None:
    cfg_str = str(config_file)
    exc.add_note(f"config_file={cfg_str} | field={field}")
    if isinstance(exc, (OrchestrationError, RunnerConfigurationError)):
        object.__setattr__(exc, "config_file", cfg_str)
        object.__setattr__(exc, "field", field)
        base_msg = str(exc)
        if cfg_str not in base_msg or field not in base_msg:
            exc.args = (f"{base_msg} [file: {cfg_str}, field: {field}]",)


def extract_error_context(
    exc: BaseException,
    *,
    default_file: str | Path | None = None,
) -> tuple[str, str, str]:
    """Return ``(config_file, field, message)`` extracted from ``exc``."""
    cfg_file = getattr(exc, "config_file", None)
    field = getattr(exc, "field", None)

    for note in getattr(exc, "__notes__", ()) or ():
        if isinstance(note, str) and "config_file=" in note and "field=" in note:
            parts = dict(item.split("=", 1) for item in note.split(" | ") if "=" in item)
            cfg_file = cfg_file or parts.get("config_file")
            field = field or parts.get("field")

    if isinstance(exc, ValidationError) and exc.errors():
        first_err = exc.errors()[0]
        field = field or _format_pydantic_loc(first_err.get("loc", ()))
        detail = first_err.get("msg", str(exc))
    else:
        detail = str(exc)

    resolved_file = str(cfg_file or default_file or "unknown")
    resolved_field = str(field or _infer_workflow_error_field(exc))
    return resolved_file, resolved_field, detail


def load_job_definition(
    workspace: str | Path,
    job_path: str | Path,
) -> tuple[JobDefinition, Path]:
    """Load and validate a :class:`JobDefinition` from ``job_path`` within ``workspace``."""
    resolved_job_path = resolve_workspace_path(
        workspace,
        job_path,
        field="job_path",
    )
    raw_job = load_yaml_file(resolved_job_path)
    try:
        job_def = JobDefinition.model_validate(raw_job)
    except ValidationError as exc:
        err_field = _format_pydantic_loc(exc.errors()[0].get("loc", ())) if exc.errors() else "root"
        _annotate_exception(exc, config_file=resolved_job_path, field=err_field)
        raise
    except Exception as exc:
        err_field = (
            "job_id"
            if "job_id" in str(exc)
            else ("workflow_ref" if "workflow_ref" in str(exc) else "root")
        )
        raise RunnerConfigurationError(
            f"Invalid job configuration in '{resolved_job_path}' (field '{err_field}'): {exc}",
            config_file=str(resolved_job_path),
            field=err_field,
        ) from exc
    return job_def, resolved_job_path


def load_workflow_definition(
    workspace: str | Path,
    workflow_ref: str | Path,
    *,
    job_parameters: Mapping[str, Any] | None = None,
    job_file: str | Path | None = None,
) -> tuple[WorkflowDefinition, dict[str, Any], Path]:
    """Load, merge parameters, audit paths, and construct a :class:`WorkflowDefinition`."""
    resolved_wf_path = resolve_workspace_path(
        workspace,
        workflow_ref,
        config_file=job_file,
        field="workflow_ref",
    )
    raw_workflow = load_yaml_file(resolved_wf_path)
    prepared_data, merged_params = apply_job_parameters_to_workflow_data(
        raw_workflow,
        job_parameters,
        workspace=workspace,
        config_file=resolved_wf_path,
    )
    try:
        workflow_def = WorkflowDefinition.model_validate(prepared_data)
    except Exception as exc:
        err_field = _infer_workflow_error_field(exc, prepared_data)
        _annotate_exception(exc, config_file=resolved_wf_path, field=err_field)
        raise
    return workflow_def, merged_params, resolved_wf_path


def validate_workflow_dag(
    workflow: WorkflowDefinition,
    *,
    config_file: str | Path | None = None,
    validator: WorkflowValidator | None = None,
) -> WorkflowValidationResult:
    """Validate workflow step dependencies, acyclicity, and topological ordering."""
    active_validator = validator or WorkflowValidator()
    try:
        return active_validator.validate(workflow)
    except Exception as exc:
        if config_file is not None:
            err_field = _infer_workflow_error_field(exc)
            _annotate_exception(exc, config_file=config_file, field=err_field)
        raise


__all__ = [
    "apply_job_parameters_to_workflow_data",
    "extract_error_context",
    "load_job_definition",
    "load_workflow_definition",
    "load_yaml_file",
    "merge_parameters",
    "resolve_step_paths",
    "resolve_workspace_path",
    "validate_workflow_dag",
]
