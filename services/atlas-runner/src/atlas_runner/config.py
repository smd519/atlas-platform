"""YAML loading, path resolution, and parameter merging for atlas-runner."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from atlas_orchestration import WorkflowDefinition
from pydantic import ValidationError

from atlas_runner.exceptions import RunnerConfigurationError
from atlas_runner.models import JobDefinition


def resolve_workspace_path(workspace: str | Path, target: str | Path) -> Path:
    """Resolve ``target`` relative to ``workspace`` unless ``target`` is already absolute."""
    ws_str = str(workspace).strip() if workspace is not None else ""
    if not ws_str:
        raise RunnerConfigurationError("Workspace path must not be empty.")

    target_str = str(target).strip() if target is not None else ""
    if not target_str:
        raise RunnerConfigurationError("Target path must not be empty.")

    workspace_root = Path(ws_str).resolve()
    target_path = Path(target_str)
    if target_path.is_absolute():
        return target_path.resolve()
    return (workspace_root / target_path).resolve()


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
    if not file_path.exists() or not file_path.is_file():
        raise FileNotFoundError(f"YAML configuration file not found: {file_path}")

    try:
        raw_text = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RunnerConfigurationError(f"Failed to read YAML file '{file_path}': {exc}") from exc

    try:
        parsed = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise RunnerConfigurationError(f"Invalid YAML syntax in '{file_path}': {exc}") from exc

    if not isinstance(parsed, dict):
        raise RunnerConfigurationError(
            f"YAML configuration '{file_path}' must be a top-level mapping, "
            f"got {type(parsed).__name__}."
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


def _resolve_maybe_relative_path(workspace: str | Path, raw_value: Any) -> Any:
    """Resolve a string path against ``workspace`` if it is not a context reference."""
    if not isinstance(raw_value, str):
        return raw_value
    stripped = raw_value.strip()
    if not stripped or "${" in stripped:
        return raw_value
    return str(resolve_workspace_path(workspace, stripped))


def resolve_step_paths(
    step_parameters: Mapping[str, Any],
    workspace: str | Path,
) -> dict[str, Any]:
    """Resolve relative data paths within a step's ``parameters`` against ``workspace``."""
    params = copy.deepcopy(dict(step_parameters))

    # 1. source.config.path (atlas-ingestion LocalFileConnector) or source.path (atlas-processing)
    raw_source = params.get("source")
    if isinstance(raw_source, dict):
        if "path" in raw_source:
            raw_source["path"] = _resolve_maybe_relative_path(workspace, raw_source["path"])
        raw_cfg = raw_source.get("config")
        if isinstance(raw_cfg, dict) and "path" in raw_cfg:
            raw_cfg["path"] = _resolve_maybe_relative_path(workspace, raw_cfg["path"])

    # 2. destination.path (atlas-ingestion / atlas-processing)
    raw_dest = params.get("destination")
    if isinstance(raw_dest, dict) and "path" in raw_dest:
        raw_dest["path"] = _resolve_maybe_relative_path(workspace, raw_dest["path"])

    # 3. storage_location.path (atlas-quality)
    raw_loc = params.get("storage_location")
    if isinstance(raw_loc, dict) and "path" in raw_loc:
        raw_loc["path"] = _resolve_maybe_relative_path(workspace, raw_loc["path"])

    # 4. artifact.path (atlas-quality)
    raw_artifact = params.get("artifact")
    if isinstance(raw_artifact, dict) and "path" in raw_artifact:
        raw_artifact["path"] = _resolve_maybe_relative_path(workspace, raw_artifact["path"])

    # 5. Flat path keys if used directly
    for flat_key in ("input_path", "output_path", "path"):
        if flat_key in params:
            params[flat_key] = _resolve_maybe_relative_path(workspace, params[flat_key])

    return params


def apply_job_parameters_to_workflow_data(
    workflow_data: Mapping[str, Any],
    job_parameters: Mapping[str, Any] | None,
    *,
    workspace: str | Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Apply job-level parameter overrides and optional workspace path resolution.

    Returns a tuple of ``(prepared_workflow_dict, merged_runtime_parameters)``.
    """
    wf_copy: dict[str, Any] = copy.deepcopy(dict(workflow_data))

    raw_wf_params = wf_copy.pop("parameters", None)
    if raw_wf_params is not None and not isinstance(raw_wf_params, dict):
        raise RunnerConfigurationError(
            "Workflow top-level 'parameters' must be a mapping, "
            f"got {type(raw_wf_params).__name__}."
        )

    merged_runtime_params = merge_parameters(raw_wf_params, job_parameters)

    raw_steps = wf_copy.get("steps")
    if isinstance(raw_steps, list):
        updated_steps: list[Any] = []
        for step in raw_steps:
            if not isinstance(step, dict):
                updated_steps.append(step)
                continue

            step_copy = copy.deepcopy(step)
            raw_step_params = step_copy.get("parameters")
            if isinstance(raw_step_params, dict) or raw_step_params is None:
                step_params: dict[str, Any] = copy.deepcopy(raw_step_params or {})
                step_id = step_copy.get("step_id")

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
                    elif key == "dataset_id":
                        step_params[key] = copy.deepcopy(value)

                if workspace is not None:
                    step_params = resolve_step_paths(step_params, workspace)

                step_copy["parameters"] = step_params

            updated_steps.append(step_copy)
        wf_copy["steps"] = updated_steps

    return wf_copy, merged_runtime_params


def load_job_definition(
    workspace: str | Path,
    job_path: str | Path,
) -> tuple[JobDefinition, Path]:
    """Load and validate a :class:`JobDefinition` from ``job_path`` relative to ``workspace``."""
    resolved_job_path = resolve_workspace_path(workspace, job_path)
    raw_job = load_yaml_file(resolved_job_path)
    try:
        job_def = JobDefinition.model_validate(raw_job)
    except ValidationError:
        raise
    except Exception as exc:
        raise RunnerConfigurationError(
            f"Invalid job configuration in '{resolved_job_path}': {exc}"
        ) from exc
    return job_def, resolved_job_path


def load_workflow_definition(
    workspace: str | Path,
    workflow_ref: str | Path,
    *,
    job_parameters: Mapping[str, Any] | None = None,
) -> tuple[WorkflowDefinition, dict[str, Any], Path]:
    """Load, merge parameters, resolve paths, and construct a :class:`WorkflowDefinition`."""
    resolved_wf_path = resolve_workspace_path(workspace, workflow_ref)
    raw_workflow = load_yaml_file(resolved_wf_path)
    prepared_data, merged_params = apply_job_parameters_to_workflow_data(
        raw_workflow,
        job_parameters,
        workspace=workspace,
    )
    workflow_def = WorkflowDefinition.model_validate(prepared_data)
    return workflow_def, merged_params, resolved_wf_path


__all__ = [
    "apply_job_parameters_to_workflow_data",
    "load_job_definition",
    "load_workflow_definition",
    "load_yaml_file",
    "merge_parameters",
    "resolve_step_paths",
    "resolve_workspace_path",
]
