"""Standalone workspace resolution and configuration for atlas-scheduler."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from atlas_scheduler.exceptions import (
    SchedulerConfigurationError,
    WorkspaceIsolationError,
)


@dataclass(frozen=True)
class WorkspaceContext:
    """Scheduler-specific lightweight workspace context.

    For logical workspace IDs (managed workspace mode), ``workspace_id`` holds
    the logical identifier (e.g. ``"customer_001"``) and ``workspace_path``
    holds ``$ATLAS_WORKSPACE/customer_001``.

    For explicit absolute workspace paths, ``workspace_id`` preserves the
    resolved absolute path string so runner subprocess dispatch uses the
    absolute-path contract.
    """

    workspace_id: str
    workspace_path: Path


def resolve_workspace(workspace: str | Path) -> WorkspaceContext:
    """Resolve ``workspace`` (logical ID or absolute path) into a :class:`WorkspaceContext`.

    Raises
    ------
    SchedulerConfigurationError
        If ``workspace`` is empty, ``ATLAS_WORKSPACE`` is unset for a logical ID,
        or the resolved workspace directory does not exist.
    WorkspaceIsolationError
        If ``workspace`` contains path traversal segments or escapes ``ATLAS_WORKSPACE``.
    """
    ws_str = str(workspace).strip() if workspace is not None else ""
    if not ws_str:
        raise SchedulerConfigurationError("Workspace identifier or path must not be empty.")

    raw_path = Path(ws_str)

    # Explicitly reject any relative parent traversal segments (e.g. "../../etc")
    if ".." in raw_path.parts:
        raise WorkspaceIsolationError(
            f"Workspace isolation violation: path traversal segment '..' is not allowed "
            f"in workspace '{ws_str}'.",
            offending_path=ws_str,
        )

    if raw_path.is_absolute():
        resolved_path = raw_path.resolve()
        if not resolved_path.exists():
            raise SchedulerConfigurationError(
                f"Workspace directory does not exist: '{resolved_path}'."
            )
        if not resolved_path.is_dir():
            raise SchedulerConfigurationError(
                f"Workspace path is not a directory: '{resolved_path}'."
            )
        return WorkspaceContext(
            workspace_id=str(resolved_path),
            workspace_path=resolved_path,
        )

    # Logical workspace ID mode — requires ATLAS_WORKSPACE environment variable
    atlas_ws_env = os.environ.get("ATLAS_WORKSPACE", "").strip()
    if not atlas_ws_env:
        raise SchedulerConfigurationError(
            f"Cannot resolve logical workspace '{ws_str}': environment variable "
            "'ATLAS_WORKSPACE' must be set when using a logical workspace ID "
            "(or provide an absolute --workspace path)."
        )

    base_root = Path(atlas_ws_env).expanduser().resolve()
    if "/" in ws_str or "\\" in ws_str:
        raise WorkspaceIsolationError(
            f"Workspace isolation violation: logical workspace ID '{ws_str}' must be a "
            f"direct workspace identifier under ATLAS_WORKSPACE ('{base_root}').",
            offending_path=ws_str,
            workspace_root=str(base_root),
        )

    candidate = (base_root / raw_path).resolve()
    if not candidate.is_relative_to(base_root) or candidate == base_root:
        raise WorkspaceIsolationError(
            f"Workspace isolation violation: logical workspace '{ws_str}' resolves to "
            f"'{candidate}', which escapes ATLAS_WORKSPACE boundary '{base_root}'.",
            offending_path=ws_str,
            workspace_root=str(base_root),
        )

    if not candidate.exists():
        raise SchedulerConfigurationError(
            f"Workspace directory for '{ws_str}' does not exist at '{candidate}'."
        )
    if not candidate.is_dir():
        raise SchedulerConfigurationError(
            f"Resolved workspace path for '{ws_str}' is not a directory: '{candidate}'."
        )

    return WorkspaceContext(
        workspace_id=ws_str,
        workspace_path=candidate,
    )


__all__ = [
    "WorkspaceContext",
    "resolve_workspace",
]
