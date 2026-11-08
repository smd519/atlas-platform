"""Workspace execution context domain model for atlas-orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from atlas_core import require_not_empty
from pydantic import BaseModel, ConfigDict, model_validator

from atlas_orchestration.exceptions import InvalidWorkflowStateError


class WorkspaceContext(BaseModel):
    """Represents the validated workspace context (logical ID and physical path)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    workspace_id: str = ""
    root_path: Path

    def __init__(
        self,
        root_path: str | Path | None = None,
        *,
        workspace_id: str | None = None,
        workspace_path: str | Path | None = None,
        **data: Any,
    ) -> None:
        resolved_input = root_path if root_path is not None else workspace_path
        if resolved_input is not None and "root_path" not in data:
            data["root_path"] = resolved_input
        if workspace_id is not None and "workspace_id" not in data:
            data["workspace_id"] = workspace_id
        try:
            super().__init__(**data)
        except InvalidWorkflowStateError:
            raise
        except Exception as exc:
            raise InvalidWorkflowStateError(f"Invalid WorkspaceContext: {exc}") from exc

    @model_validator(mode="before")
    @classmethod
    def _validate_raw_path(cls, data: Any) -> Any:
        if isinstance(data, (str, Path)):
            return {"root_path": data}
        if isinstance(data, dict):
            raw = data.get("root_path", data.get("workspace_path"))
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                raise InvalidWorkflowStateError("Workspace root_path must not be empty.")
            clean_str = str(raw).strip()
            try:
                require_not_empty(clean_str, "root_path")
            except ValueError as exc:
                raise InvalidWorkflowStateError(str(exc)) from exc
            resolved_path = Path(clean_str).resolve()
            raw_ws_id = data.get("workspace_id")
            if raw_ws_id is not None:
                clean_ws_id = str(raw_ws_id).strip()
                if not clean_ws_id:
                    raise InvalidWorkflowStateError("Workspace workspace_id must not be empty.")
            else:
                clean_ws_id = resolved_path.name or str(resolved_path)
            copied = dict(data)
            copied.pop("workspace_path", None)
            copied["root_path"] = resolved_path
            copied["workspace_id"] = clean_ws_id
            return copied
        return data

    @model_validator(mode="after")
    def _normalize_root_path(self) -> WorkspaceContext:
        raw_str = str(self.root_path).strip()
        if not raw_str:
            raise InvalidWorkflowStateError("Workspace root_path must not be empty.")
        resolved_path = Path(raw_str).resolve()
        clean_ws_id = (
            self.workspace_id.strip()
            if isinstance(self.workspace_id, str) and self.workspace_id.strip()
            else (resolved_path.name or str(resolved_path))
        )
        object.__setattr__(self, "root_path", resolved_path)
        object.__setattr__(self, "workspace_id", clean_ws_id)
        return self

    @property
    def workspace_path(self) -> Path:
        """Return the physical resolved filesystem path of the workspace."""
        return self.root_path

    @property
    def path(self) -> Path:
        """Alias for :attr:`root_path`."""
        return self.root_path


__all__ = ["WorkspaceContext"]
