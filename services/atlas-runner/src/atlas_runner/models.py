"""Configuration and validation models for atlas-runner."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from atlas_core import require_not_empty
from atlas_orchestration import WorkflowDefinition
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class JobDefinition(BaseModel):
    """Job execution manifest referencing one or more workflow definitions."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    job_id: str
    name: str | None = None
    description: str | None = None
    workflows: list[str] = Field(default_factory=list)
    workflow_ref: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("parameters", "metadata", mode="before")
    @classmethod
    def _coerce_empty_dict(cls, value: Any) -> Any:
        if value is None:
            return {}
        return value

    @model_validator(mode="before")
    @classmethod
    def _validate_workflow_references_input(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        has_workflows = "workflows" in data and data["workflows"] is not None
        has_wf_ref = "workflow_ref" in data and data["workflow_ref"] is not None
        if not has_workflows and not has_wf_ref:
            raise ValueError(
                "Job definition requires either 'workflows' (non-empty list) or 'workflow_ref'."
            )
        if has_workflows and has_wf_ref:
            raw_wfs = data["workflows"]
            raw_ref = data["workflow_ref"]
            if not (
                isinstance(raw_wfs, list)
                and len(raw_wfs) == 1
                and isinstance(raw_wfs[0], str)
                and isinstance(raw_ref, str)
                and raw_wfs[0].strip() == raw_ref.strip()
            ):
                raise ValueError(
                    "Job definition cannot specify conflicting 'workflows' and 'workflow_ref'."
                )
        return data

    @model_validator(mode="after")
    def _validate_fields(self) -> JobDefinition:
        clean_job_id = self.job_id.strip() if isinstance(self.job_id, str) else ""
        require_not_empty(clean_job_id, "job_id")

        cleaned_workflows: list[str] = []
        if self.workflows:
            for idx, item in enumerate(self.workflows):
                clean_item = item.strip() if isinstance(item, str) else ""
                require_not_empty(clean_item, f"workflows[{idx}]")
                cleaned_workflows.append(clean_item)
        elif self.workflow_ref is not None:
            clean_wf_ref = self.workflow_ref.strip() if isinstance(self.workflow_ref, str) else ""
            require_not_empty(clean_wf_ref, "workflow_ref")
            cleaned_workflows.append(clean_wf_ref)

        if not cleaned_workflows:
            raise ValueError("Job 'workflows' list must contain at least one workflow reference.")

        clean_name = self.name.strip() if isinstance(self.name, str) and self.name.strip() else None
        clean_desc = (
            self.description.strip()
            if isinstance(self.description, str) and self.description.strip()
            else None
        )

        object.__setattr__(self, "job_id", clean_job_id)
        object.__setattr__(self, "name", clean_name)
        object.__setattr__(self, "description", clean_desc)
        object.__setattr__(self, "workflows", cleaned_workflows)
        object.__setattr__(self, "workflow_ref", cleaned_workflows[0])
        object.__setattr__(self, "parameters", copy.deepcopy(self.parameters))
        object.__setattr__(self, "metadata", copy.deepcopy(self.metadata))
        return self


class ValidationReport(BaseModel):
    """Result of dry-run job and workflow validation (--validate-only)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    is_valid: bool = True
    job_id: str
    job_file: Path
    workflow_id: str
    workflow_version: int
    workflow_file: Path
    topological_order: tuple[str, ...]
    workflow: WorkflowDefinition
    workflows: list[WorkflowDefinition] = Field(default_factory=list)
    workflow_files: list[Path] = Field(default_factory=list)
    topological_orders: dict[str, tuple[str, ...]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _populate_batch_defaults(self) -> ValidationReport:
        if not self.workflows:
            object.__setattr__(self, "workflows", [self.workflow])
        if not self.workflow_files:
            object.__setattr__(self, "workflow_files", [self.workflow_file])
        if not self.topological_orders:
            object.__setattr__(
                self,
                "topological_orders",
                {self.workflow_id: self.topological_order},
            )
        return self


JobManifest = JobDefinition

__all__ = [
    "JobDefinition",
    "JobManifest",
    "ValidationReport",
]
