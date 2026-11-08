"""Configuration models for atlas-runner."""

from __future__ import annotations

import copy
from typing import Any

from atlas_core import require_not_empty
from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class JobDefinition(BaseModel):
    """Lightweight job execution manifest referencing a workflow definition."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    job_id: str
    workflow_ref: str
    parameters: dict[str, Any]

    @field_validator("parameters", mode="before")
    @classmethod
    def _coerce_empty_parameters(cls, value: Any) -> Any:
        if value is None:
            return {}
        return value

    @model_validator(mode="after")
    def _validate_fields(self) -> JobDefinition:
        clean_job_id = self.job_id.strip() if isinstance(self.job_id, str) else ""
        clean_wf_ref = self.workflow_ref.strip() if isinstance(self.workflow_ref, str) else ""
        require_not_empty(clean_job_id, "job_id")
        require_not_empty(clean_wf_ref, "workflow_ref")

        object.__setattr__(self, "job_id", clean_job_id)
        object.__setattr__(self, "workflow_ref", clean_wf_ref)
        object.__setattr__(self, "parameters", copy.deepcopy(self.parameters))
        return self


JobManifest = JobDefinition

__all__ = [
    "JobDefinition",
    "JobManifest",
]
