"""Artifact domain model."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator

from atlas_core.identifiers import DEFAULT_NAMESPACE, build_artifact_urn


class Artifact(BaseModel):
    """Represents a physical dataset artifact within the Atlas Platform."""

    id: str
    dataset_id: str
    path: str
    format: str
    urn: str | None = None
    namespace: str = DEFAULT_NAMESPACE
    layer: str | None = None
    execution_id: str | None = None
    row_count: int | None = None
    byte_size: int | None = None
    checksum: str | None = None
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _sync_artifact_urn(self) -> Artifact:
        self.namespace = (self.namespace or DEFAULT_NAMESPACE).strip().lower() or DEFAULT_NAMESPACE
        if self.urn is None and self.id and self.id.strip():
            self.urn = build_artifact_urn(self.id.strip(), namespace=self.namespace)
        if self.tags:
            self.tags = list(dict.fromkeys(str(t).strip() for t in self.tags if str(t).strip()))
        return self

    @property
    def artifact_id(self) -> str:
        """Return the artifact identifier (alias for ``id``)."""
        return self.id

    @property
    def uri(self) -> str:
        """Return the physical artifact URI/path (alias for ``path``)."""
        return self.path


__all__ = ["Artifact"]
