"""Dataset domain model."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator

from atlas_core.constants import BRONZE
from atlas_core.identifiers import DEFAULT_NAMESPACE, build_dataset_urn


class Dataset(BaseModel):
    """Represents a registered dataset within the Atlas Platform."""

    id: str
    name: str
    layer: str = BRONZE
    urn: str | None = None
    namespace: str = DEFAULT_NAMESPACE
    source_id: str | None = None
    format: str | None = None
    path: str | None = None
    description: str | None = None
    owner: str | None = None
    tags: list[str] = Field(default_factory=list)
    upstream_urns: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _sync_metadata_fields(self) -> Dataset:
        meta = dict(self.metadata or {})

        if "namespace" in meta and self.namespace == DEFAULT_NAMESPACE:
            ns_val = str(meta["namespace"]).strip().lower()
            if ns_val:
                self.namespace = ns_val
        else:
            self.namespace = (
                self.namespace or DEFAULT_NAMESPACE
            ).strip().lower() or DEFAULT_NAMESPACE

        if self.urn is None:
            raw_urn = meta.get("urn")
            if isinstance(raw_urn, str) and raw_urn.strip():
                self.urn = raw_urn.strip()
            elif self.id and self.id.strip():
                self.urn = build_dataset_urn(self.id.strip(), namespace=self.namespace)

        if not self.tags and isinstance(meta.get("tags"), (list, tuple)):
            self.tags = list(dict.fromkeys(str(t).strip() for t in meta["tags"] if str(t).strip()))
        elif self.tags:
            self.tags = list(dict.fromkeys(str(t).strip() for t in self.tags if str(t).strip()))

        if not self.upstream_urns and isinstance(meta.get("upstream_urns"), (list, tuple)):
            self.upstream_urns = list(
                dict.fromkeys(str(u).strip() for u in meta["upstream_urns"] if str(u).strip())
            )
        elif self.upstream_urns:
            self.upstream_urns = list(
                dict.fromkeys(str(u).strip() for u in self.upstream_urns if str(u).strip())
            )

        if self.owner is None and isinstance(meta.get("owner"), str):
            self.owner = meta["owner"]

        return self

    @property
    def dataset_id(self) -> str:
        """Return the dataset identifier (alias for ``id``)."""
        return self.id

    @property
    def location_uri(self) -> str | None:
        """Return the dataset storage path/URI (alias for ``path``)."""
        return self.path


__all__ = ["Dataset"]
