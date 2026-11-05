"""Dataset domain model."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from atlas_core.constants import BRONZE


class Dataset(BaseModel):
    """Represents a registered dataset within the Atlas Platform."""

    id: str
    name: str
    layer: str = BRONZE
    source_id: str | None = None
    format: str | None = None
    path: str | None = None
    description: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


__all__ = ["Dataset"]
