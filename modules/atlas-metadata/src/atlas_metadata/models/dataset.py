"""Dataset metadata domain model."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any

from atlas_core import BRONZE, Dataset, require_not_empty
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_metadata.exceptions import InvalidMetadataError, InvalidURNError
from atlas_metadata.identifiers.urn import URN


def validate_json_value(value: Any, *, path: str = "custom_properties") -> Any:
    """Recursively validate that ``value`` is strictly JSON-compatible."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise InvalidMetadataError(
                f"Non-finite float value at '{path}' is not JSON-compatible."
            )
        return value
    if isinstance(value, list):
        return [validate_json_value(item, path=f"{path}[{idx}]") for idx, item in enumerate(value)]
    if isinstance(value, dict):
        validated_dict: dict[str, Any] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise InvalidMetadataError(
                    f"Dictionary key {k!r} at '{path}' must be a string for JSON compatibility."
                )
            validated_dict[k] = validate_json_value(v, path=f"{path}.{k}")
        return validated_dict
    raise InvalidMetadataError(
        f"Value of type '{type(value).__name__}' at '{path}' is not JSON-compatible."
    )


def normalize_tags(tags: list[str]) -> list[str]:
    """Validate, strip, and deduplicate tags while preserving order."""
    seen: set[str] = set()
    normalized: list[str] = []
    for raw in tags:
        if not isinstance(raw, str) or not raw.strip():
            raise InvalidMetadataError("Tags must be non-empty strings.")
        cleaned = raw.strip()
        if cleaned not in seen:
            seen.add(cleaned)
            normalized.append(cleaned)
    return normalized


class DatasetMetadata(BaseModel):
    """Domain metadata record for a registered dataset in the Atlas catalog."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    dataset_id: str
    urn: URN
    name: str
    namespace: str = "default"
    layer: str = BRONZE
    source_id: str | None = None
    format: str | None = None
    location_uri: str | None = None
    description: str | None = None
    owner: str | None = None
    tags: list[str] = Field(default_factory=list)
    custom_properties: dict[str, Any] = Field(default_factory=dict)
    latest_schema_version: int | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def __init__(self, **data: Any) -> None:
        if "dataset_id" not in data and "id" in data:
            data = dict(data)
            data["dataset_id"] = data.pop("id")
        if "location_uri" not in data and "path" in data:
            data = dict(data)
            data["location_uri"] = data.pop("path")

        raw_urn = data.get("urn")
        raw_ds_id = data.get("dataset_id")
        raw_ns = data.get("namespace", "default")

        try:
            if raw_urn is not None:
                parsed_urn = URN.parse(raw_urn) if not isinstance(raw_urn, URN) else raw_urn
                data = dict(data)
                data["urn"] = parsed_urn
                if not raw_ds_id:
                    data["dataset_id"] = parsed_urn.entity_name
                if "namespace" not in data:
                    data["namespace"] = parsed_urn.namespace
            elif raw_ds_id is not None:
                clean_id = str(raw_ds_id).strip()
                clean_ns = str(raw_ns).strip() if isinstance(raw_ns, str) else "default"
                data = dict(data)
                data["urn"] = URN.for_dataset(clean_id, namespace=clean_ns)

            if "name" not in data and data.get("dataset_id"):
                data = dict(data)
                data["name"] = str(data["dataset_id"]).strip()

            super().__init__(**data)
        except (InvalidMetadataError, InvalidURNError):
            raise
        except Exception as exc:
            raise InvalidMetadataError(f"Invalid dataset metadata: {exc}") from exc

    @model_validator(mode="after")
    def _validate_dataset_metadata(self) -> DatasetMetadata:
        try:
            require_not_empty(self.dataset_id.strip(), "dataset_id")
            require_not_empty(self.name.strip(), "name")
            require_not_empty(self.layer.strip(), "layer")
            require_not_empty(self.namespace.strip(), "namespace")
        except ValueError as exc:
            raise InvalidMetadataError(str(exc), dataset_id=self.dataset_id) from exc

        if self.urn.entity_type != "dataset":
            raise InvalidURNError(
                "DatasetMetadata URN must have entity_type 'dataset', "
                f"got '{self.urn.entity_type}'.",
                urn=str(self.urn),
                dataset_id=self.dataset_id,
            )

        object.__setattr__(self, "dataset_id", self.dataset_id.strip())
        object.__setattr__(self, "name", self.name.strip())
        object.__setattr__(self, "layer", self.layer.strip().lower())
        object.__setattr__(self, "namespace", self.namespace.strip().lower())
        object.__setattr__(self, "tags", normalize_tags(self.tags))
        object.__setattr__(
            self,
            "custom_properties",
            validate_json_value(self.custom_properties, path="custom_properties"),
        )

        if self.latest_schema_version is not None and self.latest_schema_version < 1:
            raise InvalidMetadataError(
                f"latest_schema_version must be >= 1, got {self.latest_schema_version}.",
                dataset_id=self.dataset_id,
            )
        return self

    def to_core_dataset(self) -> Dataset:
        """Convert this :class:`DatasetMetadata` to an :class:`atlas_core.Dataset`."""
        meta = dict(self.custom_properties)
        meta["urn"] = str(self.urn)
        meta["namespace"] = self.namespace
        meta["tags"] = list(self.tags)
        if self.owner is not None:
            meta["owner"] = self.owner
        if self.latest_schema_version is not None:
            meta["latest_schema_version"] = self.latest_schema_version
        return Dataset(
            id=self.dataset_id,
            name=self.name,
            layer=self.layer,
            source_id=self.source_id,
            format=self.format,
            path=self.location_uri,
            description=self.description,
            metadata=meta,
        )

    @classmethod
    def from_core_dataset(
        cls,
        dataset: Dataset,
        *,
        namespace: str = "default",
        owner: str | None = None,
        tags: list[str] | None = None,
        custom_properties: dict[str, Any] | None = None,
    ) -> DatasetMetadata:
        """Create a :class:`DatasetMetadata` instance from an :class:`atlas_core.Dataset`."""
        raw_meta = dict(dataset.metadata or {})
        resolved_ns = str(raw_meta.pop("namespace", namespace))
        resolved_owner = owner if owner is not None else raw_meta.pop("owner", None)
        raw_tags = tags if tags is not None else raw_meta.pop("tags", [])
        raw_urn = raw_meta.pop("urn", None)
        merged_props = {**raw_meta, **(custom_properties or {})}

        init_kwargs: dict[str, Any] = {
            "dataset_id": dataset.id,
            "name": dataset.name,
            "namespace": resolved_ns,
            "layer": dataset.layer,
            "source_id": dataset.source_id,
            "format": dataset.format,
            "location_uri": dataset.path,
            "description": dataset.description,
            "owner": resolved_owner,
            "tags": list(raw_tags) if isinstance(raw_tags, (list, tuple)) else [],
            "custom_properties": merged_props,
        }
        if raw_urn is not None:
            init_kwargs["urn"] = raw_urn
        return cls(**init_kwargs)


__all__ = [
    "DatasetMetadata",
    "normalize_tags",
    "validate_json_value",
]
