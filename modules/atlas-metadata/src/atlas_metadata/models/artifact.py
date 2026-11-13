"""Artifact metadata domain model."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from atlas_core import Artifact, generate_execution_id, require_not_empty
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_metadata.exceptions import InvalidMetadataError, InvalidURNError
from atlas_metadata.identifiers.urn import URN
from atlas_metadata.models.dataset import normalize_tags, validate_json_value


class ArtifactMetadata(BaseModel):
    """Metadata record describing a physical dataset artifact."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    artifact_id: str
    urn: URN
    dataset_id: str
    namespace: str = "default"
    uri: str
    format: str
    layer: str | None = None
    execution_id: str | None = None
    schema_version: int | None = None
    row_count: int | None = None
    byte_size: int | None = None
    checksum: str | None = None
    tags: list[str] = Field(default_factory=list)
    custom_properties: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def __init__(self, **data: Any) -> None:
        if "artifact_id" not in data and "id" in data:
            data = dict(data)
            data["artifact_id"] = data.pop("id")
        if "uri" not in data and "path" in data:
            data = dict(data)
            data["uri"] = data.pop("path")
        if "artifact_id" not in data and "urn" not in data:
            data = dict(data)
            data["artifact_id"] = generate_execution_id()

        raw_urn = data.get("urn")
        raw_art_id = data.get("artifact_id")
        raw_ns = data.get("namespace", "default")

        try:
            if raw_urn is not None:
                parsed_urn = URN.parse(raw_urn) if not isinstance(raw_urn, URN) else raw_urn
                data = dict(data)
                data["urn"] = parsed_urn
                if not raw_art_id:
                    data["artifact_id"] = parsed_urn.entity_name
                if "namespace" not in data:
                    data["namespace"] = parsed_urn.namespace
            elif raw_art_id is not None:
                clean_id = str(raw_art_id).strip()
                clean_ns = str(raw_ns).strip() if isinstance(raw_ns, str) else "default"
                data = dict(data)
                data["urn"] = URN.for_artifact(clean_id, namespace=clean_ns)

            super().__init__(**data)
        except (InvalidMetadataError, InvalidURNError):
            raise
        except Exception as exc:
            raise InvalidMetadataError(f"Invalid artifact metadata: {exc}") from exc

    @model_validator(mode="after")
    def _validate_artifact(self) -> ArtifactMetadata:
        clean_id = self.artifact_id.strip() if isinstance(self.artifact_id, str) else ""
        clean_ds = self.dataset_id.strip() if isinstance(self.dataset_id, str) else ""
        clean_uri = self.uri.strip() if isinstance(self.uri, str) else ""
        clean_fmt = self.format.strip().lower() if isinstance(self.format, str) else ""
        clean_ns = self.namespace.strip().lower() if isinstance(self.namespace, str) else ""

        try:
            require_not_empty(clean_id, "artifact_id")
            require_not_empty(clean_ds, "dataset_id")
            require_not_empty(clean_uri, "uri")
            require_not_empty(clean_fmt, "format")
            require_not_empty(clean_ns, "namespace")
        except ValueError as exc:
            raise InvalidMetadataError(str(exc), dataset_id=clean_ds, artifact_id=clean_id) from exc

        if self.urn.entity_type != "artifact":
            raise InvalidURNError(
                "ArtifactMetadata URN must have entity_type 'artifact', "
                f"got '{self.urn.entity_type}'.",
                urn=str(self.urn),
                dataset_id=clean_ds,
                artifact_id=clean_id,
            )

        if self.execution_id is not None:
            clean_exec = self.execution_id.strip() if isinstance(self.execution_id, str) else ""
            try:
                require_not_empty(clean_exec, "execution_id")
            except ValueError as exc:
                raise InvalidMetadataError(
                    str(exc),
                    dataset_id=clean_ds,
                    artifact_id=clean_id,
                ) from exc
            object.__setattr__(self, "execution_id", clean_exec)

        if self.schema_version is not None and self.schema_version < 1:
            raise InvalidMetadataError(
                f"schema_version must be >= 1, got {self.schema_version}.",
                dataset_id=clean_ds,
                artifact_id=clean_id,
            )
        if self.row_count is not None and self.row_count < 0:
            raise InvalidMetadataError(
                f"row_count must be >= 0, got {self.row_count}.",
                dataset_id=clean_ds,
                artifact_id=clean_id,
            )
        if self.byte_size is not None and self.byte_size < 0:
            raise InvalidMetadataError(
                f"byte_size must be >= 0, got {self.byte_size}.",
                dataset_id=clean_ds,
                artifact_id=clean_id,
            )

        object.__setattr__(self, "artifact_id", clean_id)
        object.__setattr__(self, "dataset_id", clean_ds)
        object.__setattr__(self, "uri", clean_uri)
        object.__setattr__(self, "format", clean_fmt)
        object.__setattr__(self, "namespace", clean_ns)
        object.__setattr__(self, "tags", normalize_tags(self.tags))
        object.__setattr__(
            self,
            "custom_properties",
            validate_json_value(self.custom_properties, path="custom_properties"),
        )
        return self

    def to_core_artifact(self) -> Artifact:
        """Convert this :class:`ArtifactMetadata` to an :class:`atlas_core.Artifact`."""
        return Artifact(
            id=self.artifact_id,
            dataset_id=self.dataset_id,
            path=self.uri,
            format=self.format,
            urn=str(self.urn),
            namespace=self.namespace,
            layer=self.layer,
            execution_id=self.execution_id,
            row_count=self.row_count,
            byte_size=self.byte_size,
            checksum=self.checksum,
            tags=list(self.tags),
            metadata=dict(self.custom_properties),
        )

    @classmethod
    def from_core_artifact(
        cls,
        artifact: Artifact,
        *,
        namespace: str | None = None,
        layer: str | None = None,
        execution_id: str | None = None,
        schema_version: int | None = None,
        row_count: int | None = None,
        byte_size: int | None = None,
        checksum: str | None = None,
        tags: list[str] | None = None,
        custom_properties: dict[str, Any] | None = None,
    ) -> ArtifactMetadata:
        """Create an :class:`ArtifactMetadata` record from an :class:`atlas_core.Artifact`."""
        resolved_ns = (
            namespace
            if namespace is not None
            else getattr(artifact, "namespace", None) or "default"
        )
        resolved_layer = layer if layer is not None else getattr(artifact, "layer", None)
        resolved_exec_id = (
            execution_id if execution_id is not None else getattr(artifact, "execution_id", None)
        )
        resolved_rows = row_count if row_count is not None else getattr(artifact, "row_count", None)
        resolved_bytes = (
            byte_size if byte_size is not None else getattr(artifact, "byte_size", None)
        )
        resolved_checksum = (
            checksum if checksum is not None else getattr(artifact, "checksum", None)
        )
        resolved_tags = (
            list(tags) if tags is not None else list(getattr(artifact, "tags", None) or [])
        )
        raw_meta = dict(getattr(artifact, "metadata", None) or {})
        merged_props = {**raw_meta, **(custom_properties or {})}
        return cls(
            artifact_id=artifact.id,
            dataset_id=artifact.dataset_id,
            namespace=resolved_ns,
            uri=artifact.path,
            format=artifact.format,
            layer=resolved_layer,
            execution_id=resolved_exec_id,
            schema_version=schema_version,
            row_count=resolved_rows,
            byte_size=resolved_bytes,
            checksum=resolved_checksum,
            tags=resolved_tags,
            custom_properties=merged_props,
        )


__all__ = ["ArtifactMetadata"]
