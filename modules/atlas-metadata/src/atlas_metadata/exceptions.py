"""Domain exception hierarchy for atlas-metadata."""

from __future__ import annotations

from atlas_core import MetadataError as CoreMetadataError


class MetadataError(CoreMetadataError):
    """Base exception for all atlas-metadata errors."""

    def __init__(
        self,
        message: str,
        *,
        urn: str | None = None,
        dataset_id: str | None = None,
        schema_id: str | None = None,
        artifact_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.urn = urn
        self.dataset_id = dataset_id
        self.schema_id = schema_id
        self.artifact_id = artifact_id


class InvalidURNError(MetadataError):
    """Raised when a metadata URN string or component is malformed or invalid."""


class InvalidMetadataError(MetadataError):
    """Raised when metadata model validation fails (e.g., non-JSON-compatible custom properties)."""


class MetadataRepositoryError(MetadataError):
    """Raised when a metadata repository operation fails."""


class DatasetNotFoundError(MetadataError):
    """Raised when a requested dataset cannot be found in the catalog."""


class DuplicateDatasetError(MetadataError):
    """Raised when attempting to register a dataset that already exists without overwrite."""


DatasetConflictError = DuplicateDatasetError


class SchemaNotFoundError(MetadataError):
    """Raised when a requested schema or schema version cannot be found."""


class SchemaConflictError(MetadataError):
    """Raised when attempting to register a duplicate (dataset_id, version) schema."""


DuplicateSchemaError = SchemaConflictError


class InvalidSchemaError(InvalidMetadataError):
    """Raised when a schema or column metadata definition is invalid."""


class ArtifactNotFoundError(MetadataError):
    """Raised when a requested artifact metadata record cannot be found."""


class DuplicateArtifactError(MetadataError):
    """Raised when attempting to register a duplicate artifact metadata record."""


ArtifactConflictError = DuplicateArtifactError


__all__ = [
    "ArtifactConflictError",
    "ArtifactNotFoundError",
    "DatasetConflictError",
    "DatasetNotFoundError",
    "DuplicateArtifactError",
    "DuplicateDatasetError",
    "DuplicateSchemaError",
    "InvalidMetadataError",
    "InvalidSchemaError",
    "InvalidURNError",
    "MetadataError",
    "MetadataRepositoryError",
    "SchemaConflictError",
    "SchemaNotFoundError",
]
