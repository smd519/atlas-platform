"""Public package interface for atlas-metadata (Phase 1: Domain Foundation)."""

from atlas_metadata.catalog import DatasetCatalog
from atlas_metadata.exceptions import (
    ArtifactConflictError,
    ArtifactNotFoundError,
    DatasetConflictError,
    DatasetNotFoundError,
    DuplicateArtifactError,
    DuplicateDatasetError,
    DuplicateSchemaError,
    InvalidMetadataError,
    InvalidSchemaError,
    InvalidURNError,
    MetadataError,
    MetadataRepositoryError,
    SchemaConflictError,
    SchemaNotFoundError,
)
from atlas_metadata.identifiers import URN, URN_PREFIX
from atlas_metadata.models import (
    ArtifactMetadata,
    ColumnMetadata,
    DatasetMetadata,
    SchemaMetadata,
    normalize_tags,
    validate_json_value,
)
from atlas_metadata.repositories import (
    ArtifactRepository,
    DatasetRepository,
    InMemoryMetadataRepository,
    MetadataRepository,
    SchemaRepository,
)
from atlas_metadata.schema import SchemaRegistry
from atlas_metadata.service import MetadataService

__all__ = [
    "ArtifactConflictError",
    "ArtifactMetadata",
    "ArtifactNotFoundError",
    "ArtifactRepository",
    "ColumnMetadata",
    "DatasetCatalog",
    "DatasetConflictError",
    "DatasetMetadata",
    "DatasetNotFoundError",
    "DatasetRepository",
    "DuplicateArtifactError",
    "DuplicateDatasetError",
    "DuplicateSchemaError",
    "InMemoryMetadataRepository",
    "InvalidMetadataError",
    "InvalidSchemaError",
    "InvalidURNError",
    "MetadataError",
    "MetadataRepository",
    "MetadataRepositoryError",
    "MetadataService",
    "SchemaConflictError",
    "SchemaMetadata",
    "SchemaNotFoundError",
    "SchemaRegistry",
    "SchemaRepository",
    "URN",
    "URN_PREFIX",
    "normalize_tags",
    "validate_json_value",
]
