"""Repository contracts for dataset, schema, and artifact metadata persistence."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from atlas_metadata.identifiers.urn import URN
from atlas_metadata.models.artifact import ArtifactMetadata
from atlas_metadata.models.dataset import DatasetMetadata
from atlas_metadata.models.schema import ColumnMetadata, SchemaMetadata


class DatasetRepository(ABC):
    """Abstract repository contract for dataset catalog records."""

    @abstractmethod
    def save_dataset(
        self,
        dataset: DatasetMetadata,
        *,
        overwrite: bool = False,
    ) -> DatasetMetadata:
        """Persist ``dataset`` or raise :class:`DuplicateDatasetError` if ``overwrite=False``."""

    @abstractmethod
    def get_dataset(self, dataset_id: str) -> DatasetMetadata | None:
        """Retrieve a :class:`DatasetMetadata` by ``dataset_id``, or ``None`` if not found."""

    @abstractmethod
    def get_dataset_by_urn(self, urn: URN | str) -> DatasetMetadata | None:
        """Retrieve a :class:`DatasetMetadata` by its :class:`URN`, or ``None`` if not found."""

    @abstractmethod
    def list_datasets(
        self,
        *,
        layer: str | None = None,
        namespace: str | None = None,
        tag: str | None = None,
        owner: str | None = None,
    ) -> list[DatasetMetadata]:
        """List registered datasets with optional filtering."""

    @abstractmethod
    def delete_dataset(self, dataset_id: str) -> bool:
        """Delete a dataset by ``dataset_id`` and return True if it existed."""


class SchemaRepository(ABC):
    """Abstract repository contract for immutable versioned dataset schemas."""

    @abstractmethod
    def save_schema(self, schema: SchemaMetadata) -> SchemaMetadata:
        """Atomically persist ``schema`` or raise :class:`SchemaConflictError` on conflict."""

    @abstractmethod
    def register_next_schema(
        self,
        dataset_id: str,
        columns: Sequence[ColumnMetadata],
        *,
        namespace: str = "default",
        schema_format: str = "tabular",
        description: str | None = None,
        tags: list[str] | None = None,
        custom_properties: dict[str, object] | None = None,
    ) -> SchemaMetadata:
        """Atomically allocate the next version number for ``dataset_id`` and persist the schema."""

    @abstractmethod
    def get_schema(
        self,
        dataset_id: str,
        version: int | None = None,
    ) -> SchemaMetadata | None:
        """Return ``version`` (or latest when ``version=None``) for ``dataset_id``."""

    @abstractmethod
    def get_schema_by_urn(self, urn: URN | str) -> SchemaMetadata | None:
        """Return a :class:`SchemaMetadata` by its :class:`URN`, or ``None`` if not found."""

    @abstractmethod
    def list_schemas(self, dataset_id: str) -> list[SchemaMetadata]:
        """Return all registered schema versions for ``dataset_id`` ordered by version ascending."""


class ArtifactRepository(ABC):
    """Abstract repository contract for dataset artifact metadata."""

    @abstractmethod
    def save_artifact(
        self,
        artifact: ArtifactMetadata,
        *,
        overwrite: bool = False,
    ) -> ArtifactMetadata:
        """Persist ``artifact`` or raise :class:`DuplicateArtifactError` if ``overwrite=False``."""

    @abstractmethod
    def get_artifact(self, artifact_id: str) -> ArtifactMetadata | None:
        """Retrieve an :class:`ArtifactMetadata` by ``artifact_id``, or ``None`` if not found."""

    @abstractmethod
    def get_artifact_by_urn(self, urn: URN | str) -> ArtifactMetadata | None:
        """Retrieve an :class:`ArtifactMetadata` by its :class:`URN`, or ``None`` if not found."""

    @abstractmethod
    def list_artifacts(
        self,
        *,
        dataset_id: str | None = None,
        execution_id: str | None = None,
    ) -> list[ArtifactMetadata]:
        """List registered artifacts with optional ``dataset_id`` or ``execution_id`` filter."""


class MetadataRepository(DatasetRepository, SchemaRepository, ArtifactRepository, ABC):
    """Unified repository contract combining dataset, schema, and artifact persistence."""


__all__ = [
    "ArtifactRepository",
    "DatasetRepository",
    "MetadataRepository",
    "SchemaRepository",
]
