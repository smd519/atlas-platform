"""Dataset catalog managing dataset registration, discovery, and artifact linkage."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from atlas_core import Artifact, Dataset

from atlas_metadata.exceptions import ArtifactNotFoundError, DatasetNotFoundError
from atlas_metadata.identifiers.urn import URN
from atlas_metadata.models.artifact import ArtifactMetadata
from atlas_metadata.models.dataset import (
    DatasetMetadata,
    normalize_tags,
    validate_json_value,
)
from atlas_metadata.repositories.contracts import MetadataRepository
from atlas_metadata.repositories.memory import InMemoryMetadataRepository


class DatasetCatalog:
    """Domain catalog for registering, discovering, and updating dataset metadata."""

    def __init__(self, repository: MetadataRepository | None = None) -> None:
        self._repository = repository or InMemoryMetadataRepository()

    @property
    def repository(self) -> MetadataRepository:
        """Return the underlying :class:`MetadataRepository`."""
        return self._repository

    def register_dataset(
        self,
        dataset: DatasetMetadata | Dataset,
        *,
        overwrite: bool = False,
    ) -> DatasetMetadata:
        """Register a :class:`DatasetMetadata` (or :class:`atlas_core.Dataset`) in the catalog."""
        meta = (
            dataset
            if isinstance(dataset, DatasetMetadata)
            else DatasetMetadata.from_core_dataset(dataset)
        )
        return self._repository.save_dataset(meta, overwrite=overwrite)

    def get_dataset(
        self,
        dataset_id: str,
        *,
        raise_if_missing: bool = False,
    ) -> DatasetMetadata | None:
        """Retrieve a :class:`DatasetMetadata` by ``dataset_id``."""
        record = self._repository.get_dataset(dataset_id)
        if record is None and raise_if_missing:
            raise DatasetNotFoundError(
                f"Dataset '{dataset_id}' not found in catalog.",
                dataset_id=dataset_id,
            )
        return record

    def require_dataset(self, dataset_id: str) -> DatasetMetadata:
        """Retrieve a :class:`DatasetMetadata` or raise :class:`DatasetNotFoundError`."""
        record = self._repository.get_dataset(dataset_id)
        if record is None:
            raise DatasetNotFoundError(
                f"Dataset '{dataset_id}' not found in catalog.",
                dataset_id=dataset_id,
            )
        return record

    def get_dataset_by_urn(
        self,
        urn: URN | str,
        *,
        raise_if_missing: bool = False,
    ) -> DatasetMetadata | None:
        """Retrieve a :class:`DatasetMetadata` by its :class:`URN`."""
        record = self._repository.get_dataset_by_urn(urn)
        if record is None and raise_if_missing:
            raise DatasetNotFoundError(
                f"Dataset with URN '{urn}' not found in catalog.",
                urn=str(urn),
            )
        return record

    def list_datasets(
        self,
        *,
        layer: str | None = None,
        namespace: str | None = None,
        tag: str | None = None,
        owner: str | None = None,
    ) -> list[DatasetMetadata]:
        """List registered datasets matching optional filters."""
        return self._repository.list_datasets(
            layer=layer,
            namespace=namespace,
            tag=tag,
            owner=owner,
        )

    def update_dataset(
        self,
        dataset_id: str,
        *,
        name: str | None = None,
        layer: str | None = None,
        format: str | None = None,
        location_uri: str | None = None,
        description: str | None = None,
        owner: str | None = None,
        tags: list[str] | None = None,
        custom_properties: dict[str, Any] | None = None,
        merge_properties: bool = True,
    ) -> DatasetMetadata:
        """Update mutable fields, tags, or custom properties on an existing dataset."""
        record = self.require_dataset(dataset_id)
        if name is not None:
            record.name = name
        if layer is not None:
            record.layer = layer
        if format is not None:
            record.format = format
        if location_uri is not None:
            record.location_uri = location_uri
        if description is not None:
            record.description = description
        if owner is not None:
            record.owner = owner
        if tags is not None:
            record.tags = normalize_tags(tags)
        if custom_properties is not None:
            validated_props = validate_json_value(custom_properties, path="custom_properties")
            if merge_properties:
                record.custom_properties = {
                    **record.custom_properties,
                    **validated_props,
                }
            else:
                record.custom_properties = validated_props

        record.updated_at = datetime.now(UTC)
        return self._repository.save_dataset(record, overwrite=True)

    def delete_dataset(self, dataset_id: str) -> bool:
        """Remove ``dataset_id`` from the catalog and return True if it existed."""
        return self._repository.delete_dataset(dataset_id)

    def register_artifact(
        self,
        artifact: ArtifactMetadata | Artifact,
        *,
        overwrite: bool = False,
        require_existing_dataset: bool = False,
    ) -> ArtifactMetadata:
        """Register an :class:`ArtifactMetadata` (or :class:`atlas_core.Artifact`)."""
        meta = (
            artifact
            if isinstance(artifact, ArtifactMetadata)
            else ArtifactMetadata.from_core_artifact(artifact)
        )
        if require_existing_dataset:
            self.require_dataset(meta.dataset_id)
        return self._repository.save_artifact(meta, overwrite=overwrite)

    def get_artifact(
        self,
        artifact_id: str,
        *,
        raise_if_missing: bool = False,
    ) -> ArtifactMetadata | None:
        """Retrieve an :class:`ArtifactMetadata` by ``artifact_id``."""
        record = self._repository.get_artifact(artifact_id)
        if record is None and raise_if_missing:
            raise ArtifactNotFoundError(
                f"Artifact '{artifact_id}' not found in catalog.",
                artifact_id=artifact_id,
            )
        return record

    def list_artifacts(
        self,
        *,
        dataset_id: str | None = None,
        execution_id: str | None = None,
    ) -> list[ArtifactMetadata]:
        """List registered artifacts with optional filtering."""
        return self._repository.list_artifacts(
            dataset_id=dataset_id,
            execution_id=execution_id,
        )


__all__ = ["DatasetCatalog"]
