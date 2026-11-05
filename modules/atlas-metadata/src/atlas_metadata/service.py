"""Public metadata service facade implementing atlas_core.MetadataProvider."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from atlas_core import Artifact, Dataset, MetadataProvider

from atlas_metadata.catalog.dataset_catalog import DatasetCatalog
from atlas_metadata.identifiers.urn import URN
from atlas_metadata.models.artifact import ArtifactMetadata
from atlas_metadata.models.dataset import DatasetMetadata
from atlas_metadata.models.schema import ColumnMetadata, SchemaMetadata
from atlas_metadata.repositories.contracts import MetadataRepository
from atlas_metadata.repositories.memory import InMemoryMetadataRepository
from atlas_metadata.schema.schema_registry import SchemaRegistry


class MetadataService(MetadataProvider):
    """Dependency-injected metadata foundation service.

    Implements :class:`atlas_core.MetadataProvider` while coordinating the
    :class:`DatasetCatalog` and :class:`SchemaRegistry` over a shared thread-safe
    :class:`MetadataRepository`.
    """

    def __init__(
        self,
        repository: MetadataRepository | None = None,
        *,
        catalog: DatasetCatalog | None = None,
        schema_registry: SchemaRegistry | None = None,
    ) -> None:
        resolved_repo = repository or (
            catalog.repository if catalog is not None else InMemoryMetadataRepository()
        )
        self._repository = resolved_repo
        self._catalog = catalog or DatasetCatalog(repository=resolved_repo)
        self._schema_registry = schema_registry or SchemaRegistry(repository=resolved_repo)

    @property
    def repository(self) -> MetadataRepository:
        """Return the underlying :class:`MetadataRepository`."""
        return self._repository

    @property
    def catalog(self) -> DatasetCatalog:
        """Return the :class:`DatasetCatalog` facade."""
        return self._catalog

    @property
    def schema_registry(self) -> SchemaRegistry:
        """Return the :class:`SchemaRegistry` facade."""
        return self._schema_registry

    # ------------------------------------------------------------------
    # atlas_core.MetadataProvider contract implementation
    # ------------------------------------------------------------------

    def register_dataset(
        self,
        dataset: Dataset | DatasetMetadata,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register a :class:`Dataset` or :class:`DatasetMetadata` in the catalog."""
        self._catalog.register_dataset(dataset, overwrite=overwrite)

    def get_dataset(self, dataset_id: str) -> Dataset | None:
        """Retrieve a core :class:`Dataset` by ``dataset_id``, or ``None`` if not found."""
        meta = self._catalog.get_dataset(dataset_id)
        return meta.to_core_dataset() if meta is not None else None

    # ------------------------------------------------------------------
    # Rich Dataset Catalog operations
    # ------------------------------------------------------------------

    def register_dataset_metadata(
        self,
        dataset: DatasetMetadata | Dataset,
        *,
        overwrite: bool = False,
    ) -> DatasetMetadata:
        """Register a dataset and return the stored :class:`DatasetMetadata`."""
        return self._catalog.register_dataset(dataset, overwrite=overwrite)

    def get_dataset_metadata(
        self,
        dataset_id: str,
        *,
        raise_if_missing: bool = False,
    ) -> DatasetMetadata | None:
        """Retrieve the full :class:`DatasetMetadata` record by ``dataset_id``."""
        return self._catalog.get_dataset(dataset_id, raise_if_missing=raise_if_missing)

    def get_dataset_by_urn(
        self,
        urn: URN | str,
        *,
        raise_if_missing: bool = False,
    ) -> DatasetMetadata | None:
        """Retrieve a :class:`DatasetMetadata` record by its :class:`URN`."""
        return self._catalog.get_dataset_by_urn(urn, raise_if_missing=raise_if_missing)

    def list_datasets(
        self,
        *,
        layer: str | None = None,
        namespace: str | None = None,
        tag: str | None = None,
        owner: str | None = None,
    ) -> list[DatasetMetadata]:
        """List registered datasets matching optional filters."""
        return self._catalog.list_datasets(
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
        """Update mutable properties or tags on an existing dataset."""
        return self._catalog.update_dataset(
            dataset_id,
            name=name,
            layer=layer,
            format=format,
            location_uri=location_uri,
            description=description,
            owner=owner,
            tags=tags,
            custom_properties=custom_properties,
            merge_properties=merge_properties,
        )

    def delete_dataset(self, dataset_id: str) -> bool:
        """Delete ``dataset_id`` from the catalog."""
        return self._catalog.delete_dataset(dataset_id)

    # ------------------------------------------------------------------
    # Schema Registry operations
    # ------------------------------------------------------------------

    def register_schema(
        self,
        schema_or_dataset_id: SchemaMetadata | str,
        columns: Sequence[ColumnMetadata | dict[str, Any]] | None = None,
        *,
        version: int | None = None,
        namespace: str = "default",
        schema_format: str = "tabular",
        description: str | None = None,
        tags: list[str] | None = None,
        custom_properties: dict[str, Any] | None = None,
    ) -> SchemaMetadata:
        """Atomically register an immutable :class:`SchemaMetadata` version."""
        return self._schema_registry.register_schema(
            schema_or_dataset_id,
            columns=columns,
            version=version,
            namespace=namespace,
            schema_format=schema_format,
            description=description,
            tags=tags,
            custom_properties=custom_properties,
        )

    def get_schema(
        self,
        dataset_id: str,
        version: int | None = None,
        *,
        raise_if_missing: bool = False,
    ) -> SchemaMetadata | None:
        """Retrieve a specific ``version`` (or latest when ``version=None``) for ``dataset_id``."""
        return self._schema_registry.get_schema(
            dataset_id,
            version=version,
            raise_if_missing=raise_if_missing,
        )

    def get_latest_schema(
        self,
        dataset_id: str,
        *,
        raise_if_missing: bool = False,
    ) -> SchemaMetadata | None:
        """Retrieve the latest registered :class:`SchemaMetadata` for ``dataset_id``."""
        return self._schema_registry.get_latest_schema(
            dataset_id, raise_if_missing=raise_if_missing
        )

    def get_schema_by_urn(
        self,
        urn: URN | str,
        *,
        raise_if_missing: bool = False,
    ) -> SchemaMetadata | None:
        """Retrieve a :class:`SchemaMetadata` by its :class:`URN`."""
        return self._schema_registry.get_schema_by_urn(urn, raise_if_missing=raise_if_missing)

    def list_schemas(self, dataset_id: str) -> list[SchemaMetadata]:
        """List all registered schema versions for ``dataset_id``."""
        return self._schema_registry.list_schemas(dataset_id)

    # ------------------------------------------------------------------
    # Artifact Metadata operations
    # ------------------------------------------------------------------

    def register_artifact(
        self,
        artifact: ArtifactMetadata | Artifact,
        *,
        overwrite: bool = False,
        require_existing_dataset: bool = False,
    ) -> ArtifactMetadata:
        """Register an :class:`ArtifactMetadata` (or :class:`atlas_core.Artifact`)."""
        return self._catalog.register_artifact(
            artifact,
            overwrite=overwrite,
            require_existing_dataset=require_existing_dataset,
        )

    def get_artifact(
        self,
        artifact_id: str,
        *,
        raise_if_missing: bool = False,
    ) -> ArtifactMetadata | None:
        """Retrieve an :class:`ArtifactMetadata` by ``artifact_id``."""
        return self._catalog.get_artifact(artifact_id, raise_if_missing=raise_if_missing)

    def list_artifacts(
        self,
        *,
        dataset_id: str | None = None,
        execution_id: str | None = None,
    ) -> list[ArtifactMetadata]:
        """List registered artifacts matching optional filters."""
        return self._catalog.list_artifacts(
            dataset_id=dataset_id,
            execution_id=execution_id,
        )


__all__ = ["MetadataService"]
