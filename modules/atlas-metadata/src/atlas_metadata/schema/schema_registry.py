"""Schema registry managing versioned, immutable dataset schemas."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from atlas_metadata.exceptions import SchemaNotFoundError
from atlas_metadata.identifiers.urn import URN
from atlas_metadata.models.schema import ColumnMetadata, SchemaMetadata
from atlas_metadata.repositories.contracts import SchemaRepository
from atlas_metadata.repositories.memory import InMemoryMetadataRepository


class SchemaRegistry:
    """Versioned schema registry enforcing immutable schemas and atomic version uniqueness."""

    def __init__(self, repository: SchemaRepository | None = None) -> None:
        self._repository = repository or InMemoryMetadataRepository()

    @property
    def repository(self) -> SchemaRepository:
        """Return the underlying :class:`SchemaRepository`."""
        return self._repository

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
        """Atomically register an immutable :class:`SchemaMetadata` version.

        Can be called either with a constructed :class:`SchemaMetadata` instance
        or with ``(dataset_id, columns, version=...)``. When ``version=None``,
        atomically allocates the next version number for ``dataset_id``.
        """
        if isinstance(schema_or_dataset_id, SchemaMetadata):
            return self._repository.save_schema(schema_or_dataset_id)

        dataset_id = str(schema_or_dataset_id).strip()
        parsed_cols = [
            col if isinstance(col, ColumnMetadata) else ColumnMetadata(**col)
            for col in (columns or [])
        ]

        if version is None:
            return self._repository.register_next_schema(
                dataset_id=dataset_id,
                columns=parsed_cols,
                namespace=namespace,
                schema_format=schema_format,
                description=description,
                tags=tags,
                custom_properties=custom_properties,
            )

        schema = SchemaMetadata(
            dataset_id=dataset_id,
            namespace=namespace,
            version=version,
            columns=tuple(parsed_cols),
            schema_format=schema_format,
            description=description,
            tags=list(tags or []),
            custom_properties=dict(custom_properties or {}),
        )
        return self._repository.save_schema(schema)

    def get_schema(
        self,
        dataset_id: str,
        version: int | None = None,
        *,
        raise_if_missing: bool = False,
    ) -> SchemaMetadata | None:
        """Retrieve a specific ``version`` (or latest when ``version=None``) for ``dataset_id``."""
        record = self._repository.get_schema(dataset_id, version=version)
        if record is None and raise_if_missing:
            ver_desc = f"v{version}" if version is not None else "latest"
            raise SchemaNotFoundError(
                f"Schema ({ver_desc}) for dataset '{dataset_id}' not found.",
                dataset_id=dataset_id,
            )
        return record

    def require_schema(
        self,
        dataset_id: str,
        version: int | None = None,
    ) -> SchemaMetadata:
        """Retrieve a schema for ``dataset_id`` or raise :class:`SchemaNotFoundError`."""
        record = self.get_schema(dataset_id, version=version, raise_if_missing=True)
        assert record is not None
        return record

    def get_latest_schema(
        self,
        dataset_id: str,
        *,
        raise_if_missing: bool = False,
    ) -> SchemaMetadata | None:
        """Retrieve the highest registered version of :class:`SchemaMetadata` for ``dataset_id``."""
        return self.get_schema(
            dataset_id,
            version=None,
            raise_if_missing=raise_if_missing,
        )

    def get_schema_by_urn(
        self,
        urn: URN | str,
        *,
        raise_if_missing: bool = False,
    ) -> SchemaMetadata | None:
        """Retrieve a :class:`SchemaMetadata` by its :class:`URN`."""
        record = self._repository.get_schema_by_urn(urn)
        if record is None and raise_if_missing:
            raise SchemaNotFoundError(
                f"Schema with URN '{urn}' not found.",
                urn=str(urn),
            )
        return record

    def list_schemas(self, dataset_id: str) -> list[SchemaMetadata]:
        """List all registered schema versions for ``dataset_id`` in ascending version order."""
        return self._repository.list_schemas(dataset_id)


__all__ = ["SchemaRegistry"]
