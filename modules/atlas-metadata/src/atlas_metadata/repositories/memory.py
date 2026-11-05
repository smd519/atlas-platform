"""Thread-safe in-memory metadata repository implementation."""

from __future__ import annotations

import threading
from collections.abc import Sequence
from datetime import UTC, datetime

from atlas_metadata.exceptions import (
    DuplicateArtifactError,
    DuplicateDatasetError,
    SchemaConflictError,
)
from atlas_metadata.identifiers.urn import URN
from atlas_metadata.models.artifact import ArtifactMetadata
from atlas_metadata.models.dataset import DatasetMetadata
from atlas_metadata.models.schema import ColumnMetadata, SchemaMetadata
from atlas_metadata.repositories.contracts import MetadataRepository


class InMemoryMetadataRepository(MetadataRepository):
    """Thread-safe in-memory implementation of :class:`MetadataRepository`.

    Uses a re-entrant lock (:class:`threading.RLock`) to guarantee atomic reads
    and writes, including strict atomic uniqueness of ``(dataset_id, version)``
    during concurrent schema registration.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._datasets_by_id: dict[str, DatasetMetadata] = {}
        self._datasets_by_urn: dict[URN, str] = {}
        self._schemas_by_key: dict[tuple[str, int], SchemaMetadata] = {}
        self._schemas_by_urn: dict[URN, tuple[str, int]] = {}
        self._artifacts_by_id: dict[str, ArtifactMetadata] = {}
        self._artifacts_by_urn: dict[URN, str] = {}

    # ------------------------------------------------------------------
    # Dataset operations
    # ------------------------------------------------------------------

    def save_dataset(
        self,
        dataset: DatasetMetadata,
        *,
        overwrite: bool = False,
    ) -> DatasetMetadata:
        with self._lock:
            existing_id_for_urn = self._datasets_by_urn.get(dataset.urn)
            if (
                dataset.dataset_id in self._datasets_by_id or existing_id_for_urn is not None
            ) and not overwrite:
                raise DuplicateDatasetError(
                    f"Dataset '{dataset.dataset_id}' (URN '{dataset.urn}') is already registered.",
                    urn=str(dataset.urn),
                    dataset_id=dataset.dataset_id,
                )

            existing = self._datasets_by_id.get(dataset.dataset_id)
            stored = dataset.model_copy(deep=True)
            if existing is not None:
                if existing.urn in self._datasets_by_urn:
                    del self._datasets_by_urn[existing.urn]
                stored.created_at = existing.created_at
                stored.updated_at = datetime.now(UTC)

            self._datasets_by_id[stored.dataset_id] = stored
            self._datasets_by_urn[stored.urn] = stored.dataset_id
            return stored.model_copy(deep=True)

    def get_dataset(self, dataset_id: str) -> DatasetMetadata | None:
        with self._lock:
            record = self._datasets_by_id.get(dataset_id.strip())
            return record.model_copy(deep=True) if record is not None else None

    def get_dataset_by_urn(self, urn: URN | str) -> DatasetMetadata | None:
        parsed = URN.parse(urn)
        with self._lock:
            ds_id = self._datasets_by_urn.get(parsed)
            if ds_id is None:
                return None
            record = self._datasets_by_id.get(ds_id)
            return record.model_copy(deep=True) if record is not None else None

    def list_datasets(
        self,
        *,
        layer: str | None = None,
        namespace: str | None = None,
        tag: str | None = None,
        owner: str | None = None,
    ) -> list[DatasetMetadata]:
        with self._lock:
            items = list(self._datasets_by_id.values())
            if layer is not None:
                norm_layer = layer.strip().lower()
                items = [d for d in items if d.layer == norm_layer]
            if namespace is not None:
                norm_ns = namespace.strip().lower()
                items = [d for d in items if d.namespace == norm_ns]
            if tag is not None:
                norm_tag = tag.strip()
                items = [d for d in items if norm_tag in d.tags]
            if owner is not None:
                norm_owner = owner.strip()
                items = [d for d in items if d.owner == norm_owner]
            return [d.model_copy(deep=True) for d in items]

    def delete_dataset(self, dataset_id: str) -> bool:
        with self._lock:
            record = self._datasets_by_id.pop(dataset_id.strip(), None)
            if record is None:
                return False
            self._datasets_by_urn.pop(record.urn, None)
            return True

    # ------------------------------------------------------------------
    # Schema operations (strictly immutable & atomic)
    # ------------------------------------------------------------------

    def save_schema(self, schema: SchemaMetadata) -> SchemaMetadata:
        key = (schema.dataset_id, schema.version)
        with self._lock:
            if key in self._schemas_by_key or schema.urn in self._schemas_by_urn:
                raise SchemaConflictError(
                    f"Schema version {schema.version} for dataset '{schema.dataset_id}' "
                    f"(URN '{schema.urn}') already exists and cannot be overwritten.",
                    urn=str(schema.urn),
                    dataset_id=schema.dataset_id,
                    schema_id=schema.schema_id,
                )

            self._schemas_by_key[key] = schema
            self._schemas_by_urn[schema.urn] = key
            self._sync_dataset_latest_schema_version(schema.dataset_id, schema.version)
            return schema

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
        clean_ds = dataset_id.strip()
        with self._lock:
            existing_versions = [ver for (ds, ver) in self._schemas_by_key if ds == clean_ds]
            next_version = (max(existing_versions) + 1) if existing_versions else 1
            schema = SchemaMetadata(
                dataset_id=clean_ds,
                namespace=namespace,
                version=next_version,
                columns=tuple(columns),
                schema_format=schema_format,
                description=description,
                tags=list(tags or []),
                custom_properties=dict(custom_properties or {}),
            )
            return self.save_schema(schema)

    def get_schema(
        self,
        dataset_id: str,
        version: int | None = None,
    ) -> SchemaMetadata | None:
        clean_ds = dataset_id.strip()
        with self._lock:
            if version is not None:
                return self._schemas_by_key.get((clean_ds, version))

            matching = [
                schema for (ds, _), schema in self._schemas_by_key.items() if ds == clean_ds
            ]
            if not matching:
                return None
            return max(matching, key=lambda s: s.version)

    def get_schema_by_urn(self, urn: URN | str) -> SchemaMetadata | None:
        parsed = URN.parse(urn)
        with self._lock:
            key = self._schemas_by_urn.get(parsed)
            if key is None:
                return None
            return self._schemas_by_key.get(key)

    def list_schemas(self, dataset_id: str) -> list[SchemaMetadata]:
        clean_ds = dataset_id.strip()
        with self._lock:
            matching = [
                schema for (ds, _), schema in self._schemas_by_key.items() if ds == clean_ds
            ]
            return sorted(matching, key=lambda s: s.version)

    def _sync_dataset_latest_schema_version(
        self,
        dataset_id: str,
        version: int,
    ) -> None:
        dataset = self._datasets_by_id.get(dataset_id)
        if dataset is not None:
            current_latest = dataset.latest_schema_version or 0
            if version > current_latest:
                dataset.latest_schema_version = version
                dataset.updated_at = datetime.now(UTC)

    # ------------------------------------------------------------------
    # Artifact operations
    # ------------------------------------------------------------------

    def save_artifact(
        self,
        artifact: ArtifactMetadata,
        *,
        overwrite: bool = False,
    ) -> ArtifactMetadata:
        with self._lock:
            if (
                artifact.artifact_id in self._artifacts_by_id
                or artifact.urn in self._artifacts_by_urn
            ) and not overwrite:
                raise DuplicateArtifactError(
                    f"Artifact '{artifact.artifact_id}' (URN '{artifact.urn}') "
                    "is already registered.",
                    urn=str(artifact.urn),
                    dataset_id=artifact.dataset_id,
                    artifact_id=artifact.artifact_id,
                )

            existing = self._artifacts_by_id.get(artifact.artifact_id)
            if existing is not None and existing.urn in self._artifacts_by_urn:
                del self._artifacts_by_urn[existing.urn]

            self._artifacts_by_id[artifact.artifact_id] = artifact
            self._artifacts_by_urn[artifact.urn] = artifact.artifact_id
            return artifact

    def get_artifact(self, artifact_id: str) -> ArtifactMetadata | None:
        with self._lock:
            return self._artifacts_by_id.get(artifact_id.strip())

    def get_artifact_by_urn(self, urn: URN | str) -> ArtifactMetadata | None:
        parsed = URN.parse(urn)
        with self._lock:
            art_id = self._artifacts_by_urn.get(parsed)
            if art_id is None:
                return None
            return self._artifacts_by_id.get(art_id)

    def list_artifacts(
        self,
        *,
        dataset_id: str | None = None,
        execution_id: str | None = None,
    ) -> list[ArtifactMetadata]:
        with self._lock:
            items = list(self._artifacts_by_id.values())
            if dataset_id is not None:
                clean_ds = dataset_id.strip()
                items = [a for a in items if a.dataset_id == clean_ds]
            if execution_id is not None:
                clean_exec = execution_id.strip()
                items = [a for a in items if a.execution_id == clean_exec]
            return items


__all__ = ["InMemoryMetadataRepository"]
