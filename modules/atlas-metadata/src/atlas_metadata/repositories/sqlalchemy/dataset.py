"""SQLAlchemy implementation of :class:`DatasetRepository`."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from atlas_metadata.exceptions import DuplicateDatasetError
from atlas_metadata.identifiers.urn import URN
from atlas_metadata.models.dataset import DatasetMetadata
from atlas_metadata.repositories.contracts import DatasetRepository
from atlas_metadata.repositories.sqlalchemy.base import (
    SqlAlchemyRepositoryBase,
    translate_database_errors,
)
from atlas_metadata.repositories.sqlalchemy.mappings import (
    apply_dataset_to_record,
    record_to_dataset,
)
from atlas_metadata.repositories.sqlalchemy.models import DatasetRecord


class SqlAlchemyDatasetRepository(SqlAlchemyRepositoryBase, DatasetRepository):
    """Persists dataset catalog records through SQLAlchemy.

    Semantics mirror :class:`InMemoryMetadataRepository`: lookups are by ``dataset_id`` (which
    the contract does not qualify by namespace), overwriting preserves ``created_at`` and
    refreshes ``updated_at``, and duplicates are rejected unless ``overwrite=True``.
    """

    def save_dataset(
        self,
        dataset: DatasetMetadata,
        *,
        overwrite: bool = False,
    ) -> DatasetMetadata:
        urn = str(dataset.urn)
        try:
            with (
                translate_database_errors("save_dataset"),
                self._write_session() as session,
            ):
                existing = session.scalar(
                    select(DatasetRecord).where(
                        DatasetRecord.workspace_id == self._workspace_id,
                        DatasetRecord.dataset_id == dataset.dataset_id,
                    )
                )
                urn_owner = session.scalar(
                    select(DatasetRecord).where(
                        DatasetRecord.workspace_id == self._workspace_id,
                        DatasetRecord.urn == urn,
                    )
                )

                if (existing is not None or urn_owner is not None) and not overwrite:
                    raise self._duplicate_dataset(dataset)
                if existing is None and urn_owner is not None:
                    # The URN belongs to a different dataset_id; never silently steal it.
                    raise self._duplicate_dataset(dataset)

                if existing is None:
                    record = DatasetRecord()
                    apply_dataset_to_record(record, dataset, workspace_id=self._workspace_id)
                    session.add(record)
                else:
                    record = existing
                    created_at = existing.created_at
                    apply_dataset_to_record(record, dataset, workspace_id=self._workspace_id)
                    record.created_at = created_at
                    record.updated_at = datetime.now(UTC)

                session.flush()
                return record_to_dataset(record)
        except IntegrityError as exc:
            raise self._duplicate_dataset(dataset) from exc

    def get_dataset(self, dataset_id: str) -> DatasetMetadata | None:
        with translate_database_errors("get_dataset"), self._read_session() as session:
            record = session.scalar(
                select(DatasetRecord).where(
                    DatasetRecord.workspace_id == self._workspace_id,
                    DatasetRecord.dataset_id == dataset_id.strip(),
                )
            )
            return record_to_dataset(record) if record is not None else None

    def get_dataset_by_urn(self, urn: URN | str) -> DatasetMetadata | None:
        parsed = URN.parse(urn)
        with translate_database_errors("get_dataset_by_urn"), self._read_session() as session:
            record = session.scalar(
                select(DatasetRecord).where(
                    DatasetRecord.workspace_id == self._workspace_id,
                    DatasetRecord.urn == str(parsed),
                )
            )
            return record_to_dataset(record) if record is not None else None

    def list_datasets(
        self,
        *,
        layer: str | None = None,
        namespace: str | None = None,
        tag: str | None = None,
        owner: str | None = None,
    ) -> list[DatasetMetadata]:
        statement = (
            select(DatasetRecord)
            .where(DatasetRecord.workspace_id == self._workspace_id)
            .order_by(DatasetRecord.created_at, DatasetRecord.dataset_id)
        )
        if layer is not None:
            statement = statement.where(DatasetRecord.layer == layer.strip().lower())
        if namespace is not None:
            statement = statement.where(DatasetRecord.namespace == namespace.strip().lower())
        if owner is not None:
            statement = statement.where(DatasetRecord.owner == owner.strip())

        with translate_database_errors("list_datasets"), self._read_session() as session:
            datasets = [record_to_dataset(record) for record in session.scalars(statement)]

        if tag is not None:
            # Membership in a JSON array has no portable SQL form, so tags are filtered here.
            norm_tag = tag.strip()
            datasets = [d for d in datasets if norm_tag in d.tags]
        return datasets

    def delete_dataset(self, dataset_id: str) -> bool:
        with translate_database_errors("delete_dataset"), self._write_session() as session:
            record = session.scalar(
                select(DatasetRecord).where(
                    DatasetRecord.workspace_id == self._workspace_id,
                    DatasetRecord.dataset_id == dataset_id.strip(),
                )
            )
            if record is None:
                return False
            session.delete(record)
            return True

    @staticmethod
    def _duplicate_dataset(dataset: DatasetMetadata) -> DuplicateDatasetError:
        return DuplicateDatasetError(
            f"Dataset '{dataset.dataset_id}' (URN '{dataset.urn}') is already registered.",
            urn=str(dataset.urn),
            dataset_id=dataset.dataset_id,
        )


__all__ = ["SqlAlchemyDatasetRepository"]
