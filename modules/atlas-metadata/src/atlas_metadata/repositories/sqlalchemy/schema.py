"""SQLAlchemy implementation of :class:`SchemaRepository`."""

from __future__ import annotations

import random
import time
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from atlas_metadata.exceptions import MetadataRepositoryError, SchemaConflictError
from atlas_metadata.identifiers.urn import URN
from atlas_metadata.models.schema import ColumnMetadata, SchemaMetadata
from atlas_metadata.repositories.contracts import SchemaRepository
from atlas_metadata.repositories.sqlalchemy.base import (
    SessionFactory,
    SqlAlchemyRepositoryBase,
    translate_database_errors,
)
from atlas_metadata.repositories.sqlalchemy.mappings import record_to_schema, schema_to_record
from atlas_metadata.repositories.sqlalchemy.models import DatasetRecord, SchemaRecord

DEFAULT_MAX_ALLOCATION_ATTEMPTS = 10


class SqlAlchemySchemaRepository(SqlAlchemyRepositoryBase, SchemaRepository):
    """Persists immutable, versioned schemas (with ordered column rows) through SQLAlchemy.

    Schema version uniqueness is enforced by a database-level unique constraint on
    ``(namespace, dataset_id, version)``. :meth:`register_next_schema` allocates
    ``max(version) + 1`` inside a transaction and retries a bounded number of times when a
    concurrent writer wins the same version. Only that specific version-allocation conflict is
    retried.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        *,
        max_allocation_attempts: int = DEFAULT_MAX_ALLOCATION_ATTEMPTS,
    ) -> None:
        super().__init__(session_factory)
        if max_allocation_attempts < 1:
            raise ValueError("max_allocation_attempts must be >= 1.")
        self._max_allocation_attempts = max_allocation_attempts

    def save_schema(self, schema: SchemaMetadata) -> SchemaMetadata:
        try:
            with translate_database_errors("save_schema"), self._session_factory.begin() as session:
                self._insert_schema(session, schema)
        except IntegrityError as exc:
            raise self._schema_conflict(schema) from exc
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
        last_version = 0
        for attempt in range(1, self._max_allocation_attempts + 1):
            try:
                with (
                    translate_database_errors("register_next_schema"),
                    self._session_factory.begin() as session,
                ):
                    current = session.scalar(
                        select(func.max(SchemaRecord.version)).where(
                            SchemaRecord.dataset_id == clean_ds
                        )
                    )
                    last_version = (current or 0) + 1
                    schema = SchemaMetadata(
                        dataset_id=clean_ds,
                        namespace=namespace,
                        version=last_version,
                        columns=tuple(columns),
                        schema_format=schema_format,
                        description=description,
                        tags=list(tags or []),
                        custom_properties=dict(custom_properties or {}),
                    )
                    self._insert_schema(session, schema)
                return schema
            except SchemaConflictError:
                # Another writer committed this version between our read and our insert.
                pass
            except IntegrityError as exc:
                # Retry only a recognized allocation conflict: the version we tried now exists.
                if not self._version_exists(clean_ds, last_version):
                    raise MetadataRepositoryError(
                        f"Failed to register schema for dataset '{clean_ds}': {exc}",
                        dataset_id=clean_ds,
                    ) from exc
            time.sleep(random.uniform(0.0, 0.005 * attempt))

        raise SchemaConflictError(
            f"Could not allocate a schema version for dataset '{clean_ds}' after "
            f"{self._max_allocation_attempts} attempts due to concurrent registrations.",
            dataset_id=clean_ds,
        )

    def get_schema(
        self,
        dataset_id: str,
        version: int | None = None,
    ) -> SchemaMetadata | None:
        statement = select(SchemaRecord).where(SchemaRecord.dataset_id == dataset_id.strip())
        if version is not None:
            statement = statement.where(SchemaRecord.version == version)
        statement = statement.order_by(SchemaRecord.version.desc(), SchemaRecord.namespace).limit(1)

        with translate_database_errors("get_schema"), self._session_factory() as session:
            record = session.scalar(statement)
            return record_to_schema(record) if record is not None else None

    def get_schema_by_urn(self, urn: URN | str) -> SchemaMetadata | None:
        parsed = URN.parse(urn)
        with translate_database_errors("get_schema_by_urn"), self._session_factory() as session:
            record = session.scalar(select(SchemaRecord).where(SchemaRecord.urn == str(parsed)))
            return record_to_schema(record) if record is not None else None

    def list_schemas(self, dataset_id: str) -> list[SchemaMetadata]:
        statement = (
            select(SchemaRecord)
            .where(SchemaRecord.dataset_id == dataset_id.strip())
            .order_by(SchemaRecord.version, SchemaRecord.namespace)
        )
        with translate_database_errors("list_schemas"), self._session_factory() as session:
            return [record_to_schema(record) for record in session.scalars(statement)]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _insert_schema(self, session: Session, schema: SchemaMetadata) -> None:
        """Insert ``schema`` and sync the dataset's ``latest_schema_version`` in one transaction."""
        clash = session.scalar(
            select(SchemaRecord.id)
            .where(
                or_(
                    and_(
                        SchemaRecord.dataset_id == schema.dataset_id,
                        SchemaRecord.version == schema.version,
                    ),
                    SchemaRecord.urn == str(schema.urn),
                )
            )
            .limit(1)
        )
        if clash is not None:
            raise self._schema_conflict(schema)

        session.add(schema_to_record(schema))
        session.flush()
        self._sync_dataset_latest_version(session, schema)

    @staticmethod
    def _sync_dataset_latest_version(session: Session, schema: SchemaMetadata) -> None:
        dataset = session.scalar(
            select(DatasetRecord).where(DatasetRecord.dataset_id == schema.dataset_id)
        )
        if dataset is not None and schema.version > (dataset.latest_schema_version or 0):
            dataset.latest_schema_version = schema.version
            dataset.updated_at = datetime.now(UTC)

    def _version_exists(self, dataset_id: str, version: int) -> bool:
        with translate_database_errors("schema_version_exists"), self._session_factory() as session:
            found = session.scalar(
                select(SchemaRecord.id)
                .where(SchemaRecord.dataset_id == dataset_id, SchemaRecord.version == version)
                .limit(1)
            )
            return found is not None

    @staticmethod
    def _schema_conflict(schema: SchemaMetadata) -> SchemaConflictError:
        return SchemaConflictError(
            f"Schema version {schema.version} for dataset '{schema.dataset_id}' "
            f"(URN '{schema.urn}') already exists and cannot be overwritten.",
            urn=str(schema.urn),
            dataset_id=schema.dataset_id,
            schema_id=schema.schema_id,
        )


__all__ = ["DEFAULT_MAX_ALLOCATION_ATTEMPTS", "SqlAlchemySchemaRepository"]
