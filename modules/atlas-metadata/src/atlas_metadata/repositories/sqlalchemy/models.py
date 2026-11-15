"""SQLAlchemy ORM records for datasets, schemas, schema columns, and artifacts.

These are persistence-only structures. They are translated to and from the Pydantic domain
models by :mod:`atlas_metadata.repositories.sqlalchemy.mappings`.

Design notes:

* Portable column types only (``String``, ``Text``, ``Integer``, ``BigInteger``, ``Boolean``,
  ``JSON``). JSON-compatible fields use SQLAlchemy's ``JSON`` type rather than hand-rolled
  serialization.
* URNs are stored as their canonical string form in a unique column.
* Schema and artifact rows intentionally have no foreign key to the dataset table: the
  repository contracts allow registering schemas and artifacts before (or without) a dataset
  record, and deleting a dataset never cascades to them.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import TypeDecorator

from atlas_metadata.repositories.sqlalchemy.base import Base


class UTCDateTime(TypeDecorator[datetime]):
    """Timezone-aware UTC datetimes on every backend.

    Values are normalized to UTC and stored without a timezone (SQLite has no timezone-aware
    type), and are returned as timezone-aware UTC datetimes.
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC)


class DatasetRecord(Base):
    """Persistent form of :class:`atlas_metadata.models.dataset.DatasetMetadata`."""

    __tablename__ = "atlas_datasets"
    __table_args__ = (
        UniqueConstraint("namespace", "dataset_id", name="uq_atlas_datasets_namespace_dataset_id"),
        Index("ix_atlas_datasets_dataset_id", "dataset_id"),
        Index("ix_atlas_datasets_layer", "layer"),
        Index("ix_atlas_datasets_owner", "owner"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    urn: Mapped[str] = mapped_column(String(512), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    namespace: Mapped[str] = mapped_column(String(128), nullable=False)
    layer: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    format: Mapped[str | None] = mapped_column(String(64), nullable=True)
    location_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tags: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    upstream_urns: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    custom_properties: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    latest_schema_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)


class SchemaRecord(Base):
    """Persistent form of :class:`atlas_metadata.models.schema.SchemaMetadata` (header row)."""

    __tablename__ = "atlas_schemas"
    __table_args__ = (
        UniqueConstraint(
            "namespace",
            "dataset_id",
            "version",
            name="uq_atlas_schemas_namespace_dataset_id_version",
        ),
        Index("ix_atlas_schemas_dataset_id_version", "dataset_id", "version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    schema_id: Mapped[str] = mapped_column(String(300), nullable=False)
    urn: Mapped[str] = mapped_column(String(512), nullable=False, unique=True)
    dataset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    namespace: Mapped[str] = mapped_column(String(128), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_format: Mapped[str] = mapped_column(String(64), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    tags: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    custom_properties: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)

    columns: Mapped[list[SchemaColumnRecord]] = relationship(
        back_populates="schema",
        cascade="all, delete-orphan",
        order_by="SchemaColumnRecord.position",
        lazy="selectin",
    )


class SchemaColumnRecord(Base):
    """Persistent form of :class:`atlas_metadata.models.schema.ColumnMetadata`.

    ``position`` preserves the declared column order.
    """

    __tablename__ = "atlas_schema_columns"
    __table_args__ = (
        UniqueConstraint(
            "schema_pk", "position", name="uq_atlas_schema_columns_schema_pk_position"
        ),
        UniqueConstraint("schema_pk", "name", name="uq_atlas_schema_columns_schema_pk_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    schema_pk: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("atlas_schemas.id", ondelete="CASCADE"),
        nullable=False,
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    data_type: Mapped[str] = mapped_column(String(128), nullable=False)
    nullable: Mapped[bool] = mapped_column(Boolean, nullable=False)
    primary_key: Mapped[bool] = mapped_column(Boolean, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    default_value: Mapped[Any] = mapped_column(JSON(none_as_null=True), nullable=True)
    tags: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    custom_properties: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    schema: Mapped[SchemaRecord] = relationship(back_populates="columns")


class ArtifactRecord(Base):
    """Persistent form of :class:`atlas_metadata.models.artifact.ArtifactMetadata`."""

    __tablename__ = "atlas_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "namespace", "artifact_id", name="uq_atlas_artifacts_namespace_artifact_id"
        ),
        Index("ix_atlas_artifacts_artifact_id", "artifact_id"),
        Index("ix_atlas_artifacts_dataset_id", "dataset_id"),
        Index("ix_atlas_artifacts_execution_id", "execution_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    artifact_id: Mapped[str] = mapped_column(String(255), nullable=False)
    urn: Mapped[str] = mapped_column(String(512), nullable=False, unique=True)
    dataset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    namespace: Mapped[str] = mapped_column(String(128), nullable=False)
    uri: Mapped[str] = mapped_column(Text, nullable=False)
    format: Mapped[str] = mapped_column(String(64), nullable=False)
    layer: Mapped[str | None] = mapped_column(String(64), nullable=True)
    execution_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    schema_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    byte_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    checksum: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tags: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    custom_properties: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)


__all__ = [
    "ArtifactRecord",
    "DatasetRecord",
    "SchemaColumnRecord",
    "SchemaRecord",
    "UTCDateTime",
]
