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
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import TypeDecorator

from atlas_metadata.repositories.sqlalchemy.base import Base


class TenantOwnedMixin:
    """Mixin supplying the mandatory ``workspace_id`` column for tenant-owned tables."""

    workspace_id: Mapped[str] = mapped_column(String(128), nullable=False)


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


class DatasetRecord(TenantOwnedMixin, Base):
    """Persistent form of :class:`atlas_metadata.models.dataset.DatasetMetadata`."""

    __tablename__ = "atlas_datasets"
    __table_args__ = (
        PrimaryKeyConstraint("workspace_id", "dataset_id", name="pk_atlas_datasets"),
        UniqueConstraint(
            "workspace_id",
            "namespace",
            "dataset_id",
            name="uq_atlas_datasets_workspace_id_namespace_dataset_id",
        ),
        UniqueConstraint(
            "workspace_id",
            "urn",
            name="uq_atlas_datasets_workspace_id_urn",
        ),
        Index("ix_atlas_datasets_workspace_id_layer", "workspace_id", "layer"),
        Index("ix_atlas_datasets_workspace_id_owner", "workspace_id", "owner"),
    )

    dataset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    urn: Mapped[str] = mapped_column(String(512), nullable=False)
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


class SchemaRecord(TenantOwnedMixin, Base):
    """Persistent form of :class:`atlas_metadata.models.schema.SchemaMetadata` (header row)."""

    __tablename__ = "atlas_schemas"
    __table_args__ = (
        PrimaryKeyConstraint("workspace_id", "schema_id", name="pk_atlas_schemas"),
        UniqueConstraint(
            "workspace_id",
            "namespace",
            "dataset_id",
            "version",
            name="uq_atlas_schemas_workspace_id_namespace_dataset_id_version",
        ),
        UniqueConstraint(
            "workspace_id",
            "urn",
            name="uq_atlas_schemas_workspace_id_urn",
        ),
        Index(
            "ix_atlas_schemas_workspace_id_dataset_id_version",
            "workspace_id",
            "dataset_id",
            "version",
        ),
    )

    schema_id: Mapped[str] = mapped_column(String(300), nullable=False)
    urn: Mapped[str] = mapped_column(String(512), nullable=False)
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


class SchemaColumnRecord(TenantOwnedMixin, Base):
    """Persistent form of :class:`atlas_metadata.models.schema.ColumnMetadata`.

    ``position`` preserves the declared column order.
    """

    __tablename__ = "atlas_schema_columns"
    __table_args__ = (
        PrimaryKeyConstraint(
            "workspace_id", "schema_id", "position", name="pk_atlas_schema_columns"
        ),
        ForeignKeyConstraint(
            ["workspace_id", "schema_id"],
            ["atlas_schemas.workspace_id", "atlas_schemas.schema_id"],
            name="fk_atlas_schema_columns_workspace_id_schema_id_atlas_schemas",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "workspace_id",
            "schema_id",
            "name",
            name="uq_atlas_schema_columns_workspace_id_schema_id_name",
        ),
    )

    schema_id: Mapped[str] = mapped_column(String(300), nullable=False)
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


class ArtifactRecord(TenantOwnedMixin, Base):
    """Persistent form of :class:`atlas_metadata.models.artifact.ArtifactMetadata`."""

    __tablename__ = "atlas_artifacts"
    __table_args__ = (
        PrimaryKeyConstraint("workspace_id", "artifact_id", name="pk_atlas_artifacts"),
        UniqueConstraint(
            "workspace_id",
            "namespace",
            "artifact_id",
            name="uq_atlas_artifacts_workspace_id_namespace_artifact_id",
        ),
        UniqueConstraint(
            "workspace_id",
            "urn",
            name="uq_atlas_artifacts_workspace_id_urn",
        ),
        Index("ix_atlas_artifacts_workspace_id_dataset_id", "workspace_id", "dataset_id"),
        Index("ix_atlas_artifacts_workspace_id_execution_id", "workspace_id", "execution_id"),
    )

    artifact_id: Mapped[str] = mapped_column(String(255), nullable=False)
    urn: Mapped[str] = mapped_column(String(512), nullable=False)
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
    "TenantOwnedMixin",
    "UTCDateTime",
]
