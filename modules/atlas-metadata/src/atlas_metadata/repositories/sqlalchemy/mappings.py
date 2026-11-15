"""Explicit translation between Pydantic domain models and SQLAlchemy ORM records.

The domain models know nothing about SQLAlchemy; all conversion logic lives here.
"""

from __future__ import annotations

import copy

from atlas_metadata.models.artifact import ArtifactMetadata
from atlas_metadata.models.dataset import DatasetMetadata
from atlas_metadata.models.schema import ColumnMetadata, SchemaMetadata
from atlas_metadata.repositories.sqlalchemy.models import (
    ArtifactRecord,
    DatasetRecord,
    SchemaColumnRecord,
    SchemaRecord,
)

# ----------------------------------------------------------------------
# Datasets
# ----------------------------------------------------------------------


def apply_dataset_to_record(record: DatasetRecord, dataset: DatasetMetadata) -> DatasetRecord:
    """Copy every :class:`DatasetMetadata` field onto ``record`` (in place) and return it."""
    record.dataset_id = dataset.dataset_id
    record.urn = str(dataset.urn)
    record.name = dataset.name
    record.namespace = dataset.namespace
    record.layer = dataset.layer
    record.source_id = dataset.source_id
    record.format = dataset.format
    record.location_uri = dataset.location_uri
    record.description = dataset.description
    record.owner = dataset.owner
    record.tags = list(dataset.tags)
    record.upstream_urns = list(dataset.upstream_urns)
    record.custom_properties = copy.deepcopy(dataset.custom_properties)
    record.latest_schema_version = dataset.latest_schema_version
    record.created_at = dataset.created_at
    record.updated_at = dataset.updated_at
    return record


def record_to_dataset(record: DatasetRecord) -> DatasetMetadata:
    """Build a validated :class:`DatasetMetadata` from ``record``."""
    return DatasetMetadata(
        dataset_id=record.dataset_id,
        urn=record.urn,
        name=record.name,
        namespace=record.namespace,
        layer=record.layer,
        source_id=record.source_id,
        format=record.format,
        location_uri=record.location_uri,
        description=record.description,
        owner=record.owner,
        tags=list(record.tags),
        upstream_urns=list(record.upstream_urns),
        custom_properties=copy.deepcopy(record.custom_properties),
        latest_schema_version=record.latest_schema_version,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


# ----------------------------------------------------------------------
# Schemas
# ----------------------------------------------------------------------


def schema_to_record(schema: SchemaMetadata) -> SchemaRecord:
    """Build a new :class:`SchemaRecord` (with ordered column rows) from ``schema``."""
    record = SchemaRecord(
        schema_id=schema.schema_id,
        urn=str(schema.urn),
        dataset_id=schema.dataset_id,
        namespace=schema.namespace,
        version=schema.version,
        schema_format=schema.schema_format,
        description=schema.description,
        tags=list(schema.tags),
        custom_properties=copy.deepcopy(schema.custom_properties),
        created_at=schema.created_at,
    )
    record.columns = [
        SchemaColumnRecord(
            position=position,
            name=column.name,
            data_type=column.data_type,
            nullable=column.nullable,
            primary_key=column.primary_key,
            description=column.description,
            default_value=copy.deepcopy(column.default_value),
            tags=list(column.tags),
            custom_properties=copy.deepcopy(column.custom_properties),
        )
        for position, column in enumerate(schema.columns)
    ]
    return record


def record_to_schema(record: SchemaRecord) -> SchemaMetadata:
    """Build a validated :class:`SchemaMetadata` from ``record``, preserving column order."""
    columns = tuple(
        ColumnMetadata(
            name=column.name,
            data_type=column.data_type,
            nullable=column.nullable,
            primary_key=column.primary_key,
            description=column.description,
            default_value=copy.deepcopy(column.default_value),
            tags=list(column.tags),
            custom_properties=copy.deepcopy(column.custom_properties),
        )
        for column in sorted(record.columns, key=lambda c: c.position)
    )
    return SchemaMetadata(
        schema_id=record.schema_id,
        urn=record.urn,
        dataset_id=record.dataset_id,
        namespace=record.namespace,
        version=record.version,
        columns=columns,
        schema_format=record.schema_format,
        description=record.description,
        tags=list(record.tags),
        custom_properties=copy.deepcopy(record.custom_properties),
        created_at=record.created_at,
    )


# ----------------------------------------------------------------------
# Artifacts
# ----------------------------------------------------------------------


def apply_artifact_to_record(record: ArtifactRecord, artifact: ArtifactMetadata) -> ArtifactRecord:
    """Copy every :class:`ArtifactMetadata` field onto ``record`` (in place) and return it."""
    record.artifact_id = artifact.artifact_id
    record.urn = str(artifact.urn)
    record.dataset_id = artifact.dataset_id
    record.namespace = artifact.namespace
    record.uri = artifact.uri
    record.format = artifact.format
    record.layer = artifact.layer
    record.execution_id = artifact.execution_id
    record.schema_version = artifact.schema_version
    record.row_count = artifact.row_count
    record.byte_size = artifact.byte_size
    record.checksum = artifact.checksum
    record.tags = list(artifact.tags)
    record.custom_properties = copy.deepcopy(artifact.custom_properties)
    record.created_at = artifact.created_at
    return record


def record_to_artifact(record: ArtifactRecord) -> ArtifactMetadata:
    """Build a validated :class:`ArtifactMetadata` from ``record``."""
    return ArtifactMetadata(
        artifact_id=record.artifact_id,
        urn=record.urn,
        dataset_id=record.dataset_id,
        namespace=record.namespace,
        uri=record.uri,
        format=record.format,
        layer=record.layer,
        execution_id=record.execution_id,
        schema_version=record.schema_version,
        row_count=record.row_count,
        byte_size=record.byte_size,
        checksum=record.checksum,
        tags=list(record.tags),
        custom_properties=copy.deepcopy(record.custom_properties),
        created_at=record.created_at,
    )


__all__ = [
    "apply_artifact_to_record",
    "apply_dataset_to_record",
    "record_to_artifact",
    "record_to_dataset",
    "record_to_schema",
    "schema_to_record",
]
