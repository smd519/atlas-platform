"""Immutable schema and column metadata domain models."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from atlas_core import require_not_empty
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_metadata.exceptions import (
    InvalidMetadataError,
    InvalidSchemaError,
    InvalidURNError,
)
from atlas_metadata.identifiers.urn import URN
from atlas_metadata.models.dataset import normalize_tags, validate_json_value


class ColumnMetadata(BaseModel):
    """Immutable metadata definition for a single dataset column."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    data_type: str
    nullable: bool = True
    primary_key: bool = False
    description: str | None = None
    default_value: Any = None
    tags: list[str] = Field(default_factory=list)
    custom_properties: dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except (InvalidSchemaError, InvalidMetadataError):
            raise
        except Exception as exc:
            raise InvalidSchemaError(f"Invalid column metadata: {exc}") from exc

    @model_validator(mode="after")
    def _validate_column(self) -> ColumnMetadata:
        clean_name = self.name.strip() if isinstance(self.name, str) else ""
        clean_type = self.data_type.strip() if isinstance(self.data_type, str) else ""
        try:
            require_not_empty(clean_name, "column.name")
            require_not_empty(clean_type, "column.data_type")
        except ValueError as exc:
            raise InvalidSchemaError(str(exc)) from exc

        if self.primary_key and self.nullable and "nullable" not in self.model_fields_set:
            object.__setattr__(self, "nullable", False)

        object.__setattr__(self, "name", clean_name)
        object.__setattr__(self, "data_type", clean_type.lower())
        object.__setattr__(self, "tags", normalize_tags(self.tags))
        object.__setattr__(
            self,
            "default_value",
            validate_json_value(self.default_value, path=f"column({clean_name}).default_value"),
        )
        object.__setattr__(
            self,
            "custom_properties",
            validate_json_value(
                self.custom_properties, path=f"column({clean_name}).custom_properties"
            ),
        )
        return self


class SchemaMetadata(BaseModel):
    """Immutable versioned schema definition associated with a dataset."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_id: str
    urn: URN
    dataset_id: str
    namespace: str = "default"
    version: int = 1
    columns: tuple[ColumnMetadata, ...] = Field(default_factory=tuple)
    schema_format: str = "tabular"
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    custom_properties: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def __init__(self, **data: Any) -> None:
        raw_ds = data.get("dataset_id")
        raw_ver = data.get("version", 1)
        raw_ns = data.get("namespace", "default")
        raw_urn = data.get("urn")

        try:
            if not isinstance(raw_ver, int) or isinstance(raw_ver, bool) or raw_ver < 1:
                raise InvalidSchemaError(
                    f"Schema version must be an integer >= 1, got {raw_ver!r}."
                )
            if raw_urn is not None:
                parsed_urn = URN.parse(raw_urn) if not isinstance(raw_urn, URN) else raw_urn
                data = dict(data)
                data["urn"] = parsed_urn
                if "namespace" not in data:
                    data["namespace"] = parsed_urn.namespace
            elif raw_ds is not None:
                clean_ds = str(raw_ds).strip()
                clean_ns = str(raw_ns).strip() if isinstance(raw_ns, str) else "default"
                data = dict(data)
                data["urn"] = URN.for_schema(clean_ds, raw_ver, namespace=clean_ns)

            if "schema_id" not in data and raw_ds is not None:
                data = dict(data)
                data["schema_id"] = f"{str(raw_ds).strip()}.v{raw_ver}"

            super().__init__(**data)
        except (InvalidSchemaError, InvalidMetadataError, InvalidURNError):
            raise
        except Exception as exc:
            raise InvalidSchemaError(f"Invalid schema metadata: {exc}") from exc

    @model_validator(mode="after")
    def _validate_schema(self) -> SchemaMetadata:
        clean_id = self.schema_id.strip() if isinstance(self.schema_id, str) else ""
        clean_ds = self.dataset_id.strip() if isinstance(self.dataset_id, str) else ""
        clean_ns = self.namespace.strip().lower() if isinstance(self.namespace, str) else ""
        clean_fmt = (
            self.schema_format.strip().lower() if isinstance(self.schema_format, str) else ""
        )
        try:
            require_not_empty(clean_id, "schema_id")
            require_not_empty(clean_ds, "dataset_id")
            require_not_empty(clean_ns, "namespace")
            require_not_empty(clean_fmt, "schema_format")
        except ValueError as exc:
            raise InvalidSchemaError(str(exc), dataset_id=clean_ds, schema_id=clean_id) from exc

        if self.urn.entity_type != "schema":
            raise InvalidURNError(
                f"SchemaMetadata URN must have entity_type 'schema', got '{self.urn.entity_type}'.",
                urn=str(self.urn),
                dataset_id=clean_ds,
                schema_id=clean_id,
            )

        if not self.columns:
            raise InvalidSchemaError(
                "SchemaMetadata requires at least one ColumnMetadata entry.",
                dataset_id=clean_ds,
                schema_id=clean_id,
            )

        seen_cols: set[str] = set()
        for col in self.columns:
            if col.name in seen_cols:
                raise InvalidSchemaError(
                    f"Duplicate column name '{col.name}' in schema '{clean_id}'.",
                    dataset_id=clean_ds,
                    schema_id=clean_id,
                )
            seen_cols.add(col.name)

        object.__setattr__(self, "schema_id", clean_id)
        object.__setattr__(self, "dataset_id", clean_ds)
        object.__setattr__(self, "namespace", clean_ns)
        object.__setattr__(self, "schema_format", clean_fmt)
        object.__setattr__(self, "tags", normalize_tags(self.tags))
        object.__setattr__(
            self,
            "custom_properties",
            validate_json_value(self.custom_properties, path="custom_properties"),
        )
        return self

    @property
    def column_names(self) -> tuple[str, ...]:
        """Return ordered tuple of column names in this schema."""
        return tuple(col.name for col in self.columns)

    def get_column(self, name: str) -> ColumnMetadata | None:
        """Return the :class:`ColumnMetadata` for ``name``, or ``None`` if not present."""
        for col in self.columns:
            if col.name == name:
                return col
        return None


__all__ = [
    "ColumnMetadata",
    "SchemaMetadata",
]
