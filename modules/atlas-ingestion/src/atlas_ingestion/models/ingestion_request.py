"""Defines ingestion execution requests and parameters."""

from __future__ import annotations

from typing import Any

from atlas_connectors import SourceConfig
from atlas_core import (
    BRONZE,
    DatasetId,
    Source,
    SourceId,
    generate_execution_id,
    require_not_empty,
)
from atlas_storage import StorageLocation
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_ingestion.exceptions import InvalidIngestionRequestError

SUPPORTED_FORMATS: frozenset[str] = frozenset({"csv", "json", "parquet", "raw", "bytes"})
ALLOWED_RAW_LAYERS: frozenset[str] = frozenset({BRONZE, "raw"})
SUPPORTED_RESPONSE_MODES: frozenset[str] = frozenset({"file", "json"})


def _require_non_blank(value: str, field_name: str) -> str:
    stripped = value.strip() if isinstance(value, str) else ""
    try:
        require_not_empty(stripped, field_name)
    except ValueError as exc:
        raise InvalidIngestionRequestError(str(exc)) from exc
    return stripped


class ExecutionConfig(BaseModel):
    """Execution-level flags and parameters for an ingestion run."""

    model_config = ConfigDict(extra="forbid")

    validate_connection: bool = True
    partition_by_date: bool = False
    overwrite: bool = True
    raise_on_error: bool = False
    workspace: str | None = None
    extract_kwargs: dict[str, Any] = Field(default_factory=dict)


class IngestionRequest(BaseModel):
    """Validated specification for a single raw data ingestion execution."""

    model_config = ConfigDict(extra="forbid")

    request_id: str = Field(default_factory=generate_execution_id)
    dataset_id: DatasetId
    source: SourceConfig
    destination: StorageLocation
    source_format: str
    format_options: dict[str, Any] = Field(default_factory=dict)
    execution_config: ExecutionConfig = Field(default_factory=ExecutionConfig)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except InvalidIngestionRequestError:
            raise
        except Exception as exc:
            raise InvalidIngestionRequestError(
                f"Invalid ingestion request configuration: {exc}"
            ) from exc

    @model_validator(mode="after")
    def _validate_fields(self) -> IngestionRequest:
        return self._apply_validation()

    def _apply_validation(self) -> IngestionRequest:
        self.request_id = _require_non_blank(self.request_id, "request_id")
        dataset_str = _require_non_blank(str(self.dataset_id), "dataset_id")
        self.dataset_id = DatasetId(dataset_str)

        source_id = _require_non_blank(self.source.source_id, "source.source_id")
        source_name = _require_non_blank(self.source.source_name, "source.source_name")
        source_type = _require_non_blank(self.source.source_type, "source.source_type")
        source_cfg_dict = dict(self.source.config)

        if "response_mode" in source_cfg_dict and source_cfg_dict["response_mode"] is not None:
            raw_mode = _require_non_blank(
                str(source_cfg_dict["response_mode"]),
                "source.config.response_mode",
            ).lower()
            if raw_mode not in SUPPORTED_RESPONSE_MODES:
                bad_mode = source_cfg_dict["response_mode"]
                raise InvalidIngestionRequestError(
                    f"Unsupported source.config.response_mode '{bad_mode}'. "
                    f"Supported modes: {sorted(SUPPORTED_RESPONSE_MODES)}",
                    request_id=self.request_id,
                )
            source_cfg_dict["response_mode"] = raw_mode

        if (
            "download_filename" in source_cfg_dict
            and source_cfg_dict["download_filename"] is not None
        ):
            raw_filename = _require_non_blank(
                str(source_cfg_dict["download_filename"]),
                "source.config.download_filename",
            )
            if (
                "/" in raw_filename
                or "\\" in raw_filename
                or raw_filename in {".", ".."}
                or ".." in raw_filename
            ):
                raise InvalidIngestionRequestError(
                    f"Invalid source.config.download_filename '{raw_filename}': "
                    "must be a simple filename without path traversal segments.",
                    request_id=self.request_id,
                )
            source_cfg_dict["download_filename"] = raw_filename

        self.source = SourceConfig(
            source_id=SourceId(source_id),
            source_name=source_name,
            source_type=source_type,
            config=source_cfg_dict,
        )

        normalized_source_format = _require_non_blank(self.source_format, "source_format").lower()
        if normalized_source_format not in SUPPORTED_FORMATS:
            raise InvalidIngestionRequestError(
                f"Unsupported source_format '{self.source_format}'. "
                f"Supported formats: {sorted(SUPPORTED_FORMATS)}",
                request_id=self.request_id,
            )
        self.source_format = normalized_source_format

        dest_layer = _require_non_blank(self.destination.layer, "destination.layer").lower()
        if dest_layer not in ALLOWED_RAW_LAYERS:
            raise InvalidIngestionRequestError(
                f"Invalid destination layer '{self.destination.layer}'. "
                f"Ingestion only persists to raw layers: {sorted(ALLOWED_RAW_LAYERS)}",
                request_id=self.request_id,
            )

        dest_path = _require_non_blank(self.destination.path, "destination.path")
        dest_format = _require_non_blank(self.destination.format, "destination.format").lower()
        if dest_format not in SUPPORTED_FORMATS:
            raise InvalidIngestionRequestError(
                f"Unsupported destination format '{self.destination.format}'. "
                f"Supported formats: {sorted(SUPPORTED_FORMATS)}",
                request_id=self.request_id,
            )

        self.destination = StorageLocation(
            layer=dest_layer,
            path=dest_path,
            format=dest_format,
        )
        return self

    def validate_request(self) -> None:
        """Explicitly re-validate the request fields."""
        self._apply_validation()

    def to_core_source(self) -> Source:
        """Convert the request's :class:`SourceConfig` into a core :class:`Source` model."""
        return Source(
            id=SourceId(self.source.source_id),
            name=self.source.source_name,
            source_type=self.source.source_type,
        )


__all__ = [
    "ALLOWED_RAW_LAYERS",
    "ExecutionConfig",
    "IngestionRequest",
    "SUPPORTED_FORMATS",
]
