"""Defines processing execution requests and parameters."""

from __future__ import annotations

from typing import Any

from atlas_core import (
    BRONZE,
    GOLD,
    SILVER,
    DatasetId,
    generate_execution_id,
    require_not_empty,
)
from atlas_storage import StorageLocation
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_processing.exceptions import InvalidProcessingRequestError
from atlas_processing.transformations.base import TransformationStep

ALLOWED_SOURCE_LAYERS: frozenset[str] = frozenset({BRONZE, SILVER, "raw", "curated"})
ALLOWED_DESTINATION_LAYERS: frozenset[str] = frozenset({SILVER, GOLD, "curated"})
SUPPORTED_PROCESSING_FORMATS: frozenset[str] = frozenset({"csv", "json", "parquet"})


def _require_non_blank(value: str, field_name: str) -> str:
    stripped = value.strip() if isinstance(value, str) else ""
    try:
        require_not_empty(stripped, field_name)
    except ValueError as exc:
        raise InvalidProcessingRequestError(str(exc)) from exc
    return stripped


class ProcessingExecutionConfig(BaseModel):
    """Execution-level flags and parameters for a processing run."""

    model_config = ConfigDict(extra="forbid")

    overwrite: bool = True
    partition_by_date: bool = False
    fail_on_empty_input: bool = False
    raise_on_error: bool = False


class ProcessingRequest(BaseModel):
    """Validated specification for a dataset transformation run."""

    model_config = ConfigDict(extra="forbid")

    request_id: str = Field(default_factory=generate_execution_id)
    dataset_id: DatasetId
    source: StorageLocation
    destination: StorageLocation
    transformations: list[TransformationStep] = Field(default_factory=list)
    execution_config: ProcessingExecutionConfig = Field(default_factory=ProcessingExecutionConfig)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except InvalidProcessingRequestError:
            raise
        except Exception as exc:
            raise InvalidProcessingRequestError(
                f"Invalid processing request configuration: {exc}"
            ) from exc

    @model_validator(mode="after")
    def _validate_fields(self) -> ProcessingRequest:
        return self._apply_validation()

    def _apply_validation(self) -> ProcessingRequest:
        self.request_id = _require_non_blank(self.request_id, "request_id")
        dataset_str = _require_non_blank(str(self.dataset_id), "dataset_id")
        self.dataset_id = DatasetId(dataset_str)

        src_layer = _require_non_blank(self.source.layer, "source.layer").lower()
        if src_layer not in ALLOWED_SOURCE_LAYERS:
            raise InvalidProcessingRequestError(
                f"Invalid source layer '{self.source.layer}'. "
                f"Allowed source layers: {sorted(ALLOWED_SOURCE_LAYERS)}",
                request_id=self.request_id,
            )
        src_path = _require_non_blank(self.source.path, "source.path")
        src_fmt = _require_non_blank(self.source.format, "source.format").lower()
        if src_fmt not in SUPPORTED_PROCESSING_FORMATS:
            raise InvalidProcessingRequestError(
                f"Unsupported source format '{self.source.format}'. "
                f"Supported formats: {sorted(SUPPORTED_PROCESSING_FORMATS)}",
                request_id=self.request_id,
            )
        self.source = StorageLocation(layer=src_layer, path=src_path, format=src_fmt)

        dest_layer = _require_non_blank(self.destination.layer, "destination.layer").lower()
        if dest_layer not in ALLOWED_DESTINATION_LAYERS:
            allowed_dest = sorted(ALLOWED_DESTINATION_LAYERS)
            raise InvalidProcessingRequestError(
                f"Invalid destination layer '{self.destination.layer}'. "
                f"Processing outputs must target curated layers: {allowed_dest}",
                request_id=self.request_id,
            )
        dest_path = _require_non_blank(self.destination.path, "destination.path")
        dest_fmt = _require_non_blank(self.destination.format, "destination.format").lower()
        if dest_fmt not in SUPPORTED_PROCESSING_FORMATS:
            raise InvalidProcessingRequestError(
                f"Unsupported destination format '{self.destination.format}'. "
                f"Supported formats: {sorted(SUPPORTED_PROCESSING_FORMATS)}",
                request_id=self.request_id,
            )
        self.destination = StorageLocation(
            layer=dest_layer,
            path=dest_path,
            format=dest_fmt,
        )

        if self.source.path == self.destination.path:
            raise InvalidProcessingRequestError(
                "Source and destination paths must be distinct.",
                request_id=self.request_id,
            )

        return self

    def validate_request(self) -> None:
        """Explicitly re-validate the processing request."""
        self._apply_validation()


__all__ = [
    "ALLOWED_DESTINATION_LAYERS",
    "ALLOWED_SOURCE_LAYERS",
    "ProcessingExecutionConfig",
    "ProcessingRequest",
    "SUPPORTED_PROCESSING_FORMATS",
]
