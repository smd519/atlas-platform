"""Persists extracted data through storage interfaces."""

from __future__ import annotations

import io
import json
from datetime import datetime
from pathlib import PurePath
from typing import Any

import pandas as pd  # type: ignore[import-untyped]
from atlas_connectors.models.extraction_result import ExtractionResult
from atlas_core.contracts.storage import StorageProvider
from atlas_core.identifiers import generate_execution_id
from atlas_core.models.artifact import Artifact
from atlas_storage.backends.filesystem import FileSystemBackend
from atlas_storage.exceptions import StorageError
from atlas_storage.manager import StorageManager
from atlas_storage.models.storage_location import StorageLocation
from atlas_storage.partitioning.strategy import PartitionStrategy
from atlas_storage.registry import StorageRegistry

from atlas_ingestion.exceptions import LoadingError
from atlas_ingestion.models.ingestion_request import (
    ALLOWED_RAW_LAYERS,
    SUPPORTED_FORMATS,
    IngestionRequest,
)


def create_default_storage_registry() -> StorageRegistry:
    """Create a :class:`StorageRegistry` populated with standard storage backends."""
    registry = StorageRegistry()
    registry.register("filesystem", FileSystemBackend)
    return registry


class Loader:
    """Persists extracted datasets into the raw storage layer via :class:`StorageManager`."""

    def __init__(
        self,
        storage_manager: StorageManager | None = None,
        *,
        storage_provider: StorageProvider | None = None,
        storage_registry: StorageRegistry | None = None,
        backend_name: str = "filesystem",
    ) -> None:
        if storage_manager is not None:
            self._storage_manager = storage_manager
        elif storage_provider is not None:
            self._storage_manager = StorageManager(backend=storage_provider)
        else:
            registry = storage_registry or create_default_storage_registry()
            try:
                backend_cls = registry.get(backend_name)
                backend = backend_cls()
            except Exception as exc:
                raise LoadingError(
                    f"Failed to resolve storage backend '{backend_name}': {exc}"
                ) from exc
            self._storage_manager = StorageManager(backend=backend)

    def load(
        self,
        request: IngestionRequest,
        extraction_result: ExtractionResult,
        *,
        execution_timestamp: datetime | None = None,
    ) -> list[Artifact]:
        """Persist ``extraction_result`` into raw storage and return :class:`Artifact` list."""
        target_location = self._resolve_target_location(
            request=request,
            partition_timestamp=execution_timestamp or extraction_result.extracted_at,
        )

        try:
            if not request.execution_config.overwrite and self._storage_manager.exists(
                target_location
            ):
                raise LoadingError(
                    f"Destination path '{target_location.path}' already exists and "
                    "overwrite is disabled.",
                    request_id=request.request_id,
                )

            payload_bytes = self._serialize_payload(
                data=extraction_result.data,
                target_format=target_location.format,
            )
            storage_result = self._storage_manager.save(
                data=payload_bytes,
                location=target_location,
            )
        except LoadingError:
            raise
        except StorageError as exc:
            raise LoadingError(
                f"Storage error while persisting dataset '{request.dataset_id}' "
                f"to '{target_location.path}': {exc}",
                request_id=request.request_id,
            ) from exc
        except Exception as exc:
            raise LoadingError(
                f"Failed to persist dataset '{request.dataset_id}' "
                f"to '{target_location.path}': {exc}",
                request_id=request.request_id,
            ) from exc

        if not storage_result.success:
            raise LoadingError(
                storage_result.message
                or f"Storage manager reported unsuccessful save for '{target_location.path}'.",
                request_id=request.request_id,
            )

        artifact = Artifact(
            id=generate_execution_id(),
            dataset_id=str(request.dataset_id),
            path=storage_result.path,
            format=target_location.format,
        )
        return [artifact]

    @staticmethod
    def _resolve_target_location(
        request: IngestionRequest,
        partition_timestamp: datetime,
    ) -> StorageLocation:
        dest = request.destination
        layer = dest.layer.strip().lower()
        if layer not in ALLOWED_RAW_LAYERS:
            raise LoadingError(
                f"Loader only persists to raw layers ({sorted(ALLOWED_RAW_LAYERS)}), "
                f"got '{dest.layer}'.",
                request_id=request.request_id,
            )

        fmt = dest.format.strip().lower()
        if fmt not in SUPPORTED_FORMATS:
            raise LoadingError(
                f"Unsupported destination storage format '{dest.format}'.",
                request_id=request.request_id,
            )

        target_path = dest.path
        if request.execution_config.partition_by_date:
            partition_segment = PartitionStrategy.by_date(partition_timestamp)
            pure = PurePath(dest.path)
            if pure.suffix:
                target_path = str(pure.parent / partition_segment / pure.name)
            else:
                target_path = str(pure / partition_segment)

        return StorageLocation(
            layer=layer,
            path=target_path,
            format=fmt,
        )

    @classmethod
    def _serialize_payload(cls, data: Any, target_format: str) -> bytes:
        if data is None:
            raise ValueError("Cannot persist None payload to storage.")

        fmt = target_format.lower()
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)

        if fmt in {"raw", "bytes"}:
            if isinstance(data, str):
                return data.encode("utf-8")
            return json.dumps(data).encode("utf-8")

        dataframe = cls._to_dataframe(data)

        if fmt == "csv":
            csv_text = dataframe.to_csv(index=False)
            return csv_text.encode("utf-8")

        if fmt == "json":
            json_text = dataframe.to_json(orient="records")
            return (json_text or "[]").encode("utf-8")

        if fmt == "parquet":
            buffer = io.BytesIO()
            dataframe.to_parquet(buffer, index=False)
            return buffer.getvalue()

        raise ValueError(f"Unsupported storage serialization format '{target_format}'.")

    @staticmethod
    def _to_dataframe(data: Any) -> pd.DataFrame:
        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict):
            return pd.DataFrame([data])
        raise TypeError(
            f"Cannot convert extracted payload of type '{type(data).__name__}' to DataFrame."
        )


__all__ = [
    "Loader",
    "create_default_storage_registry",
]
