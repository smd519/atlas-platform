"""Extracts data through registered connectors."""

from __future__ import annotations

from collections.abc import Sized
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from atlas_connectors.exceptions import ConnectorError
from atlas_connectors.factory import ConnectorFactory
from atlas_connectors.files.local import LocalFileConnector
from atlas_connectors.http.rest import RestConnector
from atlas_connectors.models.extraction_result import ExtractionResult
from atlas_connectors.parsers.csv import parse_csv
from atlas_connectors.parsers.json import parse_json
from atlas_connectors.parsers.parquet import parse_parquet
from atlas_connectors.registry import ConnectorRegistry
from atlas_core.contracts.connector import Connector

from atlas_ingestion.exceptions import ExtractionError
from atlas_ingestion.models.ingestion_request import IngestionRequest


def create_default_connector_registry() -> ConnectorRegistry:
    """Create a :class:`ConnectorRegistry` populated with standard connectors."""
    registry = ConnectorRegistry()
    registry.register("local_file", LocalFileConnector)
    registry.register("rest", RestConnector)  # type: ignore[arg-type]
    return registry


class Extractor:
    """Resolves configured source connectors and executes data extraction."""

    def __init__(
        self,
        connector_factory: ConnectorFactory | None = None,
        *,
        connector_registry: ConnectorRegistry | None = None,
    ) -> None:
        if connector_factory is not None:
            self._factory = connector_factory
        else:
            registry = connector_registry or create_default_connector_registry()
            self._factory = ConnectorFactory(registry)

    def extract(self, request: IngestionRequest) -> ExtractionResult:
        """Execute extraction for ``request`` and return an :class:`ExtractionResult`."""
        connector = self._resolve_connector(request)
        self._validate_connection_if_configured(request, connector)
        raw_data = self._execute_connector(request, connector)
        return self._normalize_extraction_result(request, raw_data)

    def build_source_metadata(
        self,
        request: IngestionRequest,
        extraction_result: ExtractionResult,
    ) -> dict[str, Any]:
        """Build preserved source metadata for tracking and result reporting."""
        return {
            "source_id": request.source.source_id,
            "source_name": request.source.source_name,
            "source_type": request.source.source_type,
            "source_format": request.source_format,
            "extracted_at": extraction_result.extracted_at.isoformat(),
            "extracted_row_count": extraction_result.row_count,
        }

    def _resolve_connector(self, request: IngestionRequest) -> Connector | Any:
        source_cfg = request.source
        init_kwargs = dict(source_cfg.config)
        if source_cfg.source_type == "rest":
            init_kwargs.pop("endpoint", None)

        try:
            return self._factory.create(source_cfg.source_type, **init_kwargs)
        except Exception as exc:
            raise ExtractionError(
                f"Failed to resolve or initialize connector '{source_cfg.source_type}' "
                f"for source '{source_cfg.source_id}': {exc}",
                request_id=request.request_id,
            ) from exc

    def _validate_connection_if_configured(
        self,
        request: IngestionRequest,
        connector: Connector | Any,
    ) -> None:
        if not request.execution_config.validate_connection:
            return
        if not hasattr(connector, "validate_connection"):
            return

        try:
            is_valid = connector.validate_connection()
        except Exception as exc:
            raise ExtractionError(
                f"Connection validation raised an error for source "
                f"'{request.source.source_id}': {exc}",
                request_id=request.request_id,
            ) from exc

        if not is_valid:
            raise ExtractionError(
                f"Connection validation failed for source '{request.source.source_id}' "
                f"({request.source.source_name}).",
                request_id=request.request_id,
            )

    def _execute_connector(
        self,
        request: IngestionRequest,
        connector: Connector | Any,
    ) -> Any:
        extract_kwargs = dict(request.execution_config.extract_kwargs)
        try:
            if hasattr(connector, "extract"):
                return connector.extract(**extract_kwargs)
            if hasattr(connector, "get"):
                endpoint = str(
                    extract_kwargs.get(
                        "endpoint",
                        request.format_options.get(
                            "endpoint",
                            request.source.config.get("endpoint", ""),
                        ),
                    )
                )
                return connector.get(endpoint)
        except ExtractionError:
            raise
        except ConnectorError as exc:
            raise ExtractionError(
                f"Connector error while extracting from source '{request.source.source_id}': {exc}",
                request_id=request.request_id,
            ) from exc
        except Exception as exc:
            raise ExtractionError(
                f"Unexpected failure during extraction from source "
                f"'{request.source.source_id}': {exc}",
                request_id=request.request_id,
            ) from exc

        raise ExtractionError(
            f"Connector for source '{request.source.source_id}' does not expose a "
            "supported extraction method.",
            request_id=request.request_id,
        )

    def _normalize_extraction_result(
        self,
        request: IngestionRequest,
        raw_output: Any,
    ) -> ExtractionResult:
        if isinstance(raw_output, ExtractionResult):
            return raw_output

        try:
            data = (
                self._parse_path_output(raw_output, request.source_format)
                if isinstance(raw_output, Path)
                else raw_output
            )
            if data is None:
                raise ValueError("Extracted data payload cannot be None.")

            row_count = self._compute_row_count(data)
            return ExtractionResult(
                source_id=request.source.source_id,
                row_count=row_count,
                extracted_at=datetime.now(UTC),
                data=data,
            )
        except ExtractionError:
            raise
        except Exception as exc:
            raise ExtractionError(
                f"Failed to parse or normalize extracted payload from source "
                f"'{request.source.source_id}' as '{request.source_format}': {exc}",
                request_id=request.request_id,
            ) from exc

    @staticmethod
    def _parse_path_output(path: Path, source_format: str) -> Any:
        fmt = source_format.lower()
        if fmt == "csv":
            return parse_csv(str(path))
        if fmt == "json":
            return parse_json(str(path))
        if fmt == "parquet":
            return parse_parquet(str(path))
        if fmt in {"raw", "bytes"}:
            return path.read_bytes()
        raise ValueError(f"Unsupported source format '{source_format}' for file extraction.")

    @staticmethod
    def _compute_row_count(data: Any) -> int:
        if isinstance(data, (bytes, bytearray, str)):
            return 1 if len(data) > 0 else 0
        if isinstance(data, Sized):
            return len(data)
        return 1


__all__ = [
    "Extractor",
    "create_default_connector_registry",
]
