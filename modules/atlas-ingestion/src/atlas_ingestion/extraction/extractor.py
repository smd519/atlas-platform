"""Extracts data through registered connectors."""

from __future__ import annotations

import os
from collections.abc import Sized
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from atlas_connectors import (
    ConnectorError,
    ConnectorFactory,
    ConnectorRegistry,
    ExtractionResult,
    LocalFileConnector,
    RestConnector,
    SourceConfig,
    parse_csv,
    parse_json,
    parse_parquet,
)
from atlas_core import Connector, SourceId

from atlas_ingestion.exceptions import ExtractionError
from atlas_ingestion.models.ingestion_request import IngestionRequest

_INGESTION_SOURCE_CONFIG_KEYS: frozenset[str] = frozenset(
    {
        "response_mode",
        "download_filename",
        "workspace",
        "workspace_path",
    }
)


def create_default_connector_registry() -> ConnectorRegistry:
    """Create a :class:`ConnectorRegistry` populated with standard connectors."""
    registry = ConnectorRegistry()
    registry.register("local_file", LocalFileConnector)
    registry.register("rest", RestConnector)
    return registry


class Extractor:
    """Resolves configured source connectors and executes data extraction."""

    def __init__(
        self,
        connector_factory: ConnectorFactory | None = None,
        *,
        connector_registry: ConnectorRegistry | None = None,
        workspace: str | Path | None = None,
    ) -> None:
        if connector_factory is not None:
            self._factory = connector_factory
        else:
            registry = connector_registry or create_default_connector_registry()
            self._factory = ConnectorFactory(registry)
        self._workspace = workspace
        self._staged_directories: dict[str, Path] = {}
        self._staged_files: dict[str, Path] = {}

    def extract(
        self,
        request: IngestionRequest,
        *,
        execution_id: str | None = None,
        destination: Path | None = None,
    ) -> ExtractionResult:
        """Execute extraction for ``request`` and return an :class:`ExtractionResult`."""
        exec_id = (execution_id or request.request_id).strip()
        connector = self._resolve_connector(request)
        self._validate_connection_if_configured(request, connector)

        staging_dest = self.resolve_staging_destination(
            request,
            execution_id=exec_id,
            destination=destination,
        )
        if staging_dest is not None:
            try:
                staging_dest.parent.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise ExtractionError(
                    f"Failed to create staging directory '{staging_dest.parent}' "
                    f"for execution '{exec_id}': {exc}",
                    request_id=request.request_id,
                    execution_id=exec_id,
                ) from exc
            self._staged_directories[exec_id] = staging_dest.parent
            self._staged_files[exec_id] = staging_dest

        raw_data = self._execute_connector(
            request,
            connector,
            destination=staging_dest,
        )
        return self._normalize_extraction_result(request, raw_data)

    def get_staging_directory(self, execution_id: str) -> Path | None:
        """Return the execution-specific staging directory created for ``execution_id``."""
        staged_dirs: dict[str, Path] = getattr(self, "_staged_directories", {})
        return staged_dirs.get(execution_id)

    def get_staged_file(self, execution_id: str) -> Path | None:
        """Return the staged file path used for ``execution_id``, if any."""
        staged_files: dict[str, Path] = getattr(self, "_staged_files", {})
        return staged_files.get(execution_id)

    def resolve_staging_directory(
        self,
        request: IngestionRequest,
        execution_id: str,
    ) -> Path:
        """Resolve ``<workspace>/drop/.staging/<execution_id>`` without creating it."""
        clean_exec_id = execution_id.strip() if isinstance(execution_id, str) else ""
        if (
            not clean_exec_id
            or "/" in clean_exec_id
            or "\\" in clean_exec_id
            or clean_exec_id in {".", ".."}
            or ".." in clean_exec_id
        ):
            raise ExtractionError(
                f"Invalid execution_id '{execution_id}' for staging directory resolution.",
                request_id=request.request_id,
            )
        workspace_root = self._resolve_workspace_root(request)
        staging_root = (workspace_root / "drop" / ".staging").resolve()
        candidate = (staging_root / clean_exec_id).resolve()
        if not candidate.is_relative_to(staging_root) or candidate == staging_root:
            raise ExtractionError(
                f"Staging directory for execution '{clean_exec_id}' escaped staging root.",
                request_id=request.request_id,
            )
        return candidate

    def resolve_staging_destination(
        self,
        request: IngestionRequest,
        *,
        execution_id: str | None = None,
        destination: Path | None = None,
    ) -> Path | None:
        """Return the staging file :class:`Path` when remote file staging is required."""
        if destination is not None:
            return destination if isinstance(destination, Path) else Path(destination)

        kw_dest = request.execution_config.extract_kwargs.get("destination")
        if kw_dest is not None:
            return kw_dest if isinstance(kw_dest, Path) else Path(str(kw_dest))

        if not self._requires_staging_destination(request):
            return None

        exec_id = (execution_id or request.request_id).strip()
        staging_dir = self.resolve_staging_directory(request, exec_id)
        filename = self._resolve_download_filename(request)
        resolved_file = (staging_dir / filename).resolve()
        if not resolved_file.is_relative_to(staging_dir) or resolved_file == staging_dir:
            raise ExtractionError(
                f"Invalid download filename '{filename}': path escapes staging directory.",
                request_id=request.request_id,
                execution_id=exec_id,
            )
        return resolved_file

    def build_source_metadata(
        self,
        request: IngestionRequest,
        extraction_result: ExtractionResult,
        *,
        execution_id: str | None = None,
    ) -> dict[str, Any]:
        """Build preserved source metadata for tracking and result reporting."""
        meta: dict[str, Any] = {
            "source_id": request.source.source_id,
            "source_name": request.source.source_name,
            "source_type": request.source.source_type,
            "source_format": request.source_format,
            "extracted_at": extraction_result.extracted_at.isoformat(),
            "extracted_row_count": extraction_result.row_count,
        }
        exec_id = (execution_id or request.request_id).strip()
        staged_file = self.get_staged_file(exec_id)
        if staged_file is not None:
            meta["staging_path"] = str(staged_file)
        return meta

    @staticmethod
    def _requires_staging_destination(request: IngestionRequest) -> bool:
        source_type = request.source.source_type.strip().lower()
        if source_type == "local_file":
            return False

        raw_mode = request.source.config.get("response_mode")
        response_mode = str(raw_mode).strip().lower() if raw_mode is not None else ""
        if response_mode == "json":
            return False
        if response_mode == "file" or "download_filename" in request.source.config:
            return True
        if source_type == "rest" and request.source_format in {"csv", "parquet", "raw", "bytes"}:
            return True
        return False

    @staticmethod
    def _resolve_download_filename(request: IngestionRequest) -> str:
        raw_filename = request.source.config.get("download_filename")
        if raw_filename is not None:
            clean_name = str(raw_filename).strip()
            if (
                not clean_name
                or "/" in clean_name
                or "\\" in clean_name
                or clean_name in {".", ".."}
                or ".." in clean_name
            ):
                raise ExtractionError(
                    f"Invalid download_filename '{raw_filename}' for source "
                    f"'{request.source.source_id}'.",
                    request_id=request.request_id,
                )
            return clean_name

        raw_endpoint = request.source.config.get("endpoint")
        if isinstance(raw_endpoint, str) and raw_endpoint.strip():
            ep_clean = raw_endpoint.strip().split("?", 1)[0].rstrip("/")
            basename = PurePosixPath(ep_clean).name
            if basename and basename not in {".", ".."} and "." in basename:
                return basename

        ext = "bin" if request.source_format in {"raw", "bytes"} else request.source_format
        return f"{request.source.source_id}.{ext}"

    def _resolve_workspace_root(self, request: IngestionRequest) -> Path:
        explicit_ws = (
            request.execution_config.workspace
            or request.metadata.get("workspace")
            or request.metadata.get("workspace_path")
            or request.source.config.get("workspace")
            or request.source.config.get("workspace_path")
            or self._workspace
        )
        atlas_ws_env = os.environ.get("ATLAS_WORKSPACE", "").strip()

        if explicit_ws is not None:
            ws_str = str(explicit_ws).strip()
            if ws_str:
                ws_path = Path(ws_str).expanduser()
                if ws_path.is_absolute():
                    return ws_path.resolve()
                if atlas_ws_env:
                    return (Path(atlas_ws_env).expanduser().resolve() / ws_path).resolve()
                return ws_path.resolve()

        dest_path = Path(request.destination.path).expanduser()

        if atlas_ws_env:
            env_root = Path(atlas_ws_env).expanduser().resolve()
            if dest_path.is_absolute():
                resolved_dest = dest_path.resolve()
                if resolved_dest.is_relative_to(env_root):
                    rel_parts = resolved_dest.relative_to(env_root).parts
                    if len(rel_parts) >= 2 and rel_parts[0].lower() not in {
                        "data",
                        "bronze",
                        "raw",
                        "silver",
                        "gold",
                        "drop",
                    }:
                        return (env_root / rel_parts[0]).resolve()
            return env_root

        if dest_path.is_absolute():
            resolved_dest = dest_path.resolve()
            for ancestor in resolved_dest.parents:
                if ancestor.name.lower() in {"bronze", "raw"}:
                    parent = ancestor.parent
                    if parent.name.lower() == "data" and parent.parent != parent:
                        return parent.parent.resolve()
                    return parent.resolve()
            return resolved_dest.parent.resolve()

        return Path.cwd().resolve()

    def _resolve_connector(self, request: IngestionRequest) -> Connector:
        connector_cfg = {
            k: v for k, v in request.source.config.items() if k not in _INGESTION_SOURCE_CONFIG_KEYS
        }
        sanitized_source = SourceConfig(
            source_id=SourceId(request.source.source_id),
            source_name=request.source.source_name,
            source_type=request.source.source_type,
            config=connector_cfg,
        )
        try:
            return self._factory.from_source_config(sanitized_source)
        except Exception as exc:
            raise ExtractionError(
                f"Failed to resolve or initialize connector '{request.source.source_type}' "
                f"for source '{request.source.source_id}': {exc}",
                request_id=request.request_id,
            ) from exc

    def _validate_connection_if_configured(
        self,
        request: IngestionRequest,
        connector: Connector,
    ) -> None:
        if not request.execution_config.validate_connection:
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
        connector: Connector,
        *,
        destination: Path | None = None,
    ) -> Any:
        extract_kwargs = dict(request.execution_config.extract_kwargs)
        extract_kwargs.pop("destination", None)
        try:
            if destination is not None:
                return connector.extract(destination=destination, **extract_kwargs)
            return connector.extract(**extract_kwargs)
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
