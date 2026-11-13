"""Coordinates ingestion extraction, loading, metadata registration, and tracking."""

from __future__ import annotations

import inspect
import shutil
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from atlas_core import BRONZE, Artifact, ExecutionStatus, MetadataProvider, get_logger
from atlas_metadata import ArtifactMetadata, DatasetMetadata

from atlas_ingestion.exceptions import (
    ExecutionTrackingError,
    IngestionError,
    InvalidIngestionRequestError,
)
from atlas_ingestion.extraction.extractor import Extractor
from atlas_ingestion.lifecycle import IngestionState
from atlas_ingestion.loading.loader import Loader
from atlas_ingestion.models.ingestion_request import IngestionRequest
from atlas_ingestion.models.ingestion_result import IngestionResult
from atlas_ingestion.tracking.execution_tracker import (
    ExecutionTracker,
    IngestionExecutionRecord,
)


def _accepts_keyword_arg(fn: Callable[..., Any], keyword: str) -> bool:
    """Return True if ``fn`` accepts ``keyword`` or ``**kwargs``."""
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    for param in sig.parameters.values():
        if param.name == keyword and param.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            return True
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            return True
    return False


def _extract_request_tags(request: IngestionRequest) -> list[str]:
    """Collect and deduplicate tags from ``request.tags`` and ``request.metadata['tags']``."""
    raw_tags: list[str] = []
    if request.tags:
        raw_tags.extend(str(tag).strip() for tag in request.tags if str(tag).strip())
    meta_tags = request.metadata.get("tags")
    if isinstance(meta_tags, (list, tuple)):
        raw_tags.extend(str(tag).strip() for tag in meta_tags if str(tag).strip())
    return list(dict.fromkeys(raw_tags))


class IngestionPipeline:
    """Coordinates validation, extraction, raw layer loading, staging cleanup, and tracking.

    Staging & Cleanup Policy
    ------------------------
    - Remote file downloads are staged inside ``<workspace>/drop/.staging/<execution_id>/``.
    - Staging files are never deleted before Bronze persistence (``Loader.load``) succeeds.
    - On successful Bronze persistence, only the current execution's staging directory
      ``<workspace>/drop/.staging/<execution_id>/`` is removed. Cleanup errors are logged
      and never mask or overwrite a successful ``IngestionResult``.
    - On failed ingestion, any completed staging file in ``drop/.staging/<execution_id>/``
      is preserved for diagnostics (and recorded in ``metadata["staging_path"]``), while
      incomplete ``.part`` artifacts are removed.
    - Local files read via ``LocalFileConnector`` are never copied, moved, or deleted.
    """

    def __init__(
        self,
        extractor: Extractor | None = None,
        loader: Loader | None = None,
        tracker: ExecutionTracker | None = None,
        *,
        metadata_provider: MetadataProvider | None = None,
        workspace: str | Path | None = None,
        rmtree_fn: Callable[[Path], Any] | None = None,
    ) -> None:
        self._extractor = extractor or Extractor(workspace=workspace)
        self._loader = loader or Loader()
        self._tracker = tracker or ExecutionTracker()
        self._metadata = metadata_provider
        self._rmtree_fn = rmtree_fn or shutil.rmtree
        self._logger = get_logger("atlas.ingestion.pipeline")

    @property
    def tracker(self) -> ExecutionTracker:
        """Return the pipeline's :class:`ExecutionTracker`."""
        return self._tracker

    def run(
        self,
        request: IngestionRequest,
        *,
        execution_id: str | None = None,
        raise_on_error: bool | None = None,
    ) -> IngestionResult:
        """Execute the full ingestion lifecycle for ``request``."""
        self._validate_request(request)
        should_raise = (
            raise_on_error
            if raise_on_error is not None
            else request.execution_config.raise_on_error
        )

        record = self._tracker.create_execution(request, execution_id=execution_id)
        exec_id = record.id
        combined_metadata: dict[str, Any] = dict(request.metadata)

        self._logger.info(
            "ingestion_execution_created",
            request_id=request.request_id,
            execution_id=exec_id,
            dataset_id=str(request.dataset_id),
            source_id=request.source.source_id,
        )

        try:
            self._tracker.start(exec_id)

            self._tracker.record_transition(exec_id, IngestionState.EXTRACTING)
            if _accepts_keyword_arg(self._extractor.extract, "execution_id"):
                extraction_result = self._extractor.extract(request, execution_id=exec_id)
            else:
                extraction_result = self._extractor.extract(request)

            if _accepts_keyword_arg(self._extractor.build_source_metadata, "execution_id"):
                source_meta = self._extractor.build_source_metadata(
                    request,
                    extraction_result,
                    execution_id=exec_id,
                )
            else:
                source_meta = self._extractor.build_source_metadata(request, extraction_result)
            combined_metadata.update(source_meta)
            self._tracker.record_transition(exec_id, IngestionState.EXTRACTED)

            self._tracker.record_transition(exec_id, IngestionState.LOADING)
            artifacts = self._loader.load(
                request,
                extraction_result,
                execution_timestamp=record.started_at,
            )

            if self._metadata is not None:
                self._register_metadata(
                    request=request,
                    artifacts=artifacts,
                    execution_id=exec_id,
                    row_count=extraction_result.row_count,
                )

            self._tracker.complete(
                exec_id,
                row_count=extraction_result.row_count,
                artifact_ids=[artifact.id for artifact in artifacts],
            )
            completed_record = self._tracker.get_execution(exec_id)

            self._cleanup_staging_on_success(
                execution_id=exec_id,
                request_id=request.request_id,
                metadata=combined_metadata,
            )

            self._logger.info(
                "ingestion_execution_succeeded",
                request_id=request.request_id,
                execution_id=exec_id,
                row_count=extraction_result.row_count,
                artifact_count=len(artifacts),
            )

            return IngestionResult(
                request_id=request.request_id,
                execution_id=exec_id,
                status=ExecutionStatus.SUCCEEDED,
                lifecycle_state=completed_record.lifecycle_state,
                source=request.to_core_source(),
                dataset_id=request.dataset_id,
                destination=request.destination,
                artifacts=artifacts,
                row_count=extraction_result.row_count,
                started_at=completed_record.started_at,
                completed_at=completed_record.completed_at,
                duration_seconds=completed_record.duration_seconds,
                message="Ingestion completed successfully.",
                metadata=combined_metadata,
            )
        except Exception as exc:
            return self._handle_failure(
                request=request,
                initial_record=record,
                error=exc,
                metadata=combined_metadata,
                should_raise=should_raise,
            )

    def _register_metadata(
        self,
        *,
        request: IngestionRequest,
        artifacts: list[Artifact],
        execution_id: str,
        row_count: int | None,
    ) -> None:
        """Register dataset and physical artifact metadata in the catalog."""
        if self._metadata is None:
            return

        tags = _extract_request_tags(request)
        primary_artifact = artifacts[0] if artifacts else None
        resolved_format = (
            primary_artifact.format if primary_artifact is not None else request.destination.format
        )
        resolved_path = (
            primary_artifact.path if primary_artifact is not None else request.destination.path
        )

        dataset_meta = DatasetMetadata(
            dataset_id=str(request.dataset_id),
            name=str(request.dataset_id),
            layer=BRONZE,
            source_id=str(request.source.source_id),
            format=resolved_format,
            location_uri=resolved_path,
            tags=tags,
        )
        self._metadata.register_dataset_metadata(dataset_meta, overwrite=True)

        target_artifacts = artifacts or [
            Artifact(
                id=execution_id,
                dataset_id=str(request.dataset_id),
                path=request.destination.path,
                format=request.destination.format,
            )
        ]
        for artifact in target_artifacts:
            artifact_meta = ArtifactMetadata(
                artifact_id=artifact.id,
                dataset_id=str(request.dataset_id),
                uri=artifact.path,
                format=artifact.format,
                layer=BRONZE,
                execution_id=execution_id,
                row_count=row_count,
                tags=tags,
            )
            self._metadata.register_artifact(artifact_meta, overwrite=True)

    def _cleanup_staging_on_success(
        self,
        *,
        execution_id: str,
        request_id: str,
        metadata: dict[str, Any],
    ) -> None:
        """Delete only ``<workspace>/drop/.staging/<execution_id>`` after Bronze load succeeds."""
        if not hasattr(self._extractor, "get_staging_directory"):
            return
        staging_dir = self._extractor.get_staging_directory(execution_id)
        if staging_dir is None:
            return

        if staging_dir.name != execution_id or staging_dir.parent.name != ".staging":
            self._logger.warning(
                "ingestion_staging_cleanup_skipped_invalid_boundary",
                request_id=request_id,
                execution_id=execution_id,
                staging_dir=str(staging_dir),
            )
            return

        if not staging_dir.exists():
            return

        try:
            self._rmtree_fn(staging_dir)
            self._logger.info(
                "ingestion_staging_cleaned_up",
                request_id=request_id,
                execution_id=execution_id,
                staging_dir=str(staging_dir),
            )
        except Exception as cleanup_exc:
            self._logger.error(
                "ingestion_staging_cleanup_failed",
                request_id=request_id,
                execution_id=execution_id,
                staging_dir=str(staging_dir),
                error=str(cleanup_exc),
            )
            metadata["staging_cleanup_error"] = str(cleanup_exc)

    def _preserve_or_tidy_staging_on_failure(
        self,
        *,
        execution_id: str,
        metadata: dict[str, Any],
    ) -> None:
        """Remove any leftover ``.part`` files while preserving diagnostic staging artifacts."""
        if not hasattr(self._extractor, "get_staging_directory"):
            return
        staging_dir = self._extractor.get_staging_directory(execution_id)
        if staging_dir is None or not staging_dir.exists():
            return

        try:
            for item in list(staging_dir.iterdir()):
                if item.is_file() and item.name.endswith(".part"):
                    item.unlink(missing_ok=True)
        except OSError:
            pass

        staged_file = (
            self._extractor.get_staged_file(execution_id)
            if hasattr(self._extractor, "get_staged_file")
            else None
        )
        if staged_file is not None and staged_file.exists():
            metadata["staging_path"] = str(staged_file)

    @staticmethod
    def _validate_request(request: IngestionRequest) -> None:
        if not isinstance(request, IngestionRequest):
            raise InvalidIngestionRequestError(
                f"Expected IngestionRequest instance, got {type(request).__name__}."
            )
        request.validate_request()

    def _handle_failure(
        self,
        *,
        request: IngestionRequest,
        initial_record: IngestionExecutionRecord,
        error: Exception,
        metadata: dict[str, Any],
        should_raise: bool,
    ) -> IngestionResult:
        exec_id = initial_record.id
        self._preserve_or_tidy_staging_on_failure(
            execution_id=exec_id,
            metadata=metadata,
        )

        if isinstance(error, IngestionError):
            error.request_id = error.request_id or request.request_id
            error.execution_id = error.execution_id or exec_id
            ingestion_error: IngestionError = error
        else:
            ingestion_error = IngestionError(
                f"Unexpected ingestion error: {error}",
                request_id=request.request_id,
                execution_id=exec_id,
            )
            ingestion_error.__cause__ = error

        try:
            self._tracker.fail(
                exec_id,
                reason=str(ingestion_error),
                error_type=type(ingestion_error).__name__,
            )
            failed_record = self._tracker.get_execution(exec_id)
            completed_at = failed_record.completed_at or datetime.now(UTC)
            duration_seconds = failed_record.duration_seconds
        except ExecutionTrackingError as tracking_exc:
            # Never silently mask the original extraction/loading error when failure tracking fails.
            self._logger.error(
                "ingestion_failure_tracking_error",
                request_id=request.request_id,
                execution_id=exec_id,
                tracking_error=str(tracking_exc),
                original_error=str(ingestion_error),
            )
            metadata["tracking_error"] = str(tracking_exc)
            completed_at = datetime.now(UTC)
            duration_seconds = max(0.0, (completed_at - initial_record.started_at).total_seconds())

        cause = ingestion_error.__cause__
        error_cause = f"{type(cause).__name__}: {cause}" if cause is not None else None

        self._logger.error(
            "ingestion_execution_failed",
            request_id=request.request_id,
            execution_id=exec_id,
            error_type=type(ingestion_error).__name__,
            error_message=str(ingestion_error),
            error_cause=error_cause,
        )

        if should_raise:
            raise ingestion_error

        return IngestionResult(
            request_id=request.request_id,
            execution_id=exec_id,
            status=ExecutionStatus.FAILED,
            lifecycle_state=IngestionState.FAILED,
            source=request.to_core_source(),
            dataset_id=request.dataset_id,
            destination=request.destination,
            artifacts=[],
            row_count=None,
            started_at=initial_record.started_at,
            completed_at=completed_at,
            duration_seconds=duration_seconds,
            error_type=type(ingestion_error).__name__,
            error_message=str(ingestion_error),
            error_cause=error_cause,
            message=str(ingestion_error),
            metadata=metadata,
        )


__all__ = [
    "IngestionPipeline",
]
