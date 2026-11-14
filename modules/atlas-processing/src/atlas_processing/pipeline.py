"""Coordinates dataset reading, transformation execution, curated persistence, and tracking."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import PurePath
from typing import Any

from atlas_core import (
    DEFAULT_NAMESPACE,
    Artifact,
    Dataset,
    ExecutionStatus,
    MetadataProvider,
    StorageProvider,
    build_artifact_urn,
    build_dataset_urn,
    generate_artifact_id,
    get_logger,
    is_canonical_urn,
)
from atlas_storage import (
    FileSystemBackend,
    PartitionStrategy,
    StorageError,
    StorageLocation,
    StorageManager,
    StorageRegistry,
)

from atlas_processing.exceptions import (
    InvalidProcessingRequestError,
    ProcessingError,
    ProcessingExecutionError,
    ProcessingLifecycleError,
    ProcessingStorageReadError,
    ProcessingStorageWriteError,
)
from atlas_processing.execution.executor import TransformationExecutor
from atlas_processing.models.processing_request import ProcessingRequest
from atlas_processing.models.processing_result import (
    ProcessingResult,
    ProcessingStatistics,
)
from atlas_processing.tracking.execution_tracker import (
    ExecutionTracker,
    ProcessingExecutionRecord,
    ProcessingState,
)


def create_default_storage_registry() -> StorageRegistry:
    """Create a :class:`StorageRegistry` populated with standard storage backends."""
    registry = StorageRegistry()
    registry.register("filesystem", FileSystemBackend)
    return registry


def _resolve_request_namespace(request: ProcessingRequest) -> str:
    """Resolve the normalized namespace for canonical URN construction.

    Namespace Assumption:
    All datasets and artifacts default to ``DEFAULT_NAMESPACE`` (``"default"``) unless
    explicitly overridden on ``ProcessingRequest.namespace`` or ``request.metadata["namespace"]``.
    Workspace paths and tenant identifiers are not coupled to metadata namespaces.
    """
    meta_ns = request.metadata.get("namespace")
    if isinstance(meta_ns, str) and meta_ns.strip() and request.namespace == DEFAULT_NAMESPACE:
        return meta_ns.strip().lower()
    return (request.namespace or DEFAULT_NAMESPACE).strip().lower() or DEFAULT_NAMESPACE


def _extract_request_tags(request: ProcessingRequest) -> list[str]:
    """Collect and deduplicate tags from ``request.tags`` and ``request.metadata['tags']``."""
    raw_tags: list[str] = []
    if request.tags:
        raw_tags.extend(str(tag).strip() for tag in request.tags if str(tag).strip())
    meta_tags = request.metadata.get("tags")
    if isinstance(meta_tags, (list, tuple)):
        raw_tags.extend(str(tag).strip() for tag in meta_tags if str(tag).strip())
    return list(dict.fromkeys(raw_tags))


def _resolve_source_dataset_id(request: ProcessingRequest) -> str:
    """Resolve the primary upstream source dataset identifier from ``request``."""
    if request.source_dataset_id is not None and str(request.source_dataset_id).strip():
        return str(request.source_dataset_id).strip()
    for key in ("source_dataset_id", "upstream_dataset_id"):
        val = request.metadata.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    source_stem = PurePath(request.source.path).stem.strip()
    if source_stem:
        return source_stem
    return str(request.dataset_id).strip()


class ProcessingPipeline:
    """Coordinates validation, storage reads, transformations, curated writes, and tracking.

    Persistence, Lineage, Namespace, & Artifact History Semantics
    -------------------------------------------------------------
    1. **Non-Transactional Boundary**:
       Physical storage persistence (`StorageManager.save`) and optional metadata catalog
       registration (`MetadataProvider`) are **not transactional**. Once the output dataset
       is written to curated (`silver` / `gold`) storage, dataset and artifact metadata
       registrations are attempted independently; failures log actionable warnings without
       marking the processing execution as ``FAILED``.
    2. **Lineage Normalization**:
       Both ``source_dataset_id`` (or inferred source dataset ID) and ``upstream_urns``
       are combined, resolved to canonical ``urn:atlas:dataset:<namespace>:<id>`` URNs, and
       deduplicated in declaration order without losing any explicitly declared upstream
       lineage.
    3. **Namespace Assumption**:
       Fallback URN construction uses ``atlas_core.build_dataset_urn`` / ``build_artifact_urn``
       with ``DEFAULT_NAMESPACE = "default"`` unless ``request.namespace`` is specified or an
       existing registered dataset provides its own namespace.
    4. **Artifact History Preservation**:
       Dataset registration uses ``overwrite=True`` to update the current catalog record for
       ``dataset_id``. Artifact registration generates a unique ``artifact_id`` (and distinct
       ``urn:atlas:artifact:<namespace>:<artifact_id>``) per execution, ensuring ``overwrite=True``
       is idempotent for that execution's artifact and never overwrites historical artifacts from
       previous executions of the same dataset.
    """

    def __init__(
        self,
        executor: TransformationExecutor | None = None,
        storage_manager: StorageManager | None = None,
        tracker: ExecutionTracker | None = None,
        *,
        metadata_provider: MetadataProvider | None = None,
        storage_provider: StorageProvider | None = None,
        storage_registry: StorageRegistry | None = None,
        backend_name: str = "filesystem",
    ) -> None:
        self._executor = executor or TransformationExecutor()
        self._tracker = tracker or ExecutionTracker()
        self._metadata = metadata_provider
        self._logger = get_logger("atlas.processing.pipeline")
        self._stdlib_logger = logging.getLogger("atlas.processing.pipeline")

        if storage_manager is not None:
            self._storage_manager = storage_manager
        elif storage_provider is not None:
            self._storage_manager = StorageManager(backend=storage_provider)
        else:
            registry = storage_registry or create_default_storage_registry()
            backend_cls = registry.get(backend_name)
            self._storage_manager = StorageManager(backend=backend_cls())

    @property
    def tracker(self) -> ExecutionTracker:
        """Return the pipeline's :class:`ExecutionTracker`."""
        return self._tracker

    @property
    def executor(self) -> TransformationExecutor:
        """Return the pipeline's :class:`TransformationExecutor`."""
        return self._executor

    @property
    def metadata_provider(self) -> MetadataProvider | None:
        """Return the optional :class:`MetadataProvider` configured on this pipeline."""
        return self._metadata

    def run(
        self,
        request: ProcessingRequest,
        *,
        execution_id: str | None = None,
        raise_on_error: bool | None = None,
    ) -> ProcessingResult:
        """Execute the full processing lifecycle for ``request``."""
        self._validate_request(request)
        should_raise = (
            raise_on_error
            if raise_on_error is not None
            else request.execution_config.raise_on_error
        )

        record = self._tracker.create_execution(request, execution_id=execution_id)
        exec_id = record.id
        combined_metadata: dict[str, Any] = dict(request.metadata)
        input_artifact: Artifact | None = None

        self._logger.info(
            "processing_execution_created",
            request_id=request.request_id,
            execution_id=exec_id,
            dataset_id=str(request.dataset_id),
            source_path=request.source.path,
            destination_path=request.destination.path,
        )

        try:
            self._tracker.start(exec_id)

            # 1. Read source dataset
            self._tracker.record_transition(exec_id, ProcessingState.READING)
            raw_dataset = self._read_source_dataset(request, exec_id)
            input_artifact = Artifact(
                id=generate_artifact_id(),
                dataset_id=str(request.source_dataset_id or request.dataset_id),
                path=request.source.path,
                format=request.source.format,
                layer=request.source.layer,
                execution_id=exec_id,
            )

            # 2. Execute transformations
            self._tracker.record_transition(exec_id, ProcessingState.TRANSFORMING)
            exec_output = self._executor.execute(raw_dataset, request.transformations)
            if request.execution_config.fail_on_empty_input and exec_output.input_row_count == 0:
                raise ProcessingExecutionError(
                    f"Source dataset '{request.source.path}' is empty and "
                    "'fail_on_empty_input' is enabled.",
                    request_id=request.request_id,
                    execution_id=exec_id,
                )

            combined_metadata["engine"] = self._executor.engine.engine_name
            combined_metadata["step_metrics"] = [m.model_dump() for m in exec_output.step_metrics]

            # 3. Persist output dataset
            self._tracker.record_transition(exec_id, ProcessingState.WRITING)
            output_artifact = self._write_output_dataset(
                request=request,
                execution_id=exec_id,
                data=exec_output.data,
                partition_timestamp=record.started_at,
            )

            # 4. Register dataset and artifact metadata (non-transactional, independent)
            if self._metadata is not None:
                self._register_metadata(
                    request=request,
                    output_artifact=output_artifact,
                    execution_id=exec_id,
                    metadata=combined_metadata,
                )

            # 5. Complete execution tracking
            self._tracker.complete(
                exec_id,
                input_row_count=exec_output.input_row_count,
                output_row_count=exec_output.output_row_count,
                transformations_applied=exec_output.steps_executed,
                input_artifact_id=input_artifact.id,
                output_artifact_id=output_artifact.id,
            )
            completed_record = self._tracker.get_execution(exec_id)

            stats = ProcessingStatistics(
                input_row_count=exec_output.input_row_count,
                output_row_count=exec_output.output_row_count,
                input_column_count=len(exec_output.input_columns),
                output_column_count=len(exec_output.output_columns),
                transformations_applied=exec_output.steps_executed,
                duration_seconds=completed_record.duration_seconds,
            )

            self._logger.info(
                "processing_execution_succeeded",
                request_id=request.request_id,
                execution_id=exec_id,
                input_rows=stats.input_row_count,
                output_rows=stats.output_row_count,
                transformations_applied=stats.transformations_applied,
            )

            return ProcessingResult(
                request_id=request.request_id,
                execution_id=exec_id,
                dataset_id=request.dataset_id,
                status=ExecutionStatus.SUCCEEDED,
                lifecycle_state=completed_record.lifecycle_state,
                input_artifact=input_artifact,
                output_artifact=output_artifact,
                statistics=stats,
                started_at=completed_record.started_at,
                completed_at=completed_record.completed_at,
                duration_seconds=completed_record.duration_seconds,
                message="Processing completed successfully.",
                metadata=combined_metadata,
            )
        except Exception as exc:
            return self._handle_failure(
                request=request,
                initial_record=record,
                input_artifact=input_artifact,
                error=exc,
                metadata=combined_metadata,
                should_raise=should_raise,
            )

    @staticmethod
    def _validate_request(request: ProcessingRequest) -> None:
        if not isinstance(request, ProcessingRequest):
            raise InvalidProcessingRequestError(
                f"Expected ProcessingRequest instance, got {type(request).__name__}."
            )
        request.validate_request()

    def _read_source_dataset(
        self,
        request: ProcessingRequest,
        execution_id: str,
    ) -> Any:
        try:
            return self._storage_manager.load(request.source)
        except ProcessingStorageReadError:
            raise
        except StorageError as exc:
            raise ProcessingStorageReadError(
                f"Failed to read source dataset '{request.dataset_id}' from "
                f"'{request.source.path}': {exc}",
                request_id=request.request_id,
                execution_id=execution_id,
            ) from exc
        except Exception as exc:
            raise ProcessingStorageReadError(
                f"Unexpected error reading source dataset '{request.dataset_id}' from "
                f"'{request.source.path}': {exc}",
                request_id=request.request_id,
                execution_id=execution_id,
            ) from exc

    def _write_output_dataset(
        self,
        *,
        request: ProcessingRequest,
        execution_id: str,
        data: Any,
        partition_timestamp: datetime,
    ) -> Artifact:
        target_location = self._resolve_destination_location(request, partition_timestamp)
        try:
            if not request.execution_config.overwrite and self._storage_manager.exists(
                target_location
            ):
                raise ProcessingStorageWriteError(
                    f"Destination path '{target_location.path}' already exists and "
                    "overwrite is disabled.",
                    request_id=request.request_id,
                    execution_id=execution_id,
                )

            storage_result = self._storage_manager.save(
                data=data,
                location=target_location,
            )
        except ProcessingStorageWriteError:
            raise
        except StorageError as exc:
            raise ProcessingStorageWriteError(
                f"Storage error while persisting processed dataset '{request.dataset_id}' "
                f"to '{target_location.path}': {exc}",
                request_id=request.request_id,
                execution_id=execution_id,
            ) from exc
        except Exception as exc:
            raise ProcessingStorageWriteError(
                f"Unexpected error persisting processed dataset '{request.dataset_id}' "
                f"to '{target_location.path}': {exc}",
                request_id=request.request_id,
                execution_id=execution_id,
            ) from exc

        if not storage_result.success:
            raise ProcessingStorageWriteError(
                storage_result.message
                or f"Storage manager reported unsuccessful save for '{target_location.path}'.",
                request_id=request.request_id,
                execution_id=execution_id,
            )

        artifact_id = generate_artifact_id()
        namespace = _resolve_request_namespace(request)
        tags = _extract_request_tags(request)

        return Artifact(
            id=artifact_id,
            dataset_id=str(request.dataset_id),
            path=storage_result.path,
            format=target_location.format,
            urn=build_artifact_urn(artifact_id, namespace=namespace),
            namespace=namespace,
            layer=target_location.layer,
            execution_id=execution_id,
            row_count=storage_result.row_count,
            tags=tags,
        )

    def _resolve_canonical_dataset_urn(
        self,
        dataset_ref: str,
        *,
        namespace: str,
        target_dataset_id: str,
        artifact_id: str,
        execution_id: str,
        metadata: dict[str, Any],
    ) -> str:
        """Resolve a dataset reference or ID to a canonical `urn:atlas:dataset:...` URN."""
        cleaned = dataset_ref.strip()
        if is_canonical_urn(cleaned, entity_type="dataset"):
            parts = cleaned.split(":")
            return build_dataset_urn(parts[4].strip(), namespace=parts[3].strip().lower())

        if self._metadata is not None:
            try:
                existing = self._metadata.get_dataset(cleaned)
            except Exception as lookup_exc:
                warn_msg = (
                    f"Failed to look up upstream dataset '{cleaned}' for "
                    f"dataset_id='{target_dataset_id}', artifact_id='{artifact_id}', "
                    f"execution_id='{execution_id}': {lookup_exc}"
                )
                self._logger.warning(
                    "processing_source_metadata_lookup_failed",
                    dataset_id=target_dataset_id,
                    source_dataset_id=cleaned,
                    artifact_id=artifact_id,
                    execution_id=execution_id,
                    error_type=type(lookup_exc).__name__,
                    error=str(lookup_exc),
                    message=warn_msg,
                )
                self._stdlib_logger.warning(warn_msg)
                metadata.setdefault("metadata_warnings", []).append(warn_msg)
                existing = None

            if existing is not None:
                direct_urn = getattr(existing, "urn", None)
                if direct_urn is not None and is_canonical_urn(
                    str(direct_urn), entity_type="dataset"
                ):
                    parts = str(direct_urn).strip().split(":")
                    return build_dataset_urn(parts[4].strip(), namespace=parts[3].strip().lower())
                existing_meta = getattr(existing, "metadata", None)
                if isinstance(existing_meta, dict):
                    meta_urn = existing_meta.get("urn")
                    if isinstance(meta_urn, str) and is_canonical_urn(
                        meta_urn, entity_type="dataset"
                    ):
                        parts = meta_urn.strip().split(":")
                        return build_dataset_urn(
                            parts[4].strip(), namespace=parts[3].strip().lower()
                        )
                existing_ns = (
                    getattr(existing, "namespace", None)
                    or (existing_meta.get("namespace") if isinstance(existing_meta, dict) else None)
                    or namespace
                )
                existing_id = (
                    getattr(existing, "dataset_id", None)
                    or getattr(existing, "id", None)
                    or cleaned
                )
                return build_dataset_urn(str(existing_id), namespace=str(existing_ns))

        return build_dataset_urn(cleaned, namespace=namespace)

    def _resolve_upstream_urns(
        self,
        *,
        request: ProcessingRequest,
        namespace: str,
        artifact_id: str,
        execution_id: str,
        metadata: dict[str, Any],
    ) -> tuple[str, list[str]]:
        """Combine and normalize ``source_dataset_id`` and ``upstream_urns`` into canonical URNs."""
        target_dataset_id = str(request.dataset_id)
        source_ds_id = _resolve_source_dataset_id(request)

        raw_upstream_refs: list[str] = [source_ds_id]
        for key in ("source_dataset_id", "upstream_dataset_id"):
            val = request.metadata.get(key)
            if isinstance(val, str) and val.strip():
                raw_upstream_refs.append(val.strip())
        if request.upstream_urns:
            raw_upstream_refs.extend(
                str(item).strip() for item in request.upstream_urns if str(item).strip()
            )
        meta_upstream = request.metadata.get("upstream_urns")
        if isinstance(meta_upstream, (list, tuple)):
            raw_upstream_refs.extend(
                str(item).strip() for item in meta_upstream if str(item).strip()
            )

        resolved_by_ref: dict[str, str] = {}
        canonical_urns: list[str] = []
        seen_urns: set[str] = set()

        for ref in raw_upstream_refs:
            clean_ref = ref.strip()
            if not clean_ref:
                continue
            if clean_ref in resolved_by_ref:
                resolved_urn = resolved_by_ref[clean_ref]
            else:
                resolved_urn = self._resolve_canonical_dataset_urn(
                    clean_ref,
                    namespace=namespace,
                    target_dataset_id=target_dataset_id,
                    artifact_id=artifact_id,
                    execution_id=execution_id,
                    metadata=metadata,
                )
                resolved_by_ref[clean_ref] = resolved_urn
                resolved_by_ref[resolved_urn] = resolved_urn

            if resolved_urn not in seen_urns:
                seen_urns.add(resolved_urn)
                canonical_urns.append(resolved_urn)

        return source_ds_id, canonical_urns

    def _register_metadata(
        self,
        *,
        request: ProcessingRequest,
        output_artifact: Artifact,
        execution_id: str,
        metadata: dict[str, Any],
    ) -> None:
        """Independently register dataset and artifact metadata in the catalog."""
        if self._metadata is None:
            return

        target_dataset_id = str(request.dataset_id)
        namespace = _resolve_request_namespace(request)
        tags = _extract_request_tags(request)
        source_ds_id, upstream_urns = self._resolve_upstream_urns(
            request=request,
            namespace=namespace,
            artifact_id=output_artifact.id,
            execution_id=execution_id,
            metadata=metadata,
        )

        target_urn = build_dataset_urn(target_dataset_id, namespace=namespace)
        dataset_name = str(request.metadata.get("dataset_name") or target_dataset_id).strip()
        description = (
            str(request.metadata["description"])
            if isinstance(request.metadata.get("description"), str)
            else None
        )
        owner = (
            str(request.metadata["owner"])
            if isinstance(request.metadata.get("owner"), str)
            else None
        )

        dataset_record = Dataset(
            id=target_dataset_id,
            name=dataset_name,
            layer=output_artifact.layer or request.destination.layer,
            urn=target_urn,
            namespace=namespace,
            source_id=source_ds_id,
            format=output_artifact.format,
            path=output_artifact.path,
            description=description,
            owner=owner,
            tags=tags,
            upstream_urns=upstream_urns,
            metadata={
                "urn": target_urn,
                "namespace": namespace,
                "tags": list(tags),
                "upstream_urns": list(upstream_urns),
                **({"owner": owner} if owner is not None else {}),
            },
        )

        # 1. Dataset metadata registration (handled independently; updates current catalog entry)
        try:
            self._metadata.register_dataset_metadata(dataset_record, overwrite=True)
        except Exception as ds_exc:
            warn_msg = (
                f"Failed to register dataset metadata for dataset_id='{target_dataset_id}', "
                f"artifact_id='{output_artifact.id}', execution_id='{execution_id}': {ds_exc}"
            )
            self._logger.warning(
                "processing_dataset_metadata_registration_failed",
                dataset_id=target_dataset_id,
                artifact_id=output_artifact.id,
                execution_id=execution_id,
                error_type=type(ds_exc).__name__,
                error=str(ds_exc),
                message=warn_msg,
            )
            self._stdlib_logger.warning(warn_msg)
            metadata["dataset_metadata_registration_error"] = str(ds_exc)
            metadata.setdefault("metadata_warnings", []).append(warn_msg)

        # 2. Artifact metadata registration (handled independently; unique artifact_id per run
        #    preserves historical artifacts across executions)
        try:
            self._metadata.register_artifact(output_artifact, overwrite=True)
        except Exception as art_exc:
            warn_msg = (
                f"Failed to register artifact metadata for dataset_id='{target_dataset_id}', "
                f"artifact_id='{output_artifact.id}', execution_id='{execution_id}': {art_exc}"
            )
            self._logger.warning(
                "processing_artifact_metadata_registration_failed",
                dataset_id=target_dataset_id,
                artifact_id=output_artifact.id,
                execution_id=execution_id,
                error_type=type(art_exc).__name__,
                error=str(art_exc),
                message=warn_msg,
            )
            self._stdlib_logger.warning(warn_msg)
            metadata["artifact_metadata_registration_error"] = str(art_exc)
            metadata.setdefault("metadata_warnings", []).append(warn_msg)

    @staticmethod
    def _resolve_destination_location(
        request: ProcessingRequest,
        partition_timestamp: datetime,
    ) -> StorageLocation:
        dest = request.destination
        target_path = dest.path
        if request.execution_config.partition_by_date:
            partition_segment = PartitionStrategy.by_date(partition_timestamp)
            pure = PurePath(dest.path)
            if pure.suffix:
                target_path = str(pure.parent / partition_segment / pure.name)
            else:
                target_path = str(pure / partition_segment)

        return StorageLocation(
            layer=dest.layer,
            path=target_path,
            format=dest.format,
        )

    def _handle_failure(
        self,
        *,
        request: ProcessingRequest,
        initial_record: ProcessingExecutionRecord,
        input_artifact: Artifact | None,
        error: Exception,
        metadata: dict[str, Any],
        should_raise: bool,
    ) -> ProcessingResult:
        exec_id = initial_record.id
        if isinstance(error, ProcessingError):
            error.request_id = error.request_id or request.request_id
            error.execution_id = error.execution_id or exec_id
            processing_error: ProcessingError = error
        else:
            processing_error = ProcessingError(
                f"Unexpected processing error: {error}",
                request_id=request.request_id,
                execution_id=exec_id,
            )
            processing_error.__cause__ = error

        try:
            self._tracker.fail(
                exec_id,
                reason=str(processing_error),
                error_type=type(processing_error).__name__,
            )
            failed_record = self._tracker.get_execution(exec_id)
            completed_at = failed_record.completed_at or datetime.now(UTC)
            duration_seconds = failed_record.duration_seconds
        except ProcessingLifecycleError as tracking_exc:
            # Never mask the original transformation/storage error if failure tracking fails.
            self._logger.error(
                "processing_failure_tracking_error",
                request_id=request.request_id,
                execution_id=exec_id,
                tracking_error=str(tracking_exc),
                original_error=str(processing_error),
            )
            metadata["tracking_error"] = str(tracking_exc)
            completed_at = datetime.now(UTC)
            duration_seconds = max(0.0, (completed_at - initial_record.started_at).total_seconds())

        cause = processing_error.__cause__
        error_cause = f"{type(cause).__name__}: {cause}" if cause is not None else None

        self._logger.error(
            "processing_execution_failed",
            request_id=request.request_id,
            execution_id=exec_id,
            error_type=type(processing_error).__name__,
            error_message=str(processing_error),
            error_cause=error_cause,
        )

        if should_raise:
            raise processing_error

        return ProcessingResult(
            request_id=request.request_id,
            execution_id=exec_id,
            dataset_id=request.dataset_id,
            status=ExecutionStatus.FAILED,
            lifecycle_state=ProcessingState.FAILED,
            input_artifact=input_artifact,
            output_artifact=None,
            statistics=ProcessingStatistics(duration_seconds=duration_seconds),
            started_at=initial_record.started_at,
            completed_at=completed_at,
            duration_seconds=duration_seconds,
            error_type=type(processing_error).__name__,
            error_message=str(processing_error),
            error_cause=error_cause,
            message=str(processing_error),
            metadata=metadata,
        )


__all__ = [
    "ProcessingPipeline",
    "create_default_storage_registry",
]
