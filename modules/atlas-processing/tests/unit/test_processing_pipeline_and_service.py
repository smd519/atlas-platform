"""Unit tests for ProcessingPipeline and ProcessingService."""

from __future__ import annotations

import logging
from typing import Any

import pytest
from atlas_core import Artifact, Dataset, DatasetId, ExecutionStatus, MetadataProvider
from atlas_processing import (
    ProcessingExecutionConfig,
    ProcessingPipeline,
    ProcessingRequest,
    ProcessingService,
    ProcessingStorageReadError,
    ProcessingStorageWriteError,
    TransformationError,
    TransformationStep,
)
from atlas_storage import (
    FileSystemBackend,
    StorageLocation,
    StorageManager,
    StorageReadError,
    StorageResult,
    StorageWriteError,
)


class StubStorageManager(StorageManager):
    """Configurable stub StorageManager for unit testing pipeline failure modes."""

    def __init__(
        self,
        data_to_return: Any = None,
        read_error: Exception | None = None,
        write_error: Exception | None = None,
        dest_exists: bool = False,
    ) -> None:
        super().__init__(backend=FileSystemBackend())
        self.data_to_return = (
            data_to_return if data_to_return is not None else [{"id": 1, "val": 10}]
        )
        self.read_error = read_error
        self.write_error = write_error
        self.dest_exists = dest_exists
        self.saved_payloads: list[tuple[Any, StorageLocation]] = []

    def load(self, location: StorageLocation) -> Any:
        if self.read_error is not None:
            raise self.read_error
        return self.data_to_return

    def exists(self, location: StorageLocation) -> bool:
        return self.dest_exists

    def save(
        self,
        data: Any,
        location: StorageLocation,
        *,
        serializer: Any = None,
    ) -> StorageResult:
        if self.write_error is not None:
            raise self.write_error
        self.saved_payloads.append((data, location))
        return StorageResult(
            success=True,
            path=location.path,
            message="ok",
        )


def test_processing_service_happy_path() -> None:
    """ProcessingService processes dataset, tracks execution, and returns ProcessingResult."""
    stub_storage = StubStorageManager(
        data_to_return=[
            {"id": 1, "status": "ok", "amount": 50},
            {"id": 2, "status": "bad", "amount": 10},
            {"id": 3, "status": "ok", "amount": 70},
        ]
    )
    service = ProcessingService(storage_manager=stub_storage)

    request = ProcessingRequest(
        dataset_id=DatasetId("payments"),
        source=StorageLocation(layer="bronze", path="/tmp/bronze/payments.json", format="json"),
        destination=StorageLocation(
            layer="silver", path="/tmp/silver/payments.parquet", format="parquet"
        ),
        transformations=[
            TransformationStep(
                name="filter_ok",
                transformation_id="filter",
                params={"column": "status", "operator": "eq", "value": "ok"},
            ),
            TransformationStep(
                name="select_cols",
                transformation_id="map",
                params={"select_columns": ["id", "amount"]},
            ),
        ],
        execution_config=ProcessingExecutionConfig(
            partition_by_date=True,
        ),
    )

    result = service.process(request)
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.success is True
    assert result.output_artifact is not None
    assert "payments.parquet" in result.output_artifact.path
    assert result.statistics.input_row_count == 3
    assert result.statistics.output_row_count == 2
    assert result.statistics.rows_delta == -1
    assert result.statistics.transformations_applied == 2
    assert result.statistics.output_column_count == 2

    # Check execution tracking
    exec_record = service.get_execution(result.execution_id)
    assert exec_record.status == ExecutionStatus.SUCCEEDED
    assert exec_record.output_artifact_id == result.output_artifact.id
    assert len(service.list_executions()) == 1


def test_processing_pipeline_storage_read_failure() -> None:
    """Pipeline records FAILED execution and wraps StorageReadError."""
    stub_storage = StubStorageManager(read_error=StorageReadError("Source file missing"))
    service = ProcessingService(storage_manager=stub_storage)
    request = ProcessingRequest(
        dataset_id=DatasetId("payments"),
        source=StorageLocation(layer="bronze", path="/tmp/missing.json", format="json"),
        destination=StorageLocation(layer="silver", path="/tmp/out.json", format="json"),
    )

    result = service.process(request)
    assert result.status == ExecutionStatus.FAILED
    assert result.success is False
    assert result.output_artifact is None
    assert result.error_message is not None
    assert "Source file missing" in result.error_message

    # With raise_on_error=True, raises ProcessingStorageReadError with chained cause
    pipeline_raising = ProcessingPipeline(storage_manager=stub_storage)
    with pytest.raises(
        ProcessingStorageReadError, match="Failed to read source dataset"
    ) as exc_info:
        pipeline_raising.run(request, raise_on_error=True)
    assert isinstance(exc_info.value.__cause__, StorageReadError)


def test_processing_pipeline_transformation_failure() -> None:
    """Pipeline records FAILED execution on transformation failure and cleans up."""
    stub_storage = StubStorageManager(data_to_return=[{"id": 1}])
    pipeline = ProcessingPipeline(storage_manager=stub_storage)
    request = ProcessingRequest(
        dataset_id=DatasetId("payments"),
        source=StorageLocation(layer="bronze", path="/tmp/in.json", format="json"),
        destination=StorageLocation(layer="silver", path="/tmp/out.json", format="json"),
        transformations=[
            TransformationStep(
                transformation_id="filter",
                params={"column": "missing_col", "operator": "eq", "value": 1},
            )
        ],
    )

    with pytest.raises(TransformationError, match="Missing required column"):
        pipeline.run(request, raise_on_error=True)
    assert stub_storage.saved_payloads == []


def test_processing_pipeline_storage_write_and_overwrite_failure() -> None:
    """Pipeline enforces overwrite=False and wraps StorageWriteError."""
    stub_exists = StubStorageManager(dest_exists=True)
    pipeline_conflict = ProcessingPipeline(storage_manager=stub_exists)
    req_no_overwrite = ProcessingRequest(
        dataset_id=DatasetId("payments"),
        source=StorageLocation(layer="bronze", path="/tmp/in.json", format="json"),
        destination=StorageLocation(layer="silver", path="/tmp/out.json", format="json"),
        execution_config=ProcessingExecutionConfig(overwrite=False),
    )

    with pytest.raises(ProcessingStorageWriteError, match="overwrite is disabled"):
        pipeline_conflict.run(req_no_overwrite, raise_on_error=True)

    stub_write_err = StubStorageManager(write_error=StorageWriteError("Disk full"))
    pipeline_write_err = ProcessingPipeline(storage_manager=stub_write_err)
    req_overwrite = ProcessingRequest(
        dataset_id=DatasetId("payments"),
        source=StorageLocation(layer="bronze", path="/tmp/in.json", format="json"),
        destination=StorageLocation(layer="silver", path="/tmp/out.json", format="json"),
    )

    with pytest.raises(
        ProcessingStorageWriteError, match="Storage error while persisting"
    ) as exc_info:
        pipeline_write_err.run(req_overwrite, raise_on_error=True)
    assert isinstance(exc_info.value.__cause__, StorageWriteError)


def test_processing_pipeline_fail_on_empty_input() -> None:
    """Pipeline respects fail_on_empty_input execution configuration."""
    stub_empty = StubStorageManager(data_to_return=[])
    service = ProcessingService(storage_manager=stub_empty)
    req = ProcessingRequest(
        dataset_id=DatasetId("empty_ds"),
        source=StorageLocation(layer="bronze", path="/tmp/in.json", format="json"),
        destination=StorageLocation(layer="silver", path="/tmp/out.json", format="json"),
        execution_config=ProcessingExecutionConfig(fail_on_empty_input=True),
    )

    res = service.process(req)
    assert res.status == ExecutionStatus.FAILED
    assert res.error_message is not None
    assert "empty" in res.error_message.lower()


class SpyMetadataProvider(MetadataProvider):
    """Configurable MetadataProvider spy modeling repository-backed dataset and artifact storage."""

    def __init__(
        self,
        *,
        existing_datasets: dict[str, Dataset] | None = None,
        fail_get_dataset: Exception | None = None,
        fail_register_dataset: Exception | None = None,
        fail_register_artifact: Exception | None = None,
    ) -> None:
        self._existing = dict(existing_datasets or {})
        self._artifacts_by_id: dict[str, Artifact] = {}
        self._artifacts_by_urn: dict[str, str] = {}
        self._fail_get = fail_get_dataset
        self._fail_reg_ds = fail_register_dataset
        self._fail_reg_art = fail_register_artifact
        self.get_dataset_calls: list[str] = []
        self.registered_datasets: list[tuple[Dataset, bool]] = []
        self.registered_artifacts: list[tuple[Artifact, bool]] = []

    def get_dataset(self, dataset_id: str) -> Dataset | None:
        self.get_dataset_calls.append(dataset_id)
        if self._fail_get is not None:
            raise self._fail_get
        return self._existing.get(dataset_id)

    def register_dataset(
        self,
        dataset: Dataset,
        *,
        overwrite: bool = False,
    ) -> Dataset:
        if self._fail_reg_ds is not None:
            raise self._fail_reg_ds
        self.registered_datasets.append((dataset, overwrite))
        self._existing[dataset.id] = dataset
        return dataset

    def register_artifact(
        self,
        artifact: Artifact,
        *,
        overwrite: bool = False,
    ) -> Artifact:
        if self._fail_reg_art is not None:
            raise self._fail_reg_art
        self.registered_artifacts.append((artifact, overwrite))
        self._artifacts_by_id[artifact.id] = artifact
        if artifact.urn is not None:
            self._artifacts_by_urn[artifact.urn] = artifact.id
        return artifact

    def list_artifacts(self, *, dataset_id: str | None = None) -> list[Artifact]:
        items = list(self._artifacts_by_id.values())
        if dataset_id is not None:
            items = [a for a in items if a.dataset_id == dataset_id]
        return items


def test_processing_registers_dataset_and_artifact_with_registered_source_urn() -> None:
    """When source dataset is registered in MetadataProvider, its canonical URN is used."""
    bronze_ds = Dataset(
        id="orders_bronze",
        name="Orders Bronze",
        layer="bronze",
        urn="urn:atlas:dataset:commerce:orders_bronze",
        namespace="commerce",
        format="json",
        path="/tmp/bronze/orders_bronze.json",
    )
    spy_meta = SpyMetadataProvider(existing_datasets={"orders_bronze": bronze_ds})
    stub_storage = StubStorageManager(
        data_to_return=[
            {"id": 1, "status": "ok", "amount": 50},
            {"id": 2, "status": "ok", "amount": 75},
        ]
    )
    service = ProcessingService(
        storage_manager=stub_storage,
        metadata_provider=spy_meta,
    )

    request = ProcessingRequest(
        dataset_id=DatasetId("orders_silver"),
        source_dataset_id=DatasetId("orders_bronze"),
        namespace="commerce",
        source=StorageLocation(
            layer="bronze",
            path="/tmp/bronze/orders_bronze.json",
            format="json",
        ),
        destination=StorageLocation(
            layer="silver",
            path="/tmp/silver/orders_silver.parquet",
            format="parquet",
        ),
        tags=["curated", "orders"],
        upstream_urns=["customers_dim"],
    )

    result = service.process(request, execution_id="exec-proc-meta-1")
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.output_artifact is not None

    assert "orders_bronze" in spy_meta.get_dataset_calls
    assert len(spy_meta.registered_datasets) == 1
    reg_ds, ds_overwrite = spy_meta.registered_datasets[0]
    assert ds_overwrite is True
    assert reg_ds.id == "orders_silver"
    assert reg_ds.dataset_id == "orders_silver"
    assert reg_ds.layer == "silver"
    assert reg_ds.format == "parquet"
    assert reg_ds.path == "/tmp/silver/orders_silver.parquet"
    assert reg_ds.urn == "urn:atlas:dataset:commerce:orders_silver"
    assert reg_ds.tags == ["curated", "orders"]
    # Both registered source dataset and additional upstream dependency are canonical URNs
    assert reg_ds.upstream_urns == [
        "urn:atlas:dataset:commerce:orders_bronze",
        "urn:atlas:dataset:commerce:customers_dim",
    ]
    assert all(u.startswith("urn:atlas:dataset:") for u in reg_ds.upstream_urns)

    assert len(spy_meta.registered_artifacts) == 1
    reg_art, art_overwrite = spy_meta.registered_artifacts[0]
    assert art_overwrite is True
    assert reg_art.id == result.output_artifact.id
    assert reg_art.dataset_id == "orders_silver"
    assert reg_art.path == "/tmp/silver/orders_silver.parquet"
    assert reg_art.uri == "/tmp/silver/orders_silver.parquet"
    assert reg_art.format == "parquet"
    assert reg_art.layer == "silver"
    assert reg_art.execution_id == "exec-proc-meta-1"
    # StubStorageManager does not populate StorageResult.row_count or byte_size
    assert reg_art.row_count is None
    assert reg_art.byte_size is None


def test_processing_constructs_canonical_urn_when_source_not_registered() -> None:
    """When source dataset is unregistered, constructs canonical URN via atlas-core."""
    spy_meta = SpyMetadataProvider()
    stub_storage = StubStorageManager(data_to_return=[{"id": 1, "val": 10}])
    service = ProcessingService(
        storage_manager=stub_storage,
        metadata_provider=spy_meta,
    )

    request = ProcessingRequest(
        dataset_id=DatasetId("sensors_silver"),
        source=StorageLocation(
            layer="bronze",
            path="data/bronze/sensors_raw.csv",
            format="csv",
        ),
        destination=StorageLocation(
            layer="silver",
            path="data/silver/sensors_silver.parquet",
            format="parquet",
        ),
    )

    res1 = service.process(request, execution_id="exec-urn-1")
    res2 = service.process(request, execution_id="exec-urn-2")
    assert res1.status == ExecutionStatus.SUCCEEDED
    assert res2.status == ExecutionStatus.SUCCEEDED
    assert res1.output_artifact is not None
    assert res2.output_artifact is not None
    # Unique artifact ID generated per execution
    assert res1.output_artifact.id != res2.output_artifact.id

    assert spy_meta.get_dataset_calls == ["sensors_raw", "sensors_raw"]
    reg_ds, _ = spy_meta.registered_datasets[0]
    assert reg_ds.upstream_urns == ["urn:atlas:dataset:default:sensors_raw"]


def test_processing_handles_dataset_and_artifact_metadata_failures_independently(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Dataset and artifact metadata failures are logged independently without failing run."""
    stub_storage = StubStorageManager(data_to_return=[{"id": 1, "val": 10}])
    request = ProcessingRequest(
        dataset_id=DatasetId("payments_silver"),
        source_dataset_id=DatasetId("payments_bronze"),
        source=StorageLocation(layer="bronze", path="/tmp/in.json", format="json"),
        destination=StorageLocation(layer="silver", path="/tmp/out.parquet", format="parquet"),
    )

    # Case 1: Dataset registration fails, artifact registration still succeeds
    spy_ds_fail = SpyMetadataProvider(
        fail_register_dataset=RuntimeError("catalog db timeout"),
    )
    svc_ds_fail = ProcessingService(
        storage_manager=stub_storage,
        metadata_provider=spy_ds_fail,
    )
    with caplog.at_level(logging.WARNING, logger="atlas.processing.pipeline"):
        res_ds_fail = svc_ds_fail.process(request, execution_id="exec-ds-meta-fail")

    assert res_ds_fail.status == ExecutionStatus.SUCCEEDED
    assert res_ds_fail.success is True
    assert res_ds_fail.output_artifact is not None
    assert len(spy_ds_fail.registered_datasets) == 0
    assert len(spy_ds_fail.registered_artifacts) == 1
    assert "catalog db timeout" in res_ds_fail.metadata.get(
        "dataset_metadata_registration_error", ""
    )
    assert "payments_silver" in caplog.text
    assert res_ds_fail.output_artifact.id in caplog.text
    assert "exec-ds-meta-fail" in caplog.text

    caplog.clear()

    # Case 2: Dataset registration succeeds, artifact registration fails
    spy_art_fail = SpyMetadataProvider(
        fail_register_artifact=RuntimeError("artifact index unavailable"),
    )
    svc_art_fail = ProcessingService(
        storage_manager=stub_storage,
        metadata_provider=spy_art_fail,
    )
    with caplog.at_level(logging.WARNING, logger="atlas.processing.pipeline"):
        res_art_fail = svc_art_fail.process(request, execution_id="exec-art-meta-fail")

    assert res_art_fail.status == ExecutionStatus.SUCCEEDED
    assert res_art_fail.success is True
    assert res_art_fail.output_artifact is not None
    assert len(spy_art_fail.registered_datasets) == 1
    assert len(spy_art_fail.registered_artifacts) == 0
    assert "artifact index unavailable" in res_art_fail.metadata.get(
        "artifact_metadata_registration_error", ""
    )
    assert "payments_silver" in caplog.text
    assert res_art_fail.output_artifact.id in caplog.text
    assert "exec-art-meta-fail" in caplog.text


def test_processing_combines_normalizes_and_deduplicates_source_and_upstream_urns() -> None:
    """Both source_dataset_id and upstream_urns are combined, normalized, and deduplicated."""
    registered_fx = Dataset(
        id="fx_rates",
        name="FX Rates",
        layer="silver",
        urn="urn:atlas:dataset:finance:fx_rates",
        namespace="finance",
        format="parquet",
        path="data/silver/fx_rates.parquet",
    )
    spy_meta = SpyMetadataProvider(existing_datasets={"fx_rates": registered_fx})
    stub_storage = StubStorageManager(data_to_return=[{"id": 1, "val": 100}])
    service = ProcessingService(
        storage_manager=stub_storage,
        metadata_provider=spy_meta,
    )

    request = ProcessingRequest(
        dataset_id=DatasetId("orders_enriched_silver"),
        source_dataset_id=DatasetId("orders_bronze"),
        source=StorageLocation(
            layer="bronze",
            path="data/bronze/orders_bronze.parquet",
            format="parquet",
        ),
        destination=StorageLocation(
            layer="silver",
            path="data/silver/orders_enriched_silver.parquet",
            format="parquet",
        ),
        upstream_urns=[
            "orders_bronze",  # duplicate of source_dataset_id as raw ID
            "urn:atlas:dataset:default:orders_bronze",  # duplicate as canonical URN
            "fx_rates",  # registered in 'finance' namespace
            "customers_bronze",  # unregistered raw ID -> fallback default namespace
            "urn:atlas:dataset:Reference:country_codes",  # canonical URN with uppercase ns
        ],
    )

    result = service.process(request, execution_id="exec-lineage-combo")
    assert result.status == ExecutionStatus.SUCCEEDED

    assert len(spy_meta.registered_datasets) == 1
    reg_ds, _ = spy_meta.registered_datasets[0]
    assert reg_ds.urn == "urn:atlas:dataset:default:orders_enriched_silver"
    assert reg_ds.upstream_urns == [
        "urn:atlas:dataset:default:orders_bronze",
        "urn:atlas:dataset:finance:fx_rates",
        "urn:atlas:dataset:default:customers_bronze",
        "urn:atlas:dataset:reference:country_codes",
    ]
    # Verify orders_bronze was only looked up once despite appearing 3 times
    assert spy_meta.get_dataset_calls.count("orders_bronze") == 1


def test_processing_preserves_artifact_history_across_multiple_executions() -> None:
    """Multiple executions of the same dataset update the dataset while retaining all artifacts."""
    spy_meta = SpyMetadataProvider()
    stub_storage = StubStorageManager(
        data_to_return=[
            {"id": 1, "amount": 10},
            {"id": 2, "amount": 20},
        ]
    )
    service = ProcessingService(
        storage_manager=stub_storage,
        metadata_provider=spy_meta,
    )

    request = ProcessingRequest(
        dataset_id=DatasetId("daily_metrics_silver"),
        source_dataset_id=DatasetId("daily_metrics_bronze"),
        source=StorageLocation(
            layer="bronze",
            path="data/bronze/daily_metrics.csv",
            format="csv",
        ),
        destination=StorageLocation(
            layer="silver",
            path="data/silver/daily_metrics.parquet",
            format="parquet",
        ),
        tags=["metrics", "v1"],
    )

    r1 = service.process(request, execution_id="exec-hist-1")
    r2 = service.process(request, execution_id="exec-hist-2")

    request_v2 = request.model_copy(update={"tags": ["metrics", "v2"]})
    r3 = service.process(request_v2, execution_id="exec-hist-3")

    assert r1.status == ExecutionStatus.SUCCEEDED
    assert r2.status == ExecutionStatus.SUCCEEDED
    assert r3.status == ExecutionStatus.SUCCEEDED
    assert r1.output_artifact is not None
    assert r2.output_artifact is not None
    assert r3.output_artifact is not None

    # Current dataset catalog record reflects the latest update
    current_ds = spy_meta.get_dataset("daily_metrics_silver")
    assert current_ds is not None
    assert current_ds.tags == ["metrics", "v2"]

    # All 3 historical artifacts are preserved with distinct artifact IDs, URNs, and execution IDs
    stored_artifacts = spy_meta.list_artifacts(dataset_id="daily_metrics_silver")
    assert len(stored_artifacts) == 3
    artifact_ids = [a.id for a in stored_artifacts]
    artifact_urns = [a.urn for a in stored_artifacts]
    execution_ids = [a.execution_id for a in stored_artifacts]

    assert len(set(artifact_ids)) == 3
    assert len(set(artifact_urns)) == 3
    assert artifact_ids == [
        r1.output_artifact.id,
        r2.output_artifact.id,
        r3.output_artifact.id,
    ]
    assert execution_ids == ["exec-hist-1", "exec-hist-2", "exec-hist-3"]
