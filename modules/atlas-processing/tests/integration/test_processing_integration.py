"""Integration tests for atlas-processing with atlas-storage and atlas-core."""

from __future__ import annotations

from pathlib import Path

import pytest
from atlas_core import DatasetId, ExecutionStatus
from atlas_processing import (
    ProcessingExecutionConfig,
    ProcessingPipeline,
    ProcessingRequest,
    ProcessingService,
    ProcessingState,
    ProcessingStorageReadError,
    TransformationStep,
)
from atlas_storage import FileSystemBackend, StorageLocation, StorageManager


def test_end_to_end_bronze_to_silver_to_gold_medallion_flow(tmp_path: Path) -> None:
    """Test complete medallion flow: bronze (JSON) -> silver (Parquet) -> gold (CSV)."""
    storage_manager = StorageManager(backend=FileSystemBackend())
    service = ProcessingService(storage_manager=storage_manager)

    # 1. Seed raw bronze dataset via atlas-storage
    bronze_records = [
        {
            "order_id": "o-1",
            "customer": " alice ",
            "region": "EU",
            "status": "completed",
            "price": 100.0,
            "qty": 2,
        },
        {
            "order_id": "o-2",
            "customer": " bob ",
            "region": "US",
            "status": "cancelled",
            "price": 50.0,
            "qty": 1,
        },
        {
            "order_id": "o-3",
            "customer": " carol ",
            "region": "EU",
            "status": "completed",
            "price": 150.0,
            "qty": 1,
        },
        {
            "order_id": "o-4",
            "customer": " dave ",
            "region": "US",
            "status": "completed",
            "price": 80.0,
            "qty": 3,
        },
        {
            "order_id": "o-5",
            "customer": " eve ",
            "region": None,
            "status": "completed",
            "price": 200.0,
            "qty": 1,
        },
    ]
    bronze_loc = StorageLocation(
        layer="bronze",
        path=str(tmp_path / "bronze" / "orders.json"),
        format="json",
    )
    storage_manager.save(bronze_records, bronze_loc)

    # 2. Process Bronze (JSON) -> Silver (Parquet): filter valid completed orders & enrich columns
    silver_loc = StorageLocation(
        layer="silver",
        path=str(tmp_path / "silver" / "orders_clean.parquet"),
        format="parquet",
    )
    silver_request = ProcessingRequest(
        dataset_id=DatasetId("orders_silver"),
        source=bronze_loc,
        destination=silver_loc,
        transformations=[
            TransformationStep(
                name="completed_with_region",
                transformation_id="filter",
                params={
                    "logical_operator": "and",
                    "conditions": [
                        {"column": "status", "operator": "eq", "value": "completed"},
                        {"column": "region", "operator": "not_null"},
                    ],
                },
            ),
            TransformationStep(
                name="clean_and_enrich",
                transformation_id="map",
                params={
                    "operations": {"customer": "strip"},
                    "derived_columns": [
                        {
                            "target_column": "order_total",
                            "expression": "multiply",
                            "source_columns": ["price", "qty"],
                        }
                    ],
                    "select_columns": ["order_id", "customer", "region", "order_total"],
                },
            ),
        ],
    )

    silver_result = service.process(silver_request)
    assert silver_result.status == ExecutionStatus.SUCCEEDED
    assert silver_result.success is True
    assert silver_result.output_artifact is not None
    assert Path(silver_result.output_artifact.path).exists()
    assert silver_result.statistics.input_row_count == 5
    assert silver_result.statistics.output_row_count == 3
    assert silver_result.statistics.rows_delta == -2

    silver_df = storage_manager.load(silver_loc)
    silver_data = silver_df.to_dict(orient="records")
    assert len(silver_data) == 3
    assert {r["customer"] for r in silver_data} == {"alice", "carol", "dave"}

    # 3. Process Silver (Parquet) -> Gold (CSV): aggregate revenue by region
    gold_loc = StorageLocation(
        layer="gold",
        path=str(tmp_path / "gold" / "regional_revenue.csv"),
        format="csv",
    )
    gold_request = ProcessingRequest(
        dataset_id=DatasetId("regional_revenue_gold"),
        source=silver_loc,
        destination=gold_loc,
        transformations=[
            TransformationStep(
                name="aggregate_by_region",
                transformation_id="aggregate",
                params={
                    "group_by": ["region"],
                    "aggregations": [
                        {"column": "order_total", "function": "sum", "alias": "total_revenue"},
                        {"column": "order_id", "function": "count", "alias": "order_count"},
                        {"column": "order_total", "function": "avg", "alias": "avg_order_value"},
                    ],
                },
            )
        ],
    )

    gold_result = service.process(gold_request)
    assert gold_result.status == ExecutionStatus.SUCCEEDED
    assert gold_result.output_artifact is not None
    assert Path(gold_result.output_artifact.path).exists()
    assert gold_result.statistics.input_row_count == 3
    assert gold_result.statistics.output_row_count == 2

    gold_df = storage_manager.load(gold_loc)
    gold_data = gold_df.to_dict(orient="records")
    assert len(gold_data) == 2
    by_region = {r["region"]: r for r in gold_data}
    assert float(by_region["EU"]["total_revenue"]) == 350.0
    assert int(by_region["EU"]["order_count"]) == 2
    assert float(by_region["EU"]["avg_order_value"]) == 175.0
    assert float(by_region["US"]["total_revenue"]) == 240.0
    assert int(by_region["US"]["order_count"]) == 1

    # Verify execution tracking across both runs
    executions = service.list_executions()
    assert len(executions) == 2
    silver_exec = service.get_execution(silver_result.execution_id)
    assert silver_exec.lifecycle_state == ProcessingState.COMPLETED
    assert silver_exec.state_history == [
        ProcessingState.PENDING,
        ProcessingState.READING,
        ProcessingState.TRANSFORMING,
        ProcessingState.WRITING,
        ProcessingState.COMPLETED,
    ]


def test_integration_csv_to_json_with_date_partitioning(tmp_path: Path) -> None:
    """Test CSV source to date-partitioned JSON destination."""
    storage_manager = StorageManager(backend=FileSystemBackend())
    service = ProcessingService(storage_manager=storage_manager)

    csv_source = StorageLocation(
        layer="bronze",
        path=str(tmp_path / "bronze" / "events.csv"),
        format="csv",
    )
    storage_manager.save(
        [
            {"event_id": "e1", "type": "click", "score": 10},
            {"event_id": "e2", "type": "view", "score": 5},
        ],
        csv_source,
    )

    json_dest = StorageLocation(
        layer="silver",
        path=str(tmp_path / "silver" / "events.json"),
        format="json",
    )
    request = ProcessingRequest(
        dataset_id=DatasetId("events_curated"),
        source=csv_source,
        destination=json_dest,
        transformations=[
            TransformationStep(
                transformation_id="map",
                params={
                    "operations": {"score": "to_int", "type": "upper"},
                },
            ),
            TransformationStep(
                transformation_id="filter",
                params={"column": "score", "operator": "gte", "value": 10},
            ),
        ],
        execution_config=ProcessingExecutionConfig(
            partition_by_date=True,
        ),
    )

    result = service.process(request)
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.output_artifact is not None
    assert "events.json" in result.output_artifact.path
    assert Path(result.output_artifact.path).exists()

    persisted_loc = StorageLocation(
        layer="silver",
        path=result.output_artifact.path,
        format="json",
    )
    loaded = storage_manager.load(persisted_loc)
    records = loaded.to_dict(orient="records") if hasattr(loaded, "to_dict") else loaded
    assert records == [{"event_id": "e1", "type": "CLICK", "score": 10}]


def test_integration_missing_source_dataset_error_handling(tmp_path: Path) -> None:
    """Test missing source file handling in both non-raising service and raising pipeline modes."""
    storage_manager = StorageManager(backend=FileSystemBackend())
    service = ProcessingService(storage_manager=storage_manager)

    missing_source = StorageLocation(
        layer="bronze",
        path=str(tmp_path / "bronze" / "does_not_exist.parquet"),
        format="parquet",
    )
    dest = StorageLocation(
        layer="silver",
        path=str(tmp_path / "silver" / "out.parquet"),
        format="parquet",
    )
    request = ProcessingRequest(
        dataset_id=DatasetId("missing_test"),
        source=missing_source,
        destination=dest,
    )

    # Service captures error in ProcessingResult and ExecutionTracker
    result = service.process(request)
    assert result.status == ExecutionStatus.FAILED
    assert result.output_artifact is None
    assert result.error_message is not None
    assert not Path(dest.path).exists()

    tracked = service.get_execution(result.execution_id)
    assert tracked.status == ExecutionStatus.FAILED
    assert tracked.lifecycle_state == ProcessingState.FAILED

    # Raising pipeline propagates ProcessingStorageReadError
    pipeline = ProcessingPipeline(storage_manager=storage_manager)
    with pytest.raises(ProcessingStorageReadError):
        pipeline.run(request, raise_on_error=True)
