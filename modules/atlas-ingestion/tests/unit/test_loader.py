"""Unit tests for the Loader component using fake storage providers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd  # type: ignore[import-untyped]
import pytest
from atlas_connectors import ExtractionResult, SourceConfig
from atlas_core import DatasetId, StorageProvider
from atlas_ingestion.exceptions import LoadingError
from atlas_ingestion.loading.loader import Loader
from atlas_ingestion.models.ingestion_request import ExecutionConfig, IngestionRequest
from atlas_storage import StorageLocation, StorageManager, StorageWriteError


class InMemoryStorageProvider(StorageProvider):
    """In-memory StorageProvider implementation for unit testing."""

    def __init__(self, *, fail_on_save: bool = False) -> None:
        self.store: dict[str, bytes] = {}
        self._fail_on_save = fail_on_save

    def save(self, data: Any, path: str) -> None:
        if self._fail_on_save:
            raise StorageWriteError(f"Simulated write error for {path}")
        if not isinstance(data, bytes):
            raise TypeError("InMemoryStorageProvider expects bytes")
        self.store[path] = data

    def load(self, path: str) -> bytes:
        return self.store[path]

    def exists(self, path: str) -> bool:
        return path in self.store


def _make_request(
    dest_path: str = "data/bronze/items.csv",
    dest_format: str = "csv",
    *,
    partition_by_date: bool = False,
    overwrite: bool = True,
) -> IngestionRequest:
    return IngestionRequest(
        request_id="req-load-1",
        dataset_id=DatasetId("ds-load"),
        source=SourceConfig(
            source_id="src-load-1",
            source_name="load_source",
            source_type="local_file",
            config={"path": "input.csv"},
        ),
        destination=StorageLocation(
            layer="bronze",
            path=dest_path,
            format=dest_format,
        ),
        source_format="csv",
        execution_config=ExecutionConfig(
            partition_by_date=partition_by_date,
            overwrite=overwrite,
        ),
    )


@pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
def test_loader_persists_supported_formats(fmt: str) -> None:
    provider = InMemoryStorageProvider()
    loader = Loader(storage_manager=StorageManager(backend=provider))
    df = pd.DataFrame([{"id": 1, "city": "Toronto"}, {"id": 2, "city": "Ottawa"}])
    extraction = ExtractionResult(
        source_id="src-load-1",
        row_count=2,
        extracted_at=datetime(2026, 3, 15, 10, 0, tzinfo=UTC),
        data=df,
    )

    dest_path = f"data/bronze/cities.{fmt}"
    request = _make_request(dest_path=dest_path, dest_format=fmt)
    artifacts = loader.load(request, extraction)

    assert len(artifacts) == 1
    artifact = artifacts[0]
    assert artifact.dataset_id == "ds-load"
    assert artifact.path == dest_path
    assert artifact.format == fmt
    assert dest_path in provider.store
    assert len(provider.store[dest_path]) > 0


def test_loader_applies_date_partitioning() -> None:
    provider = InMemoryStorageProvider()
    loader = Loader(storage_provider=provider)
    extracted_at = datetime(2026, 5, 20, 8, 30, tzinfo=UTC)
    extraction = ExtractionResult(
        source_id="src-load-1",
        row_count=1,
        extracted_at=extracted_at,
        data=[{"id": 1}],
    )

    request = _make_request(
        dest_path="data/bronze/events.json",
        dest_format="json",
        partition_by_date=True,
    )
    artifacts = loader.load(request, extraction, execution_timestamp=extracted_at)

    assert len(artifacts) == 1
    assert "year=2026" in artifacts[0].path
    assert "month=05" in artifacts[0].path
    assert "day=20" in artifacts[0].path
    assert artifacts[0].path.endswith("events.json")


def test_loader_rejects_existing_destination_when_overwrite_disabled() -> None:
    provider = InMemoryStorageProvider()
    provider.store["data/bronze/items.csv"] = b"existing"
    loader = Loader(storage_provider=provider)
    extraction = ExtractionResult(
        source_id="src-load-1",
        row_count=1,
        extracted_at=datetime.now(UTC),
        data=[{"id": 1}],
    )

    request = _make_request(overwrite=False)
    with pytest.raises(LoadingError, match="already exists and overwrite is disabled"):
        loader.load(request, extraction)


def test_loader_chains_storage_exception_on_failure() -> None:
    provider = InMemoryStorageProvider(fail_on_save=True)
    loader = Loader(storage_provider=provider)
    extraction = ExtractionResult(
        source_id="src-load-1",
        row_count=1,
        extracted_at=datetime.now(UTC),
        data=[{"id": 1}],
    )

    with pytest.raises(LoadingError, match="Storage error while persisting") as exc_info:
        loader.load(_make_request(), extraction)

    assert isinstance(exc_info.value.__cause__, StorageWriteError)
