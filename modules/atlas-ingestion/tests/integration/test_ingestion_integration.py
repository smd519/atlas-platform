"""Integration tests for atlas-ingestion with atlas-connectors and atlas-storage."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import pandas as pd  # type: ignore[import-untyped]
import pytest
from atlas_connectors import (
    ConnectorRegistry,
    LocalFileConnector,
    RestConnector,
    SourceConfig,
)
from atlas_core import (
    BRONZE,
    DatasetId,
    ExecutionStatus,
    StorageProvider,
)
from atlas_ingestion import (
    ExecutionConfig,
    IngestionRequest,
    IngestionService,
    IngestionState,
)
from atlas_storage import (
    FileSystemBackend,
    StorageLocation,
    StorageManager,
    StorageRegistry,
    StorageWriteError,
    write_csv,
    write_json,
    write_parquet,
)


def _sample_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"station_id": "STN-01", "temperature_c": 21.5, "city": "Toronto"},
            {"station_id": "STN-02", "temperature_c": 18.0, "city": "Montreal"},
            {"station_id": "STN-03", "temperature_c": 15.2, "city": "Vancouver"},
        ]
    )


class _FailingWriteBackend(StorageProvider):
    """Storage backend that fails on save to verify end-to-end storage failure handling."""

    def save(self, data: Any, path: str) -> None:
        raise StorageWriteError(f"Physical storage write failure for '{path}'")

    def load(self, path: str) -> bytes:
        raise NotImplementedError

    def exists(self, path: str) -> bool:
        return False


@pytest.mark.parametrize(
    ("source_format", "dest_format"),
    [
        ("csv", "parquet"),
        ("json", "csv"),
        ("parquet", "json"),
        ("csv", "csv"),
    ],
)
def test_end_to_end_local_file_ingestion_across_formats(
    tmp_path: Path,
    source_format: str,
    dest_format: str,
) -> None:
    df = _sample_dataframe()
    source_file = tmp_path / "source" / f"weather.{source_format}"
    source_file.parent.mkdir(parents=True, exist_ok=True)

    if source_format == "csv":
        write_csv(df, str(source_file))
    elif source_format == "json":
        write_json(df, str(source_file))
    elif source_format == "parquet":
        write_parquet(df, str(source_file))

    dest_file = tmp_path / "bronze" / f"weather_raw.{dest_format}"

    connector_registry = ConnectorRegistry()
    connector_registry.register("local_file", LocalFileConnector)

    storage_registry = StorageRegistry()
    storage_registry.register("filesystem", FileSystemBackend)

    service = IngestionService(
        connector_registry=connector_registry,
        storage_registry=storage_registry,
    )

    request = IngestionRequest(
        request_id=f"req-{source_format}-to-{dest_format}",
        dataset_id=DatasetId("weather_observations"),
        source=SourceConfig(
            source_id="env-canada-local",
            source_name="Environment Canada Sample",
            source_type="local_file",
            config={"path": str(source_file)},
        ),
        destination=StorageLocation(
            layer=BRONZE,
            path=str(dest_file),
            format=dest_format,
        ),
        source_format=source_format,
        execution_config=ExecutionConfig(
            validate_connection=True,
            partition_by_date=True,
        ),
    )

    result = service.ingest(request)

    assert result.success is True
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.lifecycle_state == IngestionState.COMPLETED
    assert result.row_count == 3
    assert len(result.artifacts) == 1

    persisted_path = Path(result.artifacts[0].path)
    assert persisted_path.exists()
    assert "year=" in str(persisted_path)

    storage_manager = StorageManager(backend=FileSystemBackend())
    loaded_df = storage_manager.load(
        StorageLocation(
            layer=BRONZE,
            path=str(persisted_path),
            format=dest_format,
        )
    )

    assert len(loaded_df) == 3
    assert list(loaded_df["station_id"]) == ["STN-01", "STN-02", "STN-03"]


def test_integration_raw_byte_ingestion(tmp_path: Path) -> None:
    raw_source = tmp_path / "incoming" / "sensor_dump.bin"
    raw_source.parent.mkdir(parents=True, exist_ok=True)
    raw_payload = b"\x89ATLAS\r\n\x1a\nbinary-telemetry-payload"
    raw_source.write_bytes(raw_payload)

    dest_path = tmp_path / "bronze" / "sensor_dump.bin"
    service = IngestionService()

    request = IngestionRequest(
        request_id="req-raw-bytes",
        dataset_id=DatasetId("sensor_telemetry"),
        source=SourceConfig(
            source_id="sensor-local",
            source_name="Raw Sensor Binary",
            source_type="local_file",
            config={"path": str(raw_source)},
        ),
        destination=StorageLocation(
            layer=BRONZE,
            path=str(dest_path),
            format="raw",
        ),
        source_format="raw",
    )

    result = service.ingest(request, execution_id="exec-raw-1")
    assert result.success is True
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.row_count == 1
    assert dest_path.exists()

    storage_manager = StorageManager(backend=FileSystemBackend())
    loaded_bytes = storage_manager.load(request.destination)
    assert loaded_bytes == raw_payload


def test_integration_missing_source_file_produces_failed_result(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing" / "does_not_exist.csv"
    dest_file = tmp_path / "bronze" / "should_not_exist.parquet"

    service = IngestionService(
        storage_manager=StorageManager(backend=FileSystemBackend()),
    )

    request = IngestionRequest(
        request_id="req-missing-file",
        dataset_id=DatasetId("missing_dataset"),
        source=SourceConfig(
            source_id="missing-src",
            source_name="Missing Local File",
            source_type="local_file",
            config={"path": str(missing_file)},
        ),
        destination=StorageLocation(
            layer=BRONZE,
            path=str(dest_file),
            format="parquet",
        ),
        source_format="csv",
    )

    result = service.ingest(request, execution_id="exec-missing-1")

    assert result.success is False
    assert result.status == ExecutionStatus.FAILED
    assert result.lifecycle_state == IngestionState.FAILED
    assert result.execution_id == "exec-missing-1"
    assert result.artifacts == []
    assert not dest_file.exists()

    tracked = service.get_execution("exec-missing-1")
    assert tracked.status == ExecutionStatus.FAILED
    assert tracked.lifecycle_state == IngestionState.FAILED


def test_integration_storage_failure_and_execution_failure_tracking(tmp_path: Path) -> None:
    df = _sample_dataframe()
    source_file = tmp_path / "source" / "valid.csv"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    write_csv(df, str(source_file))

    dest_file = tmp_path / "bronze" / "failed_write.parquet"
    service = IngestionService(
        storage_manager=StorageManager(backend=_FailingWriteBackend()),
    )

    request = IngestionRequest(
        request_id="req-storage-fail",
        dataset_id=DatasetId("weather_observations"),
        source=SourceConfig(
            source_id="valid-csv",
            source_name="Valid CSV Source",
            source_type="local_file",
            config={"path": str(source_file)},
        ),
        destination=StorageLocation(
            layer=BRONZE,
            path=str(dest_file),
            format="parquet",
        ),
        source_format="csv",
    )

    result = service.ingest(request, execution_id="exec-storage-fail-1")

    assert result.success is False
    assert result.status == ExecutionStatus.FAILED
    assert result.lifecycle_state == IngestionState.FAILED
    assert result.execution_id == "exec-storage-fail-1"
    assert result.artifacts == []
    assert result.error_type == "LoadingError"
    assert "Physical storage write failure" in (result.error_message or "")

    tracked = service.get_execution("exec-storage-fail-1")
    assert tracked.status == ExecutionStatus.FAILED
    assert tracked.lifecycle_state == IngestionState.FAILED
    assert tracked.state_history == [
        IngestionState.PENDING,
        IngestionState.EXTRACTING,
        IngestionState.EXTRACTED,
        IngestionState.LOADING,
        IngestionState.FAILED,
    ]


class _MockHttpStreamResponse:
    """Mock httpx.stream response for remote file ingestion tests."""

    def __init__(
        self,
        url: str,
        chunks: list[bytes],
        *,
        status_code: int = 200,
        timeout_after_first_chunk: bool = False,
    ) -> None:
        self.url = url
        self._chunks = list(chunks)
        self.status_code = status_code
        self._timeout_after_first_chunk = timeout_after_first_chunk

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            req = httpx.Request("GET", self.url)
            resp = httpx.Response(self.status_code, request=req)
            raise httpx.HTTPStatusError(f"HTTP {self.status_code}", request=req, response=resp)

    def iter_bytes(self) -> Iterator[bytes]:
        for idx, chunk in enumerate(self._chunks):
            yield chunk
            if idx == 0 and self._timeout_after_first_chunk:
                req = httpx.Request("GET", self.url)
                raise httpx.ReadTimeout("Simulated stream interruption", request=req)


def test_integration_remote_rest_csv_ingestion_stages_loads_bronze_and_cleans_up(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_staging_file = (
        tmp_path / "drop" / ".staging" / "exec-rest-csv-1" / "corporations-active-cbca-en.csv"
    ).resolve()
    observed_during_save: dict[str, bool] = {}

    class InspectingFileSystemBackend(FileSystemBackend):
        def save(self, data: Any, path: str) -> None:
            # Verify staging file still exists on disk right when Bronze persistence runs
            observed_during_save["staged_file_existed_before_bronze_save"] = (
                expected_staging_file.exists()
            )
            super().save(data, path)

    @contextmanager
    def fake_stream(
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Iterator[_MockHttpStreamResponse]:
        yield _MockHttpStreamResponse(
            url,
            chunks=[
                b"corporation_id,name,status\n",
                b"1001,Maple Robotics Inc.,Active\n",
                b"1002,Northern Photonics Ltd.,Active\n",
            ],
        )

    monkeypatch.setattr(httpx, "stream", fake_stream)

    # Create another execution's staging directory to verify execution isolation
    other_exec_staging_dir = tmp_path / "drop" / ".staging" / "exec-other-999"
    other_exec_staging_dir.mkdir(parents=True, exist_ok=True)
    other_exec_file = other_exec_staging_dir / "other.csv"
    other_exec_file.write_text("id\n999\n", encoding="utf-8")

    dest_file = tmp_path / "data" / "bronze" / "corporations_cbca.parquet"
    service = IngestionService(
        storage_manager=StorageManager(backend=InspectingFileSystemBackend()),
        workspace=tmp_path,
    )

    request = IngestionRequest(
        request_id="req-rest-csv-1",
        dataset_id=DatasetId("corporations_cbca"),
        source=SourceConfig(
            source_id="corporations_cbca_csv",
            source_name="Corporations Active CBCA CSV",
            source_type="rest",
            config={
                "base_url": "https://example.com",
                "endpoint": "/downloads/corporations-active-cbca-en.csv",
                "response_mode": "file",
                "download_filename": "corporations-active-cbca-en.csv",
            },
        ),
        destination=StorageLocation(
            layer=BRONZE,
            path=str(dest_file),
            format="parquet",
        ),
        source_format="csv",
    )

    result = service.ingest(request, execution_id="exec-rest-csv-1")

    assert result.success is True
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.lifecycle_state == IngestionState.COMPLETED
    assert result.row_count == 2
    assert len(result.artifacts) == 1
    assert observed_during_save == {"staged_file_existed_before_bronze_save": True}

    # Bronze file was persisted and contains the parsed records
    assert dest_file.exists()
    loaded_df = StorageManager(backend=FileSystemBackend()).load(request.destination)
    assert len(loaded_df) == 2
    assert list(loaded_df["corporation_id"]) == [1001, 1002]

    # Current execution's staging directory was cleaned up after Bronze persistence succeeded
    assert not expected_staging_file.exists()
    assert not expected_staging_file.parent.exists()

    # Another execution's staging files remain completely untouched
    assert other_exec_file.exists()
    assert other_exec_file.read_text(encoding="utf-8") == "id\n999\n"


def test_integration_remote_rest_failed_download_cleans_up_part_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @contextmanager
    def fake_stream(
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Iterator[_MockHttpStreamResponse]:
        yield _MockHttpStreamResponse(
            url,
            chunks=[b"corporation_id,name\n1001,Partial Row\n", b"1002,Never Reached\n"],
            timeout_after_first_chunk=True,
        )

    monkeypatch.setattr(httpx, "stream", fake_stream)

    dest_file = tmp_path / "data" / "bronze" / "corporations_failed.parquet"
    service = IngestionService(workspace=tmp_path)

    request = IngestionRequest(
        request_id="req-rest-dl-fail",
        dataset_id=DatasetId("corporations_cbca"),
        source=SourceConfig(
            source_id="corporations_cbca_csv",
            source_name="Corporations Active CBCA CSV",
            source_type="rest",
            config={
                "base_url": "https://example.com",
                "endpoint": "/downloads/corporations-active-cbca-en.csv",
                "response_mode": "file",
                "download_filename": "corporations-active-cbca-en.csv",
            },
        ),
        destination=StorageLocation(
            layer=BRONZE,
            path=str(dest_file),
            format="parquet",
        ),
        source_format="csv",
    )

    result = service.ingest(request, execution_id="exec-dl-fail-1")

    assert result.success is False
    assert result.status == ExecutionStatus.FAILED
    assert result.error_type == "ExtractionError"
    assert not dest_file.exists()

    staging_dir = tmp_path / "drop" / ".staging" / "exec-dl-fail-1"
    part_files = list(staging_dir.glob("*.part")) if staging_dir.exists() else []
    assert part_files == []
    assert not (staging_dir / "corporations-active-cbca-en.csv").exists()


def test_integration_remote_rest_failed_bronze_load_preserves_diagnostic_staging_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @contextmanager
    def fake_stream(
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Iterator[_MockHttpStreamResponse]:
        yield _MockHttpStreamResponse(
            url,
            chunks=[b"corporation_id,name\n1001,Diagnostic Corp\n"],
        )

    monkeypatch.setattr(httpx, "stream", fake_stream)

    dest_file = tmp_path / "data" / "bronze" / "corporations_load_fail.parquet"
    connector_registry = ConnectorRegistry()
    connector_registry.register("rest", RestConnector)

    service = IngestionService(
        connector_registry=connector_registry,
        storage_manager=StorageManager(backend=_FailingWriteBackend()),
        workspace=tmp_path,
    )

    request = IngestionRequest(
        request_id="req-rest-load-fail",
        dataset_id=DatasetId("corporations_cbca"),
        source=SourceConfig(
            source_id="corporations_cbca_csv",
            source_name="Corporations Active CBCA CSV",
            source_type="rest",
            config={
                "base_url": "https://example.com",
                "endpoint": "/downloads/corporations-active-cbca-en.csv",
                "response_mode": "file",
                "download_filename": "corporations-active-cbca-en.csv",
            },
        ),
        destination=StorageLocation(
            layer=BRONZE,
            path=str(dest_file),
            format="parquet",
        ),
        source_format="csv",
    )

    result = service.ingest(request, execution_id="exec-load-fail-diag")

    assert result.success is False
    assert result.status == ExecutionStatus.FAILED
    assert result.error_type == "LoadingError"

    expected_staging_file = (
        tmp_path / "drop" / ".staging" / "exec-load-fail-diag" / "corporations-active-cbca-en.csv"
    ).resolve()
    assert expected_staging_file.exists()
    assert (
        expected_staging_file.read_text(encoding="utf-8")
        == "corporation_id,name\n1001,Diagnostic Corp\n"
    )
    assert result.metadata.get("staging_path") == str(expected_staging_file)
