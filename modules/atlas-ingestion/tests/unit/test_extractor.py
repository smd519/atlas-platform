"""Unit tests for the Extractor component using fake connectors."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from atlas_connectors import (
    ConnectorExtractionError,
    ConnectorFactory,
    ConnectorRegistry,
    ExtractionResult,
    SourceConfig,
)
from atlas_core import Connector, DatasetId
from atlas_ingestion.exceptions import ExtractionError
from atlas_ingestion.extraction.extractor import Extractor
from atlas_ingestion.models.ingestion_request import IngestionRequest
from atlas_storage import StorageLocation


class FakeSuccessConnector(Connector):
    """Fake connector returning in-memory records."""

    def __init__(self, records: list[dict[str, Any]] | None = None) -> None:
        self._records = records if records is not None else [{"id": 1, "name": "alpha"}]

    @property
    def source_name(self) -> str:
        return "fake_memory_source"

    def validate_connection(self) -> bool:
        return True

    def extract(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self._records


class FakeInvalidConnectionConnector(Connector):
    """Fake connector whose connection validation fails."""

    @property
    def source_name(self) -> str:
        return "broken_source"

    def validate_connection(self) -> bool:
        return False

    def extract(self, **kwargs: Any) -> Any:
        return []


class FakeFailingConnector(Connector):
    """Fake connector that raises a ConnectorExtractionError."""

    @property
    def source_name(self) -> str:
        return "failing_source"

    def validate_connection(self) -> bool:
        return True

    def extract(self, **kwargs: Any) -> Any:
        raise ConnectorExtractionError("upstream endpoint returned 503")


class FakeExtractionResultConnector(Connector):
    """Fake connector returning a pre-built ExtractionResult."""

    @property
    def source_name(self) -> str:
        return "prebuilt_result_source"

    def validate_connection(self) -> bool:
        return True

    def extract(self, **kwargs: Any) -> ExtractionResult:
        return ExtractionResult(
            source_id="src-prebuilt",
            row_count=2,
            extracted_at=datetime(2026, 2, 1, 0, 0, tzinfo=UTC),
            data=[{"x": 1}, {"x": 2}],
        )


def _make_request(source_type: str, config: dict[str, object] | None = None) -> IngestionRequest:
    return IngestionRequest(
        request_id="req-ext-1",
        dataset_id=DatasetId("ds-ext"),
        source=SourceConfig(
            source_id="src-ext-1",
            source_name="ext_source",
            source_type=source_type,
            config=config or {},
        ),
        destination=StorageLocation(
            layer="bronze",
            path="data/bronze/ext.json",
            format="json",
        ),
        source_format="json",
    )


def test_extractor_success_and_source_metadata() -> None:
    registry = ConnectorRegistry()
    registry.register("fake_ok", FakeSuccessConnector)
    extractor = Extractor(connector_factory=ConnectorFactory(registry))

    request = _make_request("fake_ok", {"records": [{"a": 1}, {"a": 2}, {"a": 3}]})
    result = extractor.extract(request)

    assert isinstance(result, ExtractionResult)
    assert result.source_id == "src-ext-1"
    assert result.row_count == 3
    assert result.data == [{"a": 1}, {"a": 2}, {"a": 3}]

    metadata = extractor.build_source_metadata(request, result)
    assert metadata["source_id"] == "src-ext-1"
    assert metadata["source_type"] == "fake_ok"
    assert metadata["extracted_row_count"] == 3


def test_extractor_preserves_prebuilt_extraction_result() -> None:
    registry = ConnectorRegistry()
    registry.register("fake_prebuilt", FakeExtractionResultConnector)
    extractor = Extractor(connector_registry=registry)

    result = extractor.extract(_make_request("fake_prebuilt"))
    assert result.source_id == "src-prebuilt"
    assert result.row_count == 2


def test_extractor_fails_on_unregistered_connector() -> None:
    extractor = Extractor(connector_registry=ConnectorRegistry())
    with pytest.raises(ExtractionError, match="Failed to resolve or initialize connector"):
        extractor.extract(_make_request("unknown_type"))


def test_extractor_fails_on_connection_validation_failure() -> None:
    registry = ConnectorRegistry()
    registry.register("fake_invalid_conn", FakeInvalidConnectionConnector)
    extractor = Extractor(connector_registry=registry)

    with pytest.raises(ExtractionError, match="Connection validation failed"):
        extractor.extract(_make_request("fake_invalid_conn"))


def test_extractor_chains_connector_exception_on_failure() -> None:
    registry = ConnectorRegistry()
    registry.register("fake_fail", FakeFailingConnector)
    extractor = Extractor(connector_registry=registry)

    with pytest.raises(ExtractionError, match="Connector error while extracting") as exc_info:
        extractor.extract(_make_request("fake_fail"))

    assert isinstance(exc_info.value.__cause__, ConnectorExtractionError)


def test_extractor_creates_execution_staging_dir_and_passes_destination_for_remote_file(
    tmp_path: Path,
) -> None:
    received_destinations: list[Path | None] = []

    class FakeStagingRestConnector(Connector):
        def __init__(self, base_url: str, endpoint: str = "") -> None:
            self.base_url = base_url
            self.endpoint = endpoint

        @property
        def source_name(self) -> str:
            return f"{self.base_url}{self.endpoint}"

        def validate_connection(self) -> bool:
            return True

        def extract(
            self,
            *,
            destination: Path | None = None,
            **kwargs: Any,
        ) -> Any:
            received_destinations.append(destination)
            assert destination is not None
            assert destination.parent.exists()
            destination.write_text("corp_id,name\n100,Atlas Ltd\n200,North Inc\n", encoding="utf-8")
            return destination

    registry = ConnectorRegistry()
    registry.register("rest", FakeStagingRestConnector)
    extractor = Extractor(connector_registry=registry, workspace=tmp_path)

    request = IngestionRequest(
        request_id="req-stage-001",
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
            layer="bronze",
            path=str(tmp_path / "data" / "bronze" / "corporations.parquet"),
            format="parquet",
        ),
        source_format="csv",
    )

    result = extractor.extract(request, execution_id="exec-stage-123")
    expected_staging_file = (
        tmp_path / "drop" / ".staging" / "exec-stage-123" / "corporations-active-cbca-en.csv"
    ).resolve()

    assert received_destinations == [expected_staging_file]
    assert expected_staging_file.exists()
    assert extractor.get_staging_directory("exec-stage-123") == expected_staging_file.parent
    assert extractor.get_staged_file("exec-stage-123") == expected_staging_file
    assert result.row_count == 2
    assert list(result.data["corp_id"]) == [100, 200]

    meta = extractor.build_source_metadata(request, result, execution_id="exec-stage-123")
    assert meta["staging_path"] == str(expected_staging_file)


def test_extractor_does_not_create_staging_dir_for_local_file_or_rest_json(
    tmp_path: Path,
) -> None:
    local_csv = tmp_path / "drop" / "local_corporations.csv"
    local_csv.parent.mkdir(parents=True, exist_ok=True)
    local_csv.write_text("id,name\n1,Local Co\n", encoding="utf-8")

    extractor = Extractor(workspace=tmp_path)
    local_request = IngestionRequest(
        request_id="req-local-1",
        dataset_id=DatasetId("local_corps"),
        source=SourceConfig(
            source_id="corporations_cbca_local",
            source_name="Corporations Active CBCA CSV",
            source_type="local_file",
            config={"path": str(local_csv)},
        ),
        destination=StorageLocation(
            layer="bronze",
            path=str(tmp_path / "data" / "bronze" / "local_corps.parquet"),
            format="parquet",
        ),
        source_format="csv",
    )

    result = extractor.extract(local_request, execution_id="exec-local-1")
    assert result.row_count == 1
    assert extractor.get_staging_directory("exec-local-1") is None
    assert not (tmp_path / "drop" / ".staging").exists()
    assert local_csv.exists()
