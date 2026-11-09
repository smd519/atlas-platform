"""Unit tests for atlas-connectors contracts, registry, factory, and REST streaming/adapter."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from atlas_connectors import (
    ApiKeyAuth,
    BearerTokenAuth,
    ConnectorAuthenticationError,
    ConnectorConfigurationError,
    ConnectorConnectionError,
    ConnectorExtractionError,
    ConnectorFactory,
    ConnectorRegistry,
    LocalFileConnector,
    ParserError,
    RestConnector,
    RestConnectorAdapter,
    SourceConfig,
    parse_csv,
)
from atlas_core import Connector


class _FakeStreamResponse:
    """Controllable mock for httpx.stream() response context manager."""

    def __init__(
        self,
        url: str,
        *,
        status_code: int = 200,
        chunks: list[bytes] | None = None,
        error_after_first_chunk: Exception | None = None,
        on_chunk_yielded: Any = None,
    ) -> None:
        self.url = url
        self.status_code = status_code
        self._chunks = list(chunks or [])
        self._error_after_first_chunk = error_after_first_chunk
        self._on_chunk_yielded = on_chunk_yielded
        self.closed = False

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            req = httpx.Request("GET", self.url)
            resp = httpx.Response(self.status_code, request=req)
            raise httpx.HTTPStatusError(
                f"HTTP {self.status_code}",
                request=req,
                response=resp,
            )

    def iter_bytes(self) -> Iterator[bytes]:
        for idx, chunk in enumerate(self._chunks):
            yield chunk
            if self._on_chunk_yielded is not None:
                self._on_chunk_yielded(idx)
            if idx == 0 and self._error_after_first_chunk is not None:
                raise self._error_after_first_chunk

    def json(self) -> Any:
        raise AssertionError("response.json() must not be called in streaming mode")

    @property
    def content(self) -> bytes:
        raise AssertionError("response.content must not be accessed in streaming mode")


def test_registry_and_factory_create_from_source_config(tmp_path: Path) -> None:
    sample_file = tmp_path / "data.csv"
    sample_file.write_text("id,name\n1,a\n", encoding="utf-8")

    registry = ConnectorRegistry()
    registry.register("local_file", LocalFileConnector)
    registry.register("rest", RestConnectorAdapter)

    assert registry.exists("local_file") is True
    assert set(registry.list_connectors()) == {"local_file", "rest"}

    factory = ConnectorFactory(registry)
    cfg = SourceConfig(
        source_id="src-local",
        source_name="Local CSV",
        source_type="local_file",
        config={"path": str(sample_file)},
    )
    connector = factory.from_source_config(cfg)
    assert isinstance(connector, Connector)
    assert connector.validate_connection() is True
    assert connector.extract() == sample_file


def test_registry_rejects_invalid_registration_and_unknown_types() -> None:
    registry = ConnectorRegistry()
    with pytest.raises(ConnectorConfigurationError):
        registry.register("", LocalFileConnector)

    with pytest.raises(ConnectorConfigurationError):
        registry.register("invalid", object)  # type: ignore[arg-type]

    with pytest.raises(ConnectorConfigurationError, match="not registered"):
        registry.get("nonexistent")


def test_local_file_connector_validation_missing_file_and_destination_ignored(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConnectorConfigurationError):
        LocalFileConnector("")

    missing = LocalFileConnector(tmp_path / "missing.csv")
    assert missing.validate_connection() is False
    with pytest.raises(ConnectorExtractionError):
        missing.extract()

    source_file = tmp_path / "original.csv"
    source_file.write_text("col1,col2\n1,2\n", encoding="utf-8")
    ignored_destination = tmp_path / "staging" / "copied.csv"

    local = LocalFileConnector(source_file)
    assert local.extract() == source_file
    assert local.extract(destination=ignored_destination) == source_file
    assert not ignored_destination.exists()


def test_rest_connector_and_adapter_implement_standard_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_get(url: str, headers: dict[str, str], timeout: float) -> httpx.Response:
        captured["url"] = url
        captured["headers"] = headers
        captured["timeout"] = timeout
        req = httpx.Request("GET", url)
        return httpx.Response(200, json=[{"id": 10, "status": "active"}], request=req)

    monkeypatch.setattr(httpx, "get", fake_get)

    auth = BearerTokenAuth("secret-token")
    rest = RestConnector(base_url="https://api.example.com", auth=auth, endpoint="/v1/items")
    assert isinstance(rest, Connector)
    assert rest.validate_connection() is True
    assert rest.source_name == "https://api.example.com/v1/items"

    # Legacy direct .get() call
    direct_payload = rest.get("/v1/items")
    assert direct_payload == [{"id": 10, "status": "active"}]
    assert captured["headers"] == {"Authorization": "Bearer secret-token"}

    # Backward-compatible .extract() without destination returns parsed JSON
    extracted_json = rest.extract()
    assert extracted_json == [{"id": 10, "status": "active"}]

    # Adapter contract .extract() call
    adapter = RestConnectorAdapter(
        base_url="https://api.example.com",
        auth=ApiKeyAuth("X-API-Key", "key-123"),
        endpoint="/v1/stations",
    )
    assert isinstance(adapter, Connector)
    assert adapter.validate_connection() is True
    assert adapter.source_name == "https://api.example.com/v1/stations"
    extracted = adapter.extract()
    assert extracted == [{"id": 10, "status": "active"}]
    assert captured["url"] == "https://api.example.com/v1/stations"
    assert captured["headers"] == {"X-API-Key": "key-123"}


def test_rest_connector_error_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_401(url: str, headers: dict[str, str], timeout: float) -> httpx.Response:
        req = httpx.Request("GET", url)
        return httpx.Response(401, json={"error": "unauthorized"}, request=req)

    monkeypatch.setattr(httpx, "get", fake_401)
    connector = RestConnector(base_url="https://api.example.com", endpoint="/secure")
    with pytest.raises(ConnectorAuthenticationError):
        connector.extract()

    def fake_500(url: str, headers: dict[str, str], timeout: float) -> httpx.Response:
        req = httpx.Request("GET", url)
        return httpx.Response(500, json={"error": "server error"}, request=req)

    monkeypatch.setattr(httpx, "get", fake_500)
    with pytest.raises(ConnectorExtractionError):
        connector.extract()

    def fake_conn_err(url: str, headers: dict[str, str], timeout: float) -> httpx.Response:
        req = httpx.Request("GET", url)
        raise httpx.ConnectError("DNS lookup failed", request=req)

    monkeypatch.setattr(httpx, "get", fake_conn_err)
    with pytest.raises(ConnectorConnectionError):
        connector.extract()


def test_rest_connector_streams_to_destination_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging_dir = tmp_path / "drop" / ".staging" / "exec-001"
    staging_dir.mkdir(parents=True, exist_ok=True)
    destination = staging_dir / "corporations.csv"
    part_path = destination.with_suffix(destination.suffix + ".part")

    chunks = [b"id,name\n", b"1,Acme Corp\n", b"2,Globex Inc\n"]
    observed_mid_stream: dict[str, bool] = {}
    captured_req: dict[str, Any] = {}

    def record_mid_stream(idx: int) -> None:
        if idx == 1:
            # After chunk 0 has been written to disk, .part exists while destination does not
            observed_mid_stream["part_exists"] = part_path.exists()
            observed_mid_stream["dest_exists"] = destination.exists()

    @contextmanager
    def fake_stream(
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Iterator[_FakeStreamResponse]:
        captured_req["method"] = method
        captured_req["url"] = url
        captured_req["headers"] = headers
        captured_req["timeout"] = timeout
        resp = _FakeStreamResponse(
            url,
            status_code=200,
            chunks=chunks,
            on_chunk_yielded=record_mid_stream,
        )
        try:
            yield resp
        finally:
            resp.closed = True
            captured_req["closed"] = resp.closed

    def forbidden_get(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("httpx.get must not be called when destination is provided")

    monkeypatch.setattr(httpx, "stream", fake_stream)
    monkeypatch.setattr(httpx, "get", forbidden_get)

    connector = RestConnector(
        base_url="https://api.example.com",
        auth=BearerTokenAuth("stream-token"),
        endpoint="/exports/corporations.csv",
        timeout=45.0,
    )
    returned_path = connector.extract(destination=destination)

    assert returned_path == destination
    assert isinstance(returned_path, Path)
    assert captured_req["method"] == "GET"
    assert captured_req["url"] == "https://api.example.com/exports/corporations.csv"
    assert captured_req["headers"] == {"Authorization": "Bearer stream-token"}
    assert captured_req["timeout"] == 45.0
    assert captured_req["closed"] is True
    assert observed_mid_stream == {"part_exists": True, "dest_exists": False}
    assert destination.exists() is True
    assert part_path.exists() is False
    assert destination.read_bytes() == b"".join(chunks)


def test_rest_connector_cleans_up_part_file_on_http_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "sensors.csv"
    part_path = destination.with_suffix(destination.suffix + ".part")
    req = httpx.Request("GET", "https://api.example.com/sensors.csv")
    timeout_exc = httpx.ReadTimeout("Stream timed out mid-download", request=req)
    part_existed_before_timeout = False

    def check_part_written(idx: int) -> None:
        nonlocal part_existed_before_timeout
        if idx == 0:
            part_existed_before_timeout = part_path.exists()

    @contextmanager
    def fake_stream(
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Iterator[_FakeStreamResponse]:
        yield _FakeStreamResponse(
            url,
            status_code=200,
            chunks=[b"station_id,reading\n101,42.0\n", b"102,43.5\n"],
            error_after_first_chunk=timeout_exc,
            on_chunk_yielded=check_part_written,
        )

    monkeypatch.setattr(httpx, "stream", fake_stream)

    connector = RestConnector(base_url="https://api.example.com", endpoint="/sensors.csv")
    with pytest.raises(ConnectorConnectionError) as exc_info:
        connector.extract(destination=destination)

    assert exc_info.value.__cause__ is timeout_exc
    assert part_existed_before_timeout is True
    assert part_path.exists() is False
    assert destination.exists() is False


def test_rest_connector_fails_if_destination_parent_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    http_called = False

    @contextmanager
    def fake_stream(*args: Any, **kwargs: Any) -> Iterator[Any]:
        nonlocal http_called
        http_called = True
        yield _FakeStreamResponse("https://api.example.com/data.csv")

    monkeypatch.setattr(httpx, "stream", fake_stream)

    missing_parent_dest = tmp_path / "nonexistent_staging_dir" / "data.csv"
    part_path = missing_parent_dest.with_suffix(missing_parent_dest.suffix + ".part")

    connector = RestConnector(base_url="https://api.example.com", endpoint="/data.csv")
    with pytest.raises(ConnectorConfigurationError, match="parent directory does not exist"):
        connector.extract(destination=missing_parent_dest)

    assert http_called is False
    assert missing_parent_dest.parent.exists() is False
    assert missing_parent_dest.exists() is False
    assert part_path.exists() is False


@pytest.mark.parametrize(
    ("status_code", "expected_exc"),
    [
        (401, ConnectorAuthenticationError),
        (403, ConnectorAuthenticationError),
        (500, ConnectorExtractionError),
    ],
)
def test_rest_connector_streaming_http_status_errors_and_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    expected_exc: type[Exception],
) -> None:
    destination = tmp_path / "payload.csv"
    part_path = destination.with_suffix(destination.suffix + ".part")
    # Pre-create a stale .part file to verify cleanup on HTTP status failure
    part_path.write_bytes(b"stale partial content")

    @contextmanager
    def fake_stream(
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Iterator[_FakeStreamResponse]:
        yield _FakeStreamResponse(url, status_code=status_code)

    monkeypatch.setattr(httpx, "stream", fake_stream)

    connector = RestConnector(base_url="https://api.example.com", endpoint="/payload.csv")
    with pytest.raises(expected_exc) as exc_info:
        connector.extract(destination=destination)

    assert isinstance(exc_info.value.__cause__, httpx.HTTPStatusError)
    assert part_path.exists() is False
    assert destination.exists() is False


def test_rest_connector_preserves_existing_destination_on_failed_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "existing.csv"
    original_bytes = b"id,value\n1,original_intact\n"
    destination.write_bytes(original_bytes)
    part_path = destination.with_suffix(destination.suffix + ".part")

    req = httpx.Request("GET", "https://api.example.com/existing.csv")
    read_timeout = httpx.ReadTimeout("Dropped connection during refresh", request=req)

    @contextmanager
    def fake_stream(
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Iterator[_FakeStreamResponse]:
        yield _FakeStreamResponse(
            url,
            status_code=200,
            chunks=[b"id,value\n1,corrupted_partial\n", b"2,never_finished\n"],
            error_after_first_chunk=read_timeout,
        )

    monkeypatch.setattr(httpx, "stream", fake_stream)

    connector = RestConnector(base_url="https://api.example.com", endpoint="/existing.csv")
    with pytest.raises(ConnectorConnectionError):
        connector.extract(destination=destination)

    assert destination.exists() is True
    assert destination.read_bytes() == original_bytes
    assert part_path.exists() is False


def test_rest_connector_adapter_delegates_streaming_to_wrapped_connector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "adapter_download.json"
    captured: dict[str, Any] = {}

    @contextmanager
    def fake_stream(
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Iterator[_FakeStreamResponse]:
        captured["url"] = url
        captured["headers"] = headers
        captured["timeout"] = timeout
        yield _FakeStreamResponse(
            url,
            status_code=200,
            chunks=[b'[{"id": 1,', b' "name": "Sensor A"}]'],
        )

    monkeypatch.setattr(httpx, "stream", fake_stream)

    adapter = RestConnectorAdapter(
        base_url="https://api.example.com",
        auth=ApiKeyAuth("X-API-Key", "adapter-key"),
        endpoint="/v1/default",
        timeout=30.0,
    )
    result_path = adapter.extract(destination=destination, endpoint="/v1/override")

    assert result_path == destination
    assert destination.read_bytes() == b'[{"id": 1, "name": "Sensor A"}]'
    assert captured["url"] == "https://api.example.com/v1/override"
    assert captured["headers"] == {"X-API-Key": "adapter-key"}
    assert captured["timeout"] == 30.0


def test_parser_raises_parser_error_on_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ParserError):
        parse_csv(str(tmp_path / "does_not_exist.csv"))
