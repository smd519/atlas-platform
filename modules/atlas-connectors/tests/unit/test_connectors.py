"""Unit tests for atlas-connectors contracts, registry, factory, and REST adapter."""

from __future__ import annotations

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


def test_local_file_connector_validation_and_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ConnectorConfigurationError):
        LocalFileConnector("")

    missing = LocalFileConnector(tmp_path / "missing.csv")
    assert missing.validate_connection() is False
    with pytest.raises(ConnectorExtractionError):
        missing.extract()


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


def test_parser_raises_parser_error_on_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ParserError):
        parse_csv(str(tmp_path / "does_not_exist.csv"))
