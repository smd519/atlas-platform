"""Integration tests for atlas-connectors file extraction, streaming, and parsing."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import pandas as pd  # type: ignore[import-untyped]
import pytest
from atlas_connectors import (
    ConnectorFactory,
    ConnectorRegistry,
    LocalFileConnector,
    RestConnectorAdapter,
    parse_csv,
    parse_json,
    parse_parquet,
)


def test_local_file_connector_and_parsers_roundtrip(tmp_path: Path) -> None:
    df = pd.DataFrame([{"station": "S1", "temp": 12.5}, {"station": "S2", "temp": 19.0}])
    csv_path = tmp_path / "stations.csv"
    json_path = tmp_path / "stations.json"
    parquet_path = tmp_path / "stations.parquet"

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records")
    df.to_parquet(parquet_path, index=False)

    registry = ConnectorRegistry()
    registry.register("local_file", LocalFileConnector)
    factory = ConnectorFactory(registry)

    for path, parser in (
        (csv_path, parse_csv),
        (json_path, parse_json),
        (parquet_path, parse_parquet),
    ):
        connector = factory.create("local_file", path=str(path))
        assert connector.validate_connection() is True
        extracted_path = connector.extract()
        parsed_df = parser(str(extracted_path))
        assert len(parsed_df) == 2
        assert list(parsed_df["station"]) == ["S1", "S2"]


def test_rest_connector_streaming_download_and_csv_parse_roundtrip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _StreamResp:
        def raise_for_status(self) -> None:
            return None

        def iter_bytes(self) -> Iterator[bytes]:
            yield b"station,temp\n"
            yield b"S10,21.5\n"
            yield b"S20,18.0\n"

    @contextmanager
    def fake_stream(*args: Any, **kwargs: Any) -> Iterator[_StreamResp]:
        yield _StreamResp()

    monkeypatch.setattr(httpx, "stream", fake_stream)

    staging_dir = tmp_path / "drop" / ".staging" / "exec-100"
    staging_dir.mkdir(parents=True, exist_ok=True)
    destination = staging_dir / "remote_stations.csv"

    registry = ConnectorRegistry()
    registry.register("rest", RestConnectorAdapter)
    factory = ConnectorFactory(registry)
    connector = factory.create(
        "rest",
        base_url="https://api.example.com",
        endpoint="/datasets/stations.csv",
    )

    downloaded_path = connector.extract(destination=destination)
    assert downloaded_path == destination
    assert destination.exists() is True
    assert not destination.with_suffix(".csv.part").exists()

    parsed_df = parse_csv(str(downloaded_path))
    assert len(parsed_df) == 2
    assert list(parsed_df["station"]) == ["S10", "S20"]
