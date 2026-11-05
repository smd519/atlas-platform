"""Integration tests for atlas-connectors file extraction and parsing."""

from __future__ import annotations

from pathlib import Path

import pandas as pd  # type: ignore[import-untyped]
from atlas_connectors import (
    ConnectorFactory,
    ConnectorRegistry,
    LocalFileConnector,
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
