"""Integration tests for atlas-storage with FileSystemBackend."""

from __future__ import annotations

from pathlib import Path

import pandas as pd  # type: ignore[import-untyped]
import pytest
from atlas_storage import (
    FileSystemBackend,
    StorageLocation,
    StorageManager,
    StorageReadError,
)


@pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
def test_filesystem_backend_roundtrip_for_structured_formats(
    tmp_path: Path,
    fmt: str,
) -> None:
    manager = StorageManager(backend=FileSystemBackend())
    df = pd.DataFrame(
        [
            {"id": 101, "city": "Toronto", "score": 9.5},
            {"id": 102, "city": "Vancouver", "score": 8.7},
        ]
    )
    target = tmp_path / "bronze" / f"cities.{fmt}"
    location = StorageLocation(layer="bronze", path=str(target), format=fmt)

    result = manager.save(df, location)
    assert result.success is True
    assert result.row_count == 2
    assert manager.exists(location) is True
    assert target.exists()

    loaded = manager.load(location)
    assert isinstance(loaded, pd.DataFrame)
    assert len(loaded) == 2
    assert list(loaded["city"]) == ["Toronto", "Vancouver"]


def test_filesystem_backend_raw_bytes_and_missing_file(tmp_path: Path) -> None:
    manager = StorageManager(backend=FileSystemBackend())
    raw_loc = StorageLocation(
        layer="bronze",
        path=str(tmp_path / "raw" / "payload.bin"),
        format="raw",
    )
    manager.save(b"atlas-raw-bytes", raw_loc)
    assert manager.load(raw_loc) == b"atlas-raw-bytes"

    missing_loc = StorageLocation(
        layer="bronze",
        path=str(tmp_path / "raw" / "missing.csv"),
        format="csv",
    )
    assert manager.exists(missing_loc) is False
    with pytest.raises(StorageReadError, match="does not exist"):
        manager.load(missing_loc)
