"""Local filesystem connector implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from atlas_core import Connector

from atlas_connectors.exceptions import (
    ConnectorConfigurationError,
    ConnectorExtractionError,
)


class LocalFileConnector(Connector):
    """Connector that reads files from the local filesystem."""

    def __init__(self, path: str | Path) -> None:
        raw_path = str(path).strip() if path is not None else ""
        if not raw_path:
            raise ConnectorConfigurationError("LocalFileConnector requires a non-empty 'path'.")
        self._path = Path(raw_path)

    @property
    def source_name(self) -> str:
        """Return the string representation of the configured file path."""
        return str(self._path)

    def validate_connection(self) -> bool:
        """Return True if the configured path exists and is a file."""
        return self._path.exists() and self._path.is_file()

    def extract(self, **kwargs: Any) -> Path:
        """Validate that the local file exists and return its :class:`Path`."""
        if not self.validate_connection():
            raise ConnectorExtractionError(f"Local source file does not exist: '{self._path}'.")
        return self._path


__all__ = ["LocalFileConnector"]
