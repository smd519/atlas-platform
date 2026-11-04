from pathlib import Path
from typing import Any

from atlas_core.contracts.connector import Connector


class LocalFileConnector(Connector):
    def __init__(self, path: str):
        self._path = Path(path)

    @property
    def source_name(self) -> str:
        return str(self._path)

    def validate_connection(self) -> bool:
        return self._path.exists()

    def extract(self, **kwargs: Any) -> Path:
        return self._path