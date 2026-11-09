from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class Connector(ABC):
    """Base contract for data source connectors."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        pass

    @abstractmethod
    def validate_connection(self) -> bool:
        pass

    @abstractmethod
    def extract(
        self,
        *,
        destination: Path | None = None,
        **kwargs: Any,
    ) -> Any:
        pass
