# modules\atlas-core\src\atlas_core\contracts\storage.py
from abc import ABC, abstractmethod
from typing import Any


class StorageProvider(ABC):
    @abstractmethod
    def save(self, data: Any, path: str) -> None:
        pass

    @abstractmethod
    def load(self, path: str) -> Any:
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass
