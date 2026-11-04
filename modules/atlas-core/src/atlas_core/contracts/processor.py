from abc import ABC, abstractmethod
from typing import Any


class Processor(ABC):
    @abstractmethod
    def process(self, data: Any) -> Any:
        pass
