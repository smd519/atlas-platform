from abc import ABC, abstractmethod
from typing import Any


class QualityRule(ABC):
    @property
    @abstractmethod
    def rule_name(self) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass
