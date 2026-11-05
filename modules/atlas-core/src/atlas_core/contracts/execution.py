from abc import ABC, abstractmethod


class ExecutionManager(ABC):
    @abstractmethod
    def start(self, execution_id: str) -> None:
        pass

    @abstractmethod
    def complete(self, execution_id: str) -> None:
        pass

    @abstractmethod
    def fail(self, execution_id: str, reason: str) -> None:
        pass
