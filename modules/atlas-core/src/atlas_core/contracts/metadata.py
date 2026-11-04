from abc import ABC, abstractmethod

from atlas_core.models.dataset import Dataset


class MetadataProvider(ABC):
    @abstractmethod
    def register_dataset(self, dataset: Dataset) -> None:
        pass

    @abstractmethod
    def get_dataset(self, dataset_id: str) -> Dataset | None:
        pass
