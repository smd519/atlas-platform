import inspect
from abc import ABC, abstractmethod
from typing import Any

from atlas_core.models.dataset import Dataset


class MetadataProvider(ABC):
    @abstractmethod
    def register_dataset(self, dataset: Any, *, overwrite: bool = False) -> Any:
        pass

    @abstractmethod
    def get_dataset(self, dataset_id: str) -> Dataset | None:
        pass

    def register_dataset_metadata(
        self,
        dataset: Any,
        *,
        overwrite: bool = False,
    ) -> Any:
        """Register a rich dataset metadata record in the catalog."""
        try:
            sig = inspect.signature(self.register_dataset)
            accepts_overwrite = any(
                p.name == "overwrite" or p.kind == inspect.Parameter.VAR_KEYWORD
                for p in sig.parameters.values()
            )
        except (TypeError, ValueError):
            accepts_overwrite = True

        if accepts_overwrite:
            return self.register_dataset(dataset, overwrite=overwrite)
        return self.register_dataset(dataset)

    def register_artifact(
        self,
        artifact: Any,
        *,
        overwrite: bool = False,
    ) -> Any:
        """Register a physical artifact metadata record in the catalog."""
        return None
