from atlas_core.contracts.storage import StorageProvider


class StorageRegistry:
    def __init__(self) -> None:
        self._backends: dict[str, type[StorageProvider]] = {}

    def register(
        self,
        name: str,
        backend_class: type[StorageProvider],
    ) -> None:
        self._backends[name] = backend_class

    def get(
        self,
        name: str,
    ) -> type[StorageProvider]:
        return self._backends[name]

    def exists(
        self,
        name: str,
    ) -> bool:
        return name in self._backends