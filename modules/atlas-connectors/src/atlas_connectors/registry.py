from atlas_core.contracts.connector import Connector


class ConnectorRegistry:
    def __init__(self) -> None:
        self._connectors: dict[str, type[Connector]] = {}

    def register(
        self,
        name: str,
        connector_class: type[Connector],
    ) -> None:
        self._connectors[name] = connector_class

    def get(
        self,
        name: str,
    ) -> type[Connector]:
        return self._connectors[name]

    def exists(
        self,
        name: str,
    ) -> bool:
        return name in self._connectors