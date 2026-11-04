from atlas_core.contracts.connector import Connector

from atlas_connectors.registry import ConnectorRegistry


class ConnectorFactory:
    def __init__(self, registry: ConnectorRegistry):
        self._registry = registry

    def create(
        self,
        connector_type: str,
        *args,
        **kwargs,
    ) -> Connector:
        connector_class = self._registry.get(connector_type)

        return connector_class(*args, **kwargs)