from abc import ABC, abstractmethod


class Connector(ABC):

    @abstractmethod
    def extract(self):
        pass
