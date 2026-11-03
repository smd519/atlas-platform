from abc import ABC, abstractmethod


class QualityRule(ABC):

    @abstractmethod
    def validate(self, data):
        pass
