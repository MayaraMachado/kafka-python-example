from abc import ABC, abstractmethod
from typing import Any, Dict


class MyConnectorAbstract(ABC):
    @abstractmethod
    def run(self, data: Dict[str, Any]):
        raise NotImplementedError("Needs to be implemented!")


class StdoutConnector(MyConnectorAbstract):
    """Prints out the param data."""

    def run(self, data: Dict[str, Any]):
        print(data)
