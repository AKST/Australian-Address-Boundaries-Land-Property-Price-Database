from abc import ABC, abstractmethod
from typing import Dict

class AbstractClientSession(ABC):
    @abstractmethod
    def get(self, url: str, headers: Dict[str, str]):
        pass

    @abstractmethod
    async def __aenter__(self):
        pass

    @abstractmethod
    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

    @property
    @abstractmethod
    def closed(self):
        pass

class AbstractGetResponse(ABC):
    @property
    @abstractmethod
    def status(self):
        pass

    @abstractmethod
    async def json(self):
        pass

    @abstractmethod
    async def text(self):
        pass

    @abstractmethod
    async def __aenter__(self):
        pass

    @abstractmethod
    async def __aexit__(self, exc_type, exc_value, traceback):
        pass
