from abc import ABC, abstractmethod
from typing import Dict, AsyncIterator, AsyncGenerator

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
    def stream(self, chunk_size: int) -> AsyncGenerator[str, None]:
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
