from aiohttp import ClientSession as ThirdPartyClientSession
from aiohttp.client_exceptions import ClientConnectorError as ThirdPartyClientConnectorError
from dataclasses import dataclass, field
from typing import Any, Optional, Dict, AsyncIterator, AsyncGenerator

from .base import AbstractClientSession, AbstractGetResponse

class ConnectionError(Exception):
    pass

class ClientSession(AbstractClientSession):
    """
    Why does this exist, well I started with the API of
    `aiohttp` but I realised maybe I want to divate from
    their API and lock down the exact API I use.
    """
    def __init__(self, session: ThirdPartyClientSession):
        self._session = session

    def get(self, url: str, headers: Optional[Dict[str, str]]=None):
        if self._session.closed:
            raise RuntimeError("http session has been closed")

        request_context_manager = self._session.get(url, headers=headers)
        return GetResponse(_request_context_manager=request_context_manager)

    async def __aenter__(self):
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._session.__aexit__(exc_type, exc_value, traceback)
        return False

    @staticmethod
    def create():
        return ClientSession(ThirdPartyClientSession())

    @property
    def closed(self):
        return self._session.closed

@dataclass
class GetResponse(AbstractGetResponse):
    _request_context_manager: Any
    _response: Any = field(default=None)

    @property
    def status(self):
        return self._response.status

    async def json(self):
        return await self._response.json()

    async def text(self):
        return await self._response.text()

    async def stream(self, chunk_size: int) -> AsyncGenerator[str, None]:
        async for chunk in self._response.content.iter_chunked(chunk_size):
            if chunk:
                yield str(chunk)

    async def __aenter__(self):
        try:
            self._response = await self._request_context_manager.__aenter__()
            return self
        except ThirdPartyClientConnectorError as e:
            # Connection manager complains if you call `__aexit__` after
            # a connection error, so lets just drop it and move on.
            self._request_context_manager = None
            raise ConnectionError(e)

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._request_context_manager:
            await self._request_context_manager.__aexit__(exc_type, exc_value, traceback)
        if self._response:
            await self._response.__aexit__(exc_type, exc_value, traceback)
        return False
