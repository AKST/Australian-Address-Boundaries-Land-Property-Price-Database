from aiohttp import ClientSession as ThirdPartyClientSession
from dataclasses import dataclass, field
from typing import Any, Optional, Dict

from .base import AbstractClientSession, AbstractGetResponse

class ClientSession(AbstractClientSession):
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

    async def __aenter__(self):
        self._response = await self._request_context_manager.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._request_context_manager:
            await self._request_context_manager.__aexit__(exc_type, exc_value, traceback)
        if self._response:
            await self._response.__aexit__(exc_type, exc_value, traceback)
        return False
