import asyncio
from collections import namedtuple
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, List, Dict, Optional

from lib.service.http import ClientSession
from lib.service.http.client_session import AbstractClientSession, AbstractGetResponse
from .util import url_host

HostSemaphoreConfig = namedtuple('HostSemaphoreConfig', ['host', 'limit'])

class ThrottledSession(AbstractClientSession):
    _logger = getLogger(f'{__name__}.ThrottledSession')
    """
    We want to throttle on a host basis to avoid getting rate
    limited of blocked. So we have a different semaphore for
    the different hosts.
    """
    _semaphores: Dict[str, asyncio.Semaphore]

    def __init__(self, session: ClientSession, semaphores):
        self._semaphores = semaphores
        self._session = session

    @staticmethod
    def create(host_configs: List[HostSemaphoreConfig],
               session: ClientSession = None):
        semaphores = { c.host: asyncio.Semaphore(c.limit) for c in host_configs }
        session = session or ClientSession.create()
        return ThrottledSession(session, semaphores)

    async def __aenter__(self):
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._session.__aexit__(exc_type, exc_value, traceback)
        return False

    def get(self, url: str, headers: Optional[Dict[str, str]]=None):
        host = url_host(url)
        if host not in self._semaphores:
            return self._session.get(url)

        return ThrottledGetResponse(url=url,
                                    headers=headers,
                                    semaphore=self._semaphores[host],
                                    session=self._session)

    @property
    def closed(self):
        return self._session.closed

class ThrottledGetResponse(AbstractGetResponse):
    url: str
    headers: Dict[str, str] | None

    _semaphore: Any
    _session: ClientSession
    _response: AbstractGetResponse = None

    def __init__(self, url, headers, semaphore, session):
        self.url = url
        self.headers = headers
        self._session = session
        self._semaphore = semaphore

    @property
    def status(self):
        return self._response.status

    async def text(self):
        return await self._response.text()

    async def json(self):
        return await self._response.json()

    async def __aenter__(self):
        await self._semaphore.__aenter__()

        if self._session.closed:
            raise RuntimeError("http session has been closed")

        self._response = self._session.get(self.url, headers=self.headers)
        await self._response.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._semaphore.__aexit__(exc_type, exc_value, traceback)

        if self._response:
            await self._response.__aexit__(exc_type, exc_value, traceback)

        return False


