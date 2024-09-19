import asyncio
from collections import namedtuple
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, List, Dict, Optional

from lib.service.http import ClientSession, AbstractClientSession
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

        return ThrottledGetResponse(
            _config=(url, headers),
            _semaphore=self._semaphores[host],
            _session=self._session,
        )

    @property
    def closed(self):
        return self._session.closed

@dataclass
class ThrottledGetResponse:
    _config: (str, Optional[Dict[str, str]])
    _semaphore: Any
    _session: ClientSession
    _request: Any = field(default=None)

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._request:
            await self._request.__aexit__(exc_type, exc_value, traceback)
        return False

    async def __aenter__(self):
        async with self._semaphore:
            if self._session.closed:
                raise RuntimeError("http session has been closed")
            url, headers = self._config
            self._request = self._session.get(url, headers=headers)
            return await self._request.__aenter__()

