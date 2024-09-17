from aiohttp import ClientSession
import asyncio
from collections import namedtuple
from logging import getLogger
from typing import Any, List, Dict

HostSemaphoreConfig = namedtuple('HostSemaphoreConfig', ['host', 'limit'])

class ThrottledSession:
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
        session = session or ClientSession()
        return ThrottledSession(session, semaphores)

    async def __aenter__(self):
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if await self._session.__aexit__(exc_type, exc_value, traceback):
            return True
        return False
    
    async def get_json(self, url: str, params: Any):
        from urllib.parse import urlparse, urlencode

        url_with_params = f"{url}?{urlencode(params)}"
        host = urlparse(url).netloc
        
        async with self._semaphores[host]:
            if self.closed:
                raise GisAbortError("http session has been closed")
        
            async with self._session.get(url_with_params) as response:
                if response.status != 200:
                    self._logger.error(f"Crashed at {url_with_params}")
                    self._logger.error(response)
                    raise GisNetworkError(response.status, response)
                r = await response.json()
                return r

    @property
    def closed(self):
        return self._session.closed