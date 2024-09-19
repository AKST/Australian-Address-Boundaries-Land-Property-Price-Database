import aiofiles
from aiohttp import ClientSession
import asyncio
from dataclasses import dataclass, field
import json
from logging import getLogger
from typing import Any, Dict, Optional

from lib.service.http import AbstractClientSession, AbstractGetResponse
from lib.service.http.util import url_with_params, url_host
from .expiry import CacheExpire
from .file_cache import FileCacher
from .headers import InstructionHeaders, CacheHeader

class CachedClientSession(AbstractClientSession):
    _logger = getLogger(f'{__name__}.CachedClientSession')

    def __init__(self, session: ClientSession, cacher, create_get_request):
        self._session = session
        self._cache = cacher
        self._create_get_request = create_get_request

    @staticmethod
    def create(session: ClientSession = None, cacher: Any = None):
        session = session or ClientSession.create()
        cache = cacher or FileCacher.create()
        create_get_request = lambda url, headers, meta: CachedGet(
            _config=(url, headers, meta),
            _session=session,
            _cache=cache,
        )
        return CachedClientSession(session, cache, create_get_request)

    def get(self, url, headers=None):
        if not isinstance(url, str):
            raise TypeError(f'URL should be string, got {url}')
        host = url_host(url)
        headers, meta = InstructionHeaders.from_headers(headers, host)
        if meta.disabled:
            return self._session.get(url, headers=headers)
        return self._create_get_request(url, headers, meta)

    async def __aenter__(self):
        await self._session.__aenter__()
        await self._cache.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._session.__aexit__(exc_type, exc_value, traceback)
        await self._cache.__aexit__(exc_type, exc_value, traceback)
        return False

    @property
    def closed(self):
        return self._session.closed

@dataclass
class CachedGet(AbstractGetResponse):
    """
    This only exists to mimic the interface of `ClientSessions`
    so it can be removed if no necessary. If you have to ask
    why is this so confusing, start there and work backwards.
    """
    _config: (str, Dict[str, str], Any)
    _session: ClientSession
    _cache: Any
    _status: int | None = field(default=None)
    _state: Any = field(default=None)
    _response: Any = field(default=None)

    @property
    def status(self):
        if not self._status:
            raise ValueError('status not yet read')
        return self._status

    async def __aenter__(self):
        url, headers, meta = self._config

        state = self._cache.read(url, meta.format)
        if state is None:
            self._response = self._session.get(url, headers=headers)
            await self._response.__aenter__()
            self._status = self._response.status
            if self._status == 200:
                data = await self._response.text()
                state = await self._cache.write(url, meta, data)
            else:
                return self._response
        else:
            self._status = 200

        self._state = state
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._response:
            await self._response.__aexit__(exc_type, exc_value, traceback)
        return False

    async def json(self):
        if 'json' not in self._state:
            raise ValueError('Incorrect cache hint')

        async with aiofiles.open(self._state['json'].location, 'r') as f:
            t = await f.read()
            return json.loads(t)

    async def text(self):
        if 'text' not in self._state:
            raise ValueError('Incorrect cache hint')

        async with aiofiles.open(self._state['text'].location, 'r') as f:
            return await f.read()

