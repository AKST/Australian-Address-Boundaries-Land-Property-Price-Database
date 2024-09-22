import asyncio
from dataclasses import dataclass, field
import json
from logging import getLogger, Logger
from typing import Any, Dict, Optional, Tuple

from lib.service.io import IoService
from lib.service.http import ConnectionError
from lib.service.http import AbstractClientSession, AbstractGetResponse
from lib.service.http import ClientSession
from lib.service.http.util import url_with_params, url_host
from .expiry import CacheExpire
from .file_cache import FileCacher, RequestCache
from .headers import InstructionHeaders, CacheHeader

class CachedClientSession(AbstractClientSession):
    _logger = getLogger(f'{__name__}.CachedClientSession')
    _session: AbstractClientSession

    def __init__(self, session, cacher, create_get_request):
        self._session = session
        self._cache = cacher
        self._create_get_request = create_get_request

    @staticmethod
    def create(session: AbstractClientSession | None = None,
               file_cache: FileCacher | None = None,
               io_service: IoService | None = None):
        cache = file_cache or FileCacher.create(None)
        session = session or ClientSession.create()
        logger = getLogger(f'{__name__}.CachedGet')

        create_get_request = lambda url, headers, meta: CachedGet(
            _io=io_service or IoService.create(None),
            _config=(url, headers, meta),
            _logger=logger,
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
    _io: IoService
    _config: Tuple[str, Dict[str, str], InstructionHeaders]
    _session: AbstractClientSession
    _cache: FileCacher
    _logger: Logger
    _status: int | None = field(default=None)
    _state: Any = field(default=None)
    _response: Any = field(default=None)

    @property
    def status(self):
        return self._status

    async def __aenter__(self):
        url, headers, meta = self._config

        state, valid = self._cache.read(url, meta.format)
        if state is None or not valid:
            self._response = self._session.get(url, headers=headers)
            try:
                response = await self._response.__aenter__()
                self._status = response.status
                if self._status == 200:
                    data = await response.text()
                    state = await self._cache.write(url, meta, data)
                elif state is not None:
                    self._logger.warning(
                        'request failed, calling back to cache, '
                        f'status: {self._status}'
                    )
                    self._status = 200
                else:
                    return response

            except ConnectionError as e:
                if state:
                    self._status = 200
                    self._logger.warning('connection error falling back to cache')
                else:
                    self._logger.warning('connection error without cache')
                    raise e
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

        return json.loads(await self._io.f_read(self._state['json'].location))

    async def text(self):
        if 'text' not in self._state:
            raise ValueError('Incorrect cache hint')

        return await self._io.f_read(self._state['text'].location)

