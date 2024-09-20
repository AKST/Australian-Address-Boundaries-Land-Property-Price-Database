import aiofiles
import asyncio
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger
import json
import os
from typing import Any, Dict, Optional

from lib.service.clock import ClockService
from lib.service.io import IoService
from lib.service.uuid import UuidService
from .constants import STATE_INIT, CACHE_VERSION
from .expiry import CacheExpire
from .headers import InstructionHeaders

class FileCacher:
    _logger = getLogger(f'{__name__}.FileCacher')

    _io: IoService
    _uuid: UuidService
    _clock: ClockService
    _rc_factory: Any

    _state: Optional[Dict[str, str]]
    _save_dir: str
    _config_path: str

    def __init__(self,
                 save_dir: str,
                 config_path: str,
                 rc_factory: Any,
                 io: IoService,
                 uuid: UuidService,
                 clock: ClockService,
                 state: Optional[Dict[str, str]] = None):
        self._save_dir = save_dir
        self._config_path = config_path
        self._state = state
        self._io = io
        self._uuid = uuid
        self._clock = clock
        self._rc_factory = rc_factory

    def read(self, url: str, fmt: str):
        if url not in self._state:
            return None, False

        if fmt not in self._state[url]:
            return None, False

        now = self._clock.now()
        state = self.parse_state(self._state, url)
        cache_found = state and fmt in state
        cache_expired = cache_found and state[fmt].has_expired(now)

        if cache_found:
            return state, not cache_expired

        return None, False

    async def write(self, url: str, meta: InstructionHeaders, data: str):
        fname = f"{meta.request_label}-{self._uuid.get_uuid4_hex()}.{meta.ext}"
        fpath = os.path.join(self._save_dir, fname)
        await self._io.f_write(fpath, data)

        fmts = self._state.get(url, {})

        if meta.format in fmts:
            cache = self._rc_factory.from_json(fmts[meta.format])
            await self._io.f_delete(cache.location)

        request_cache = self._rc_factory.create(meta.expiry, fname, self._clock.now())
        fmts[meta.format] = request_cache.to_json()
        self._state[url] = fmts

        await self._save_cache_state()
        return self.parse_state(self._state, url)

    def parse_state(self, state, key):
        return {
            fmt: self._rc_factory.from_json(s)
            for fmt, s in state[key].items()
        }

    async def __aenter__(self):
        try:
            if not await self._io.f_exists(self._config_path):
                await self._io.f_write(self._config_path, STATE_INIT)
            state = json.loads(await self._io.f_read(self._config_path))
            if state['version'] != CACHE_VERSION:
                raise Exception("cache doesn't match version")
            self._state = state['files']
        except Exception as e:
            self._logger.error("Failed to save cache state, possibly corrupted")
            raise e
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._save_cache_state()
        return False

    async def _save_cache_state(self):
        state = { 'version': CACHE_VERSION, 'files': self._state }
        await self._io.f_write(self._config_path, json.dumps(state, indent=1))

    @staticmethod
    def create(config_path: str | None = None,
               cache_dir: str | None = None):
        cache_dir = cache_dir or './_out_cache'
        clock = ClockService()
        uuid = UuidService()
        io = IoService()
        factory = RequestCacheFactory(cache_dir=cache_dir)
        return FileCacher(cache_dir,
                          config_path or './_out_state/http-cache.json',
                          rc_factory=factory,
                          io=IoService(),
                          uuid=UuidService(),
                          clock=ClockService())

_date_format = '%Y-%m-%d %H:%M:%S'

@dataclass
class RequestCache:
    expire: Any
    file_name: str
    age: datetime
    cache_dir: str

    @property
    def location(self):
        return f'{self.cache_dir}/{self.file_name}'

    def has_expired(self, now: datetime):
        return self.expire.has_expired(self.age, now)

    def to_json(self):
        age = self.age.strftime(_date_format)
        return {
            'expire': str(self.expire),
            'location': self.file_name,
            'age': age,
        }

@dataclass
class RequestCacheFactory:
    cache_dir: str

    def create(self, expire, fname, age):
        return RequestCache(expire, fname, age, self.cache_dir)

    def from_json(self, json):
        return RequestCache(
            CacheExpire.parse_expire(json['expire']),
            json['location'],
            datetime.strptime(json['age'], _date_format),
            cache_dir=self.cache_dir,
        )
