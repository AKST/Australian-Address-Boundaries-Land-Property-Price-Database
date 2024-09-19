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
from .expiry import CacheExpire
from .headers import InstructionHeaders

_date_format = '%Y-%m-%d %H:%M:%S'

@dataclass
class RequestCache:
    expire: Any
    location: str
    age: datetime

    def has_expired(self, now: datetime):
        return self.expire.has_expired(self.age, now)

    @staticmethod
    def from_json(json):
        return RequestCache(
            CacheExpire.parse_expire(json['expire']),
            json['location'],
            datetime.strptime(json['age'], _date_format),
        )

    def to_json(self):
        age = self.age.strftime(_date_format)
        return {
            'expire': str(self.expire),
            'location': self.location,
            'age': age,
        }

class FileCacher:
    _logger = getLogger(f'{__name__}.FileCacher')

    _io: IoService
    _uuid: UuidService
    _clock: ClockService

    _state: Optional[Dict[str, str]]
    _save_dir: str
    _config_path: str

    def __init__(self,
                 save_dir: str,
                 config_path: str,
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

    def read(self, url: str, fmt: str):
        if url not in self._state:
            return None

        if fmt not in self._state[url]:
            return None

        now = self._clock.now()
        state = FileCacher.parse_state(self._state, url)
        cache_found = state and fmt in state
        cache_expired = cache_found and state[fmt].has_expired(now)

        if cache_found and not cache_expired:
            return state
        elif cache_found and cache_expired:
            return None

        return None

    async def write(self, url: str, meta: InstructionHeaders, data: str):
        fname = f"{meta.request_label}-{self._uuid.get_uuid4_hex()}.{meta.ext}"
        fpath = os.path.join(self._save_dir, fname)
        await self._io.f_write(fpath, data)

        fmts = self._state.get(url, {})

        if meta.format in fmts:
            await self._io.f_delete(fmts[meta.format]['location'])

        fmts[meta.format] = RequestCache(meta.expiry, fpath, self._clock.now()).to_json()
        self._state[url] = fmts
        return FileCacher.parse_state(self._state, url)

    @staticmethod
    def parse_state(state, key):
        return {
            fmt: RequestCache.from_json(s)
            for fmt, s in state[key].items()
        }

    async def __aenter__(self):
        try:
            self._state = json.loads(await self._io.f_read(self._config_path))
        except Exception as e:
            self._logger.error("Failed to save cache state, possibly corrupted")
            raise e
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._io.f_write(self._config_path, json.dumps(self._state))
        return False

    @staticmethod
    def create(config_path: str | None = None,
               save_dir: str | None = None):
        clock = ClockService()
        uuid = UuidService()
        io = IoService()
        return FileCacher(save_dir or './_out_cache',
                          config_path or './_out_state/http-cache.json',
                          io=IoService(),
                          uuid=UuidService(),
                          clock=ClockService())
