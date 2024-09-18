import aiofiles
import asyncio
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger
import json
import os
from typing import Any, Dict, Optional
import uuid

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

    _state: Optional[Dict[str, str]] = None
    _save_dir: str

    def __init__(self,
                 save_dir,
                 read_state,
                 write_state,
                 get_now):
        self._save_dir = save_dir
        self._read_state = read_state
        self._write_state = write_state
        self._get_now = get_now

    async def read(self, url: str, fmt: str):
        # TODO delete anything that's expired
        if url not in self._state:
            return None

        if fmt not in self._state[url]:
            return None

        now = self._get_now()
        state = FileCacher.parse_state(self._state, url)
        cache_found = state and fmt in state
        cache_expired = cache_found and state[fmt].has_expired(now)
        
        if cache_found and not cache_expired:
            return state
        elif cache_found and cache_expired: 
            # TODO delete state saved to disc
            return None

        return None

    async def write(self, url: str, meta: InstructionHeaders, data: str):
        fname = f"{meta.filename}-{uuid.uuid4().hex}.{meta.ext}"
        fpath = os.path.join(self._save_dir, fname)

        async with aiofiles.open(fpath, 'w') as f:
            await f.write(data)

        fmts = self._state.get(url, {})
        fmts[meta.format] = RequestCache(meta.expiry, fpath, datetime.now()).to_json()
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
            self._state = await self._read_state()
        except Exception as e:
            self._logger.error("Failed to save cache state, possibly corrupted")
            raise e
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._write_state(self._state)
        return False

    @staticmethod
    def create(config_path: str = './state-out/http-cache.json',
               save_dir: str = './cache-out'):
        async def read_state():
            if not await _file_exists(config_path):
                async with aiofiles.open(config_path, 'w') as f:
                    await f.write("{}")

            async with aiofiles.open(config_path, 'r') as f:
                return json.loads(await f.read())

        async def write_state(state):
            async with aiofiles.open(config_path, 'w') as f:
                await f.write(json.dumps(state))

        get_now = lambda: datetime.now()

        return FileCacher(save_dir, read_state, write_state, get_now)

async def _file_exists(file_path):
    from pathlib import Path
    path = Path(file_path)
    return await asyncio.to_thread(path.exists)
