from aiohttp import ClientSession
import aiofiles
import asyncio
import calendar
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import json
from logging import getLogger
import os
from typing import Any, Dict, Optional
import uuid

from lib.http.util import url_with_params, url_host

class CacheHeader:
    FORMAT = 'X-Cache-Format'
    EXPIRE = 'X-Cache-Expire'
    DISABLED = 'X-Cache-Disabled'
    NAME = 'X-Cache-Name'

class CachedFileFetcher:
    _logger = getLogger(f'{__name__}.CachedFileFetcher')

    def __init__(self, session: ClientSession, cacher, create_get_request):
        self._session = session
        self._cache = cacher
        self._create_get_request = create_get_request

    @staticmethod
    def create(session: ClientSession, cacher: Any = None):
        cache = cacher or FileCacher.create()
        create_get_request = lambda url, headers, meta: CachedGet(
            _config=(url, headers, meta),
            _session=session,
            _cache=cache,
        )
        return CachedFileFetcher(session, cache, create_get_request)

    def get(self, url, headers=None):
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
        s_ext = await self._session.__aexit__(exc_type, exc_value, traceback)
        c_ext = await self._cache.__aexit__(exc_type, exc_value, traceback)
        return s_ext or c_ext

@dataclass
class CachedGet:
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
    _request_context_manager: Any = field(default=None)

    @property
    def status(self):
        if not self._status:
            raise ValueError('status not yet read')
        return self._status

    async def __aenter__(self):
        url, headers, meta = self._config

        state = await self._cache.read(url, meta.format)
        if state is None:
            self._request_context_manager = self._session.get(url, headers=headers)
            response = await self._request_context_manager.__aenter__()
            self._status = response.status
            if self._status == 200:
                data = await response.text()
                state = await self._cache.write(url, meta, data)
        else:
            self._status = 200

        self._state = state
        return self


    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._request_context_manager:
            self._request_context_manager.__aexit__(exc_type, exc_value, traceback)
        return self

    async def json(self):
        if 'json' not in self._state:
            raise ValueError('Incorrect cache hint')

        cache = self._state['json']
        async with aiofiles.open(cache.location, 'r') as f:
            t = await f.read()
            return json.loads(t)

    async def text(self):
        if 'text' not in self._state:
            raise ValueError('Incorrect cache hint')
        cache = self._state['text']

        async with aiofiles.open(self._state.location, 'r') as f:
            return await f.read()

class CacheExpire:
    @dataclass
    class Never:
        def __str__(self):
            return 'never'

        def has_expired(self, saved, now):
            return False

    @dataclass
    class Delta:
        unit: str
        amount: int

        @classmethod
        def parse(cls, s):
            slice_idx = s.find(':')
            return cls(s[:slice_idx], int(s[slice_idx+1:]))

        def has_expired(self, saved, now):
            delta = timedelta(**{ self.unit: self.amount })
            return saved + delta < now

        def __str__(self):
            return f'delta:{self.unit}:{self.amount}'

    @dataclass
    class TillNextDayOfWeek:
        day_of_week: int

        @classmethod
        def parse(cls, s):
            dow = list(calendar.day_name).index(s)
            if 0 > dow < 6:
                raise ValueError(f'invalid day of the week, {s}')
            return cls(dow)

        def __str__(self):
            dow = calendar.day_name[self.day_of_week]
            return f'till_next_day_of_week:{dow}'

        def has_expired(self, saved, now):
            saved_weekday = saved.weekday()

            days_ahead = (self.day_of_week - saved_weekday) % 7
            days_ahead = 7 if days_ahead == 0 else days_ahead
            return saved + timedelta(days=days_ahead) < now

    @classmethod
    def parse_expire(cls, expire_str: str | None):
        if not expire_str:
            return None
        if expire_str == 'never':
            return cls.Never()

        slice_idx = expire_str.find(':')
        if slice_idx < 0:
            raise ValueError('Invalid Expire string')

        kind = expire_str[:slice_idx]
        if kind == 'delta':
            return cls.Delta.parse(expire_str[slice_idx+1:])
        elif kind == 'till_next_day_of_week':
            return cls.TillNextDayOfWeek.parse(expire_str[slice_idx+1:])
        else:
            raise ValueError(f'Invalid Expire string, {expire_str}')

@dataclass
class InstructionHeaders:
    format: str
    expiry: Any | None
    disabled: bool
    filename: Optional[str]

    @property
    def ext(self):
        if self.format == 'json':
            return 'json'
        if self.format == 'text':
            return 'txt'
        raise ValueError(f'unknown format {self.format}')

    @staticmethod
    def from_headers(headers, host):
        headers = headers or {}
        cache_fmt = headers.get(CacheHeader.FORMAT, 'text')
        cache_ttl = headers.get(CacheHeader.EXPIRE, None)
        cache_disabled = headers.get(CacheHeader.DISABLED, 'False') == True
        cache_name = headers.get(CacheHeader.NAME, None)
        cache_name = f'{host}-{cache_name if cache_name else "?"}'

        if CacheHeader.FORMAT in headers:
            del headers[CacheHeader.FORMAT]
        if CacheHeader.EXPIRE in headers:
            del headers[CacheHeader.EXPIRE]
        if CacheHeader.DISABLED in headers:
            del headers[CacheHeader.DISABLED]
        if CacheHeader.NAME in headers:
            del headers[CacheHeader.NAME]

        return headers, InstructionHeaders(
            format=cache_fmt,
            expiry=CacheExpire.parse_expire(cache_ttl),
            disabled=cache_disabled,
            filename=cache_name,
        )

_date_format = '%Y-%m-%d %H:%M:%S'

@dataclass
class RequestCache:
    expire: Any
    location: str
    age: datetime

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

        state = FileCacher.parse_state(self._state, url)
        now = self._get_now()
        if state and not state.has_expired(now):
            return state
        elif state:
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
