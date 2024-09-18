from aiohttp import ClientSession
import aiofiles
import asyncio
from datetime import datetime
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
    NEVER = 'never'

    @staticmethod
    def parse_expire(expire_str: str | None):
        if not expire_str:
            return None
        if expire_str == 'never':
            return expire_str
        raise ValueError('Invalid Expire string')

@dataclass
class InstructionHeaders:
    format: str
    expiry: Optional[str]
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
    expire: str
    location: str
    age: str

    @staticmethod
    def from_json(json):
        age = datetime.strptime(json['age'], _date_format)
        return RequestCache(json['expire'], json['location'], age)
        
    def to_json(self):
        age = self.age.strftime(_date_format)
        return { 'expire': self.expire, 'location': self.location, 'age': age } 

class FileCacher:
    _logger = getLogger(f'{__name__}.FileCacher')
    
    _state: Optional[Dict[str, str]] = None
    _config_path: str
    _save_dir: str
    
    def __init__(self, config_path, save_dir):
        self._config_path = config_path
        self._save_dir = save_dir
        
    @staticmethod
    def create(config_path: str = './state-out/http-cache.json', 
               save_dir: str = './cache-out'):
        return FileCacher(config_path, save_dir)

    async def read(self, url: str, fmt: str):
        # TODO delete anything that's expired
        if url not in self._state:
            return None
            
        if fmt not in self._state[url]:
            return None
        
        return FileCacher.parse_state(self._state, url)
        
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
        if not await _file_exists(self._config_path):
            async with aiofiles.open(self._config_path, 'w') as f:
                await f.write("{}")
            
        async with aiofiles.open(self._config_path, 'r') as f:
            try:
                self._state = json.loads(await f.read())
            except Exception as e:
                self._logger.error("Failed to save cache state, possibly corrupted")
                raise e
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        async with aiofiles.open(self._config_path, 'w') as f:
            await f.write(json.dumps(self._state))
        return False

async def _file_exists(file_path):
    from pathlib import Path
    path = Path(file_path)
    return await asyncio.to_thread(path.exists)