from aiohttp import ClientSession
import aiofiles
import asyncio
import json
from typing import Any

_RESPONSE_READERS = {
    'json': lambda resp: resp.json(),
    'text': lambda resp: resp.text(),
}

class CachedFileFetcher:
    def __init__(self, session: ClientSession, cacher: Any):
        self._session = session
        self._cacher = cacher

    @staticmethod
    def create(session: ClientSession = None, cacher: Any = None):
        cacher = cacher or FileCacher.create()
        session = ClientSession()
        return CachedFileFetcher(session, save_dir)

    # async def get(self, url, format='json'):

    async def __aenter__(self):
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if await self._session.__aexit__(exc_type, exc_value, traceback):
            return True
        return False
        

    async def _write_GET_to_disc(self, url, reader):
        async with self._session.get(url) as resp:
            if response.status != 200:
                return await reader(resp)
                
            async with aiofiles.open(file_path, 'wb') as f:
                async for chunk in response.content.iter_chunked(1024):
                    await f.write(chunk)

class FileCacher:
    @staticmethod
    def create(config_path: str = './state-out/http-cache.json', 
               save_dir: str = './cache-out'):
        session = ClientSession()
        return CachedFileFetcher(session, save_dir)
    


async def _file_exists_async(file_path):
    from pathlib import Path
    path = Path(file_path)
    return await asyncio.to_thread(path.exists)