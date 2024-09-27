import aiofiles
import asyncio
from dataclasses import dataclass
import os
from pathlib import Path
from typing import AsyncIterator, AsyncGenerator, Tuple, List
from zipfile import ZipFile

from lib.utility.concurrent import NullableSemaphore, iterator_thread

WalkItem = Tuple[str, List[str], List[str]]

class IoService:
    """
    This exists for the purpose of making io straight
    forward to mock in tests, but also ensuring I don't
    violate any limitations in terms of open files.
    """
    _semaphore: 'NullableSemaphore'

    def __init__(self, semaphore: 'NullableSemaphore'):
        self._semaphore = semaphore

    @staticmethod
    def create(file_limit: int | None) -> 'IoService':
        semaphore = asyncio.Semaphore(file_limit) if file_limit else None
        return IoService(NullableSemaphore(semaphore, disabled=False))

    async def extract_zip(self, zipfile: str, unzip_to: str) -> None:
        async with self._semaphore:
            await asyncio.to_thread(_sync_unzip, zipfile, unzip_to)

    async def mk_dir(self, dir_name: str):
        await asyncio.to_thread(os.mkdir, dir_name)

    async def grep_dir(self, dir_name: str, pattern: str) -> AsyncGenerator[str, None]:
        directory = Path(dir_name)
        async for item in iterator_thread(directory.rglob, pattern):
            yield str(item)

    async def walk_dir(self, dir_name: str) -> AsyncGenerator[WalkItem, None]:
        async with self._semaphore:
            async for item in iterator_thread(os.walk, dir_name):
                yield item

    async def f_read(self, file_path: str) -> str:
        async with self._semaphore:
            async with aiofiles.open(file_path, 'r') as f:
                data = await f.read()
        return data

    async def f_read_slice(self, file_path: str, offset: int, length: int) -> bytes:
        async with self._semaphore:
            async with aiofiles.open(file_path, 'rb') as f:
                await f.seek(offset)
                data = await f.read(length)
        return data

    async def f_write_chunks(self,
                             file_path: str,
                             chunks: AsyncGenerator[bytes, None]) -> None:
        async with self._semaphore:
            async with aiofiles.open(file_path, 'wb') as f:
                async for chunk in chunks:
                    await f.write(chunk)

    async def f_read_chunks(self,
                            file_path: str,
                            chunk_size=1024) -> AsyncGenerator[bytes, None]:
        async with self._semaphore:
            async with aiofiles.open(file_path, 'rb') as f:
                while True:
                    chunk = await f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

    async def f_write(self, file_path: str, data: str):
        async with self._semaphore:
            async with aiofiles.open(file_path, 'w') as f:
                await f.write(data)

    async def f_delete(self, file_path: str):
        await asyncio.to_thread(os.remove, file_path)

    async def f_exists(self, file_path: str) -> bool:
        return await asyncio.to_thread(Path(file_path).exists)

    async def f_size(self, file_path: str) -> int:
        return await asyncio.to_thread(os.path.getsize, file_path)

    async def is_dir(self, dir_name: str) -> bool:
        return await asyncio.to_thread(os.path.isdir, dir_name)

    async def is_file(self, file_name: str) -> bool:
        return await asyncio.to_thread(os.path.isfile, file_name)

    async def is_directory_empty(self, dir_name: str) -> bool:
        async with self._semaphore:
            is_empty = await asyncio.to_thread(_sync_check_if_dir_empty, dir_name)
        return is_empty


def _sync_unzip(zipfile: str, unzip_to: str):
    with ZipFile(zipfile, 'r') as z:
        z.extractall(unzip_to)

def _sync_check_if_dir_empty(dir_name: str) -> bool:
    with os.scandir(dir_name) as it:
        for entry in it:
            return False
        return True

def _sync_os_walk(root_dir: str, queue: asyncio.Queue[WalkItem | None]) -> None:
    for dirpath, dirnames, filenames in os.walk(root_dir):
        queue.put_nowait((dirpath, dirnames, filenames))
    queue.put_nowait(None)
