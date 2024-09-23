import aiofiles
import asyncio
from dataclasses import dataclass
import os
from pathlib import Path

from lib.utility.async_util import NullableSemaphore

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

    async def f_read(self, file_path: str) -> str:
        data = ''
        async with self._semaphore:
            async with aiofiles.open(file_path, 'r') as f:
                data = await f.read()
        return data

    async def f_write(self, file_path: str, data: str):
        async with self._semaphore:
            async with aiofiles.open(file_path, 'w') as f:
                await f.write(data)

    async def f_delete(self, file_path: str):
        await asyncio.to_thread(os.remove, file_path)

    async def f_exists(self, file_path: str) -> bool:
        return await asyncio.to_thread(Path(file_path).exists)


