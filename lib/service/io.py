import aiofiles
import asyncio
from dataclasses import dataclass
import os
from pathlib import Path

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
    def create(file_limit: int | None):
        semaphore = asyncio.Semaphore(file_limit) if file_limit else None
        return IoService(NullableSemaphore(semaphore))

    async def f_read(self, file_path: str) -> str:
        async with self._semaphore:
            async with aiofiles.open(file_path, 'r') as f:
                return await f.read()

    async def f_write(self, file_path: str, data: str):
        async with self._semaphore:
            async with aiofiles.open(file_path, 'w') as f:
                await f.write(data)

    async def f_delete(self, file_path: str):
        await asyncio.to_thread(os.remove, file_path)

    async def f_exists(self, file_path: str) -> bool:
        return await asyncio.to_thread(Path(file_path).exists)

@dataclass
class NullableSemaphore:
    semaphore: asyncio.Semaphore | None

    async def __aenter__(self):
        if self.semaphore:
            return await self.semaphore.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.semaphore:
            return await self.semaphore.__aexit__(exc_type, exc_value, traceback)
        return False

