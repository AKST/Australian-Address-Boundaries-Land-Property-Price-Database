import aiofiles
import asyncio
import os
from pathlib import Path

class IoService:
    """
    This exists for the purpose of making io straight
    forward to mock in tests.
    """
    async def f_read(self, file_path: str) -> str:
        async with aiofiles.open(file_path, 'r') as f:
            return await f.read()

    async def f_write(self, file_path: str, data: str):
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(data)

    async def f_delete(self, file_path: str):
        await asyncio.to_thread(os.remove, file_path)

    async def f_exists(self, file_path: str) -> bool:
        return await asyncio.to_thread(Path(file_path).exists)

