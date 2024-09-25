import asyncio
from typing import Self

class SizeSemaphore:
    def __init__(self, max_size):
        self.max_size = max_size
        self.current_size = 0
        self._condition = asyncio.Condition()

    async def acquire(self: Self, file_size):
        return await AcquireRelease(self, file_size).__aenter__()

class AcquireRelease:
    file_size: int
    _semaphore: SizeSemaphore

    def __init__(self: Self, semaphore, file_size):
        self._semaphore = semaphore
        self.file_size = file_size

    async def __aenter__(self: Self):
        async with self._semaphore._condition:
            # Wait until enough size is available
            while self._semaphore.current_size + self.file_size > self._semaphore.max_size:
                await self._semaphore._condition.wait()
            # Reserve the size
            self._semaphore.current_size += self.file_size
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        async with self.semaphore._condition:
            # Release the size
            self.semaphore.current_size -= self.file_size
            self.semaphore._condition.notify_all()

