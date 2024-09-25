import asyncio

class SizeSemaphore:
    def __init__(self, max_size):
        self.max_size = max_size
        self.current_size = 0
        self._condition = asyncio.Condition()

    async def acquire(self, file_size):
        return await SizeSemaphore._AcquireRelease(self, file_size).__aenter__()

    class _AcquireRelease:
        def __init__(self, semaphore, file_size):
            self.semaphore = semaphore
            self.file_size = file_size

        async def __aenter__(self):
            async with self.semaphore._condition:
                # Wait until enough size is available
                while self.semaphore.current_size + self.file_size > self.semaphore.max_size:
                    await self.semaphore._condition.wait()
                # Reserve the size
                self.semaphore.current_size += self.file_size
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            async with self.semaphore._condition:
                # Release the size
                self.semaphore.current_size -= self.file_size
                self.semaphore._condition.notify_all()
