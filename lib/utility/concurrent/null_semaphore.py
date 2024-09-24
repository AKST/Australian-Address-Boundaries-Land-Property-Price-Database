import asyncio

class NullableSemaphore:
    _semaphore: asyncio.Semaphore | None
    _disabled: bool

    def __init__(self,
                 semaphore: asyncio.Semaphore | None,
                 disabled: bool = False) -> None:
        self._semaphore = semaphore
        self._disabled = disabled

    @property
    def disabled(self) -> bool:
        return self._disabled

    async def disable(self) -> None:
        if self._semaphore:
            async with self._semaphore:
                self._disabled = True

    async def enable(self) -> None:
        if self._semaphore:
            async with self._semaphore:
                self._disabled = False

    async def __aenter__(self) -> 'NullableSemaphore':
        if self._semaphore and not self._disabled:
            await self._semaphore.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> bool:
        if self._semaphore and not self._disabled:
            await self._semaphore.__aexit__(exc_type, exc_value, traceback)
        return False
