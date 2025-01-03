import abc
import asyncio
from datetime import datetime
from time import time

class AbstractClockService(abc.ABC):
    @abc.abstractmethod
    def now(self) -> datetime:
        raise NotImplementedError()

    @abc.abstractmethod
    def time(self) -> float:
        raise NotImplementedError()

    @abc.abstractmethod
    async def sleep(self, ms: int) -> None:
        raise NotImplementedError()

class ClockService(AbstractClockService):
    """
    This exists for the sole purpose of mocking time. Not
    as a concept but as a dependency in more complex code.
    """
    def now(self):
        return datetime.now()

    def time(self):
        return time()

    async def sleep(self, seconds: float | int):
        await asyncio.sleep(seconds)

