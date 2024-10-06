from dataclasses import dataclass, field
from datetime import datetime

from .service import AbstractClockService

@dataclass
class MockClockService(AbstractClockService):
    dt: datetime
    clock_time: float = field(default=0)

    def time(self) -> float:
        return self.clock_time

    def now(self):
        return self.dt

    async def sleep(self, ms: int):
        return

    def tick_time(self, distance: float = 1):
        self.clock_time += distance
