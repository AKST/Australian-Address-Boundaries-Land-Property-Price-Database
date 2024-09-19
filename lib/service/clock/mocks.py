from dataclasses import dataclass
from datetime import datetime

@dataclass
class MockClockService:
    dt: datetime

    def now(self):
        return self.dt
