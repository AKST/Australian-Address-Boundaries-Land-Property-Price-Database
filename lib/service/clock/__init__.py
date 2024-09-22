import asyncio
from datetime import datetime

class ClockService:
    """
    This exists for the sole purpose of mocking time. Not
    as a concept but as a dependency in more complex code.
    """
    def now(self):
        return datetime.now()

    async def sleep(self, ms: int):
        await asyncio.sleep(ms)

