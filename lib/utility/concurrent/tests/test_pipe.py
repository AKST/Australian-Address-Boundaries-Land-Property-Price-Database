from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from lib.utility.concurrent import pipe

class PipeTestCase(IsolatedAsyncioTestCase):
    async def test_simple(self):
        async def consumer(x):
            return x * 2

        async def producer():
            for x in [1, 2, 3]:
                yield x

        result = [y async for y in pipe(producer, consumer)]
        self.assertEqual(result, [2, 4, 6])

