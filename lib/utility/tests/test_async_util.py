from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from lib.utility.async_util import pipe, merge_async_iters

class PipeTestCase(IsolatedAsyncioTestCase):
    async def test_simple(self):
        async def consumer(x):
            return x * 2

        async def producer():
            for x in [1, 2, 3]:
                yield x

        result = [y async for y in pipe(producer, consumer)]
        self.assertEqual(result, [2, 4, 6])

class MergeAsyncIter(IsolatedAsyncioTestCase):
    async def test_merge(self):
        async def with_range(a, b):
            for x in range(a, b):
                yield x
        items = [x async for x in merge_async_iters([
            with_range(0, 5),
            with_range(5, 9),
        ])]
        self.assertEqual(sorted(items), list(range(0, 9)))



