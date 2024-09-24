from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from lib.utility.concurrent import merge_async_iters

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



