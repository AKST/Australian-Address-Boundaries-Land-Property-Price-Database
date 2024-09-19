from aiohttp import ClientSession
from unittest import IsolatedAsyncioTestCase, AsyncMock

from lib.service.http.cache import *
from lib.service.http.cache.file_cache import FileCacher
from lib.service.http.cache.client_session import CachedGet

class GetResponseTestCase(IsolatedAsyncioTestCase):
    mock_cache = None
    mock_session = None

    async def asyncSetUp(self):
        self.mock_cache = AsyncMock(spec=FileCacher)
        self.mock_session = AsyncMock(spec=ClientSession)

    async def test_async_context(self):
        instance = CachedGet(
            _config=('...', {}, InstructionHeaders(
                format='json',
                expiry=NeverExpire(),
                disabled=False,
                label='context-test',
            ),
            _cache=self.mock_cache,
            _session=self.mock_session,
        )
