from datetime import datetime, timedelta
import logging
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from lib.service.io import IoService
from lib.service.http import ClientSession, GetResponse
from lib.service.http.cache import *
from lib.service.http.cache.file_cache import FileCacher, RequestCache
from lib.service.http.cache.client_session import CachedGet

_date_str = '2012-10-15 10:10:10'
_date_obj = datetime(2012, 10, 15, 10, 10, 10)

_never_instructions = InstructionHeaders(
    format='json',
    expiry=NeverExpire(),
    disabled=False,
    request_label='context-test',
)

class GetResponseTestCase(IsolatedAsyncioTestCase):
    mock_io = None
    mock_cache = None
    mock_session = None
    mock_response = None
    mock_logger = None

    async def asyncSetUp(self):
        self.mock_io = AsyncMock(spec=IoService)
        self.mock_cache = AsyncMock(spec=FileCacher)
        self.mock_session = AsyncMock(spec=ClientSession)
        self.mock_logger = AsyncMock(spec=logging.Logger)

        self.mock_response = AsyncMock(spec=GetResponse)
        self.mock_response.__aenter__.return_value = self.mock_response

        self.mock_session.get.return_value = self.mock_response

    async def test_async_context_with_no_cache(self):
        meta = _never_instructions
        fmts = {'json': RequestCache(meta.expiry, 'file_location', _date_obj)}

        self.mock_io.f_read.return_value = '{"count":0}'
        self.mock_cache.read.return_value = (None, False)
        self.mock_cache.write.return_value = fmts
        self.mock_response.text.return_value = '{"count":0}'
        self.mock_response.status = 200

        instance = CachedGet(
            _config=('my_url', {}, meta),
            _io=self.mock_io,
            _cache=self.mock_cache,
            _logger=self.mock_logger,
            _session=self.mock_session,
        )

        async with instance as request:
            self.mock_cache.read.assert_called_once_with('my_url', meta.format)
            self.mock_session.get.assert_called_once_with('my_url', headers={})
            self.mock_response.__aenter__.assert_called_once()
            self.mock_response.text.assert_called_once()
            self.mock_cache.write.assert_called_once_with('my_url', meta, '{"count":0}')
            self.assertEqual(request.status, 200)

            self.assertEqual(await request.json(), { 'count': 0 })
            self.mock_io.f_read.assert_called_once_with('file_location')

    async def test_async_context_with_failed_request_with_no_cache(self):
        meta = _never_instructions
        fmts = {'json': RequestCache(meta.expiry, 'file_location', _date_obj)}

        self.mock_io.f_read.return_value = '{"count":0}'
        self.mock_cache.read.return_value = (None, False)
        self.mock_response.status = 400

        instance = CachedGet(
            _config=('my_url', {}, meta),
            _io=self.mock_io,
            _cache=self.mock_cache,
            _logger=self.mock_logger,
            _session=self.mock_session,
        )

        async with instance as request:
            self.mock_cache.read.assert_called_once_with('my_url', meta.format)
            self.mock_session.get.assert_called_once_with('my_url', headers={})
            self.mock_response.__aenter__.assert_called_once()
            self.mock_response.text.assert_not_called()
            self.mock_cache.write.assert_not_called()
            self.assertNotEqual(request, instance)
            self.assertEqual(instance.status, 400)

    async def test_async_context_with_failed_request_with_expired_cache(self):
        meta = _never_instructions
        fmts = {'json': RequestCache(meta.expiry, 'file_location', _date_obj)}

        self.mock_io.f_read.return_value = '{"count":0}'
        self.mock_cache.read.return_value = (fmts, False)
        self.mock_response.status = 400

        instance = CachedGet(
            _config=('my_url', {}, meta),
            _io=self.mock_io,
            _cache=self.mock_cache,
            _logger=self.mock_logger,
            _session=self.mock_session,
        )

        async with instance as request:
            self.mock_cache.read.assert_called_once_with('my_url', meta.format)
            self.mock_session.get.assert_called_once_with('my_url', headers={})
            self.mock_response.__aenter__.assert_called_once()
            self.mock_response.text.assert_not_called()
            self.mock_cache.write.assert_not_called()
            self.assertEqual(request, instance)
            self.assertEqual(request.status, 200)

            self.assertEqual(await request.json(), { 'count': 0 })
            self.mock_io.f_read.assert_called_once_with('file_location')

    async def test_async_context_with_cache(self):
        meta = _never_instructions
        fmts = {'json': RequestCache(meta.expiry, 'file_location', _date_obj)}

        self.mock_io.f_read.return_value = '{"count":0}'
        self.mock_cache.read.return_value = (fmts, True)

        instance = CachedGet(
            _config=('my_url', {}, meta),
            _io=self.mock_io,
            _cache=self.mock_cache,
            _logger=self.mock_logger,
            _session=self.mock_session,
        )

        async with instance as request:
            self.mock_cache.read.assert_called_once_with('my_url', meta.format)
            self.mock_session.get.assert_not_called()
            self.assertEqual(request, instance)
            self.assertEqual(request.status, 200)

            self.assertEqual(await request.json(), { 'count': 0 })
            self.mock_io.f_read.assert_called_once_with('file_location')

