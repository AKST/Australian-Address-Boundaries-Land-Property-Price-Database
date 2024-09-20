import logging
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from lib.service.clock import ClockService
from lib.service.http import ConnectionError
from lib.service.http import AbstractClientSession, AbstractGetResponse
from lib.service.http.exp_backoff import BackoffConfig, RetryPreference
from lib.service.http.exp_backoff.client_session import ExpBackoffGetResponse, ResponseFactory

class GetResponseTestCase(IsolatedAsyncioTestCase):
    mock_clock = None
    mock_session = None
    mock_response = None

    async def asyncSetUp(self):
        self.mock_response = AsyncMock(spec=AbstractGetResponse)
        self.mock_session = AsyncMock(spec=AbstractClientSession)
        self.mock_session.get.return_value = self.mock_response
        self.mock_clock = AsyncMock(spec=ClockService)

    def instance(self, conf, url, headers=None):
        return ExpBackoffGetResponse(url, headers, conf, self.mock_session, self.mock_clock)

    async def test_connection_error(self):
        self.mock_response.status = 200
        self.mock_response.__aenter__.side_effect = [ConnectionError(), self.mock_response]
        config = RetryPreference(allowed=2)
        async with self.instance(config, 'my_url') as response:
            assert self.mock_session.get.call_count == 2
            self.mock_session.get.assert_called_with('my_url', headers=None)

class GetResponseTestCase(IsolatedAsyncioTestCase):
    mock_clock = None
    mock_session = None
    mock_response = None

    async def asyncSetUp(self):
        self.mock_response = AsyncMock(spec=AbstractGetResponse)
        self.mock_session = AsyncMock(spec=AbstractClientSession)
        self.mock_session.get.return_value = self.mock_response
        self.mock_clock = AsyncMock(spec=ClockService)

    def instance(self, conf, url, headers=None):
        return ExpBackoffGetResponse(url, headers, conf, self.mock_session, self.mock_clock)

    async def test_connection_error(self):
        self.mock_response.status = 200
        self.mock_response.__aenter__.side_effect = [ConnectionError(), self.mock_response]
        config = RetryPreference(allowed=2)
        async with self.instance(config, 'my_url') as response:
            assert self.mock_session.get.call_count == 2
            self.mock_session.get.assert_called_with('my_url', headers=None)

class ResponseFactoryTestCase(IsolatedAsyncioTestCase):
    mock_session = None
    mock_clock = None

    async def asyncSetUp(self):
        self.mock_session = AsyncMock(spec=AbstractClientSession)
        self.mock_clock = AsyncMock(spec=ClockService)

    def instance(self, conf):
        return ResponseFactory(config=conf,
                               session=self.mock_session,
                               clock=self.mock_clock)

    async def test_create_no_hosts(self):
        factory = self.instance(BackoffConfig(RetryPreference(allowed=2)))
        response = factory.create('http://www.google.com/asdfasf', None)
        self.assertEqual(response._config, RetryPreference(allowed=2))

    async def test_create_with_hosts(self):
        hosts = { 'www.google.com': RetryPreference(allowed=3) }
        factory = self.instance(BackoffConfig(RetryPreference(allowed=2), hosts))
        response = factory.create('http://www.google.com/asdfasf', None)
        self.assertEqual(response._config, RetryPreference(allowed=3))



