import asyncio
import logging
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock
from typing import Any

from lib.service.clock import ClockService
from lib.service.http import ConnectionError
from lib.service.http import AbstractClientSession, AbstractGetResponse
from lib.service.http.middleware.exp_backoff import *
from lib.service.http.middleware.exp_backoff.host_state import *
from lib.service.http.middleware.exp_backoff.client_session import ExpBackoffGetResponse, ResponseFactory

my_url = 'http://server.com/api'

async def flush_event_loop():
    await asyncio.sleep(0.05)


class GetResponseTestCase(IsolatedAsyncioTestCase):
    mock_state: AsyncMock
    mock_clock: AsyncMock
    mock_session: AsyncMock
    mock_response: AsyncMock

    async def asyncSetUp(self):
        self.mock_response = AsyncMock(spec=AbstractGetResponse)
        self.mock_session = AsyncMock(spec=AbstractClientSession)
        self.mock_session.get.return_value = self.mock_response
        self.mock_state = AsyncMock(spec=HostState)
        self.mock_clock = AsyncMock(spec=ClockService)

    def instance(self,
                 conf,
                 url,
                 state=None,
                 headers=None) -> ExpBackoffGetResponse:
        return ExpBackoffGetResponse(url,
                                     headers,
                                     conf,
                                     state or self.mock_state,
                                     self.mock_session,
                                     self.mock_clock)

    async def test_connection_error(self) -> None:
        self.mock_response.status = 200
        self.mock_response.__aenter__.side_effect = [ConnectionError(), self.mock_response]
        config = RetryPreference(allowed=2)
        async with self.instance(config, my_url) as response:
            assert self.mock_session.get.call_count == 2
            self.mock_session.get.assert_called_with(my_url, headers=None)

    async def test_semaphore_on_error(self) -> None:
        a_resp = AsyncMock(spec=AbstractGetResponse)
        a_resp.__aenter__.side_effect = [ConnectionError()]

        b_event = asyncio.Event()
        b_resp = AsyncMock(spec=AbstractGetResponse)
        b_resp.status = 200
        async def b_resp_enter() -> AbstractGetResponse:
            await b_event.wait()
            return b_resp
        b_resp.__aenter__.side_effect = b_resp_enter

        c_resp = AsyncMock(spec=AbstractGetResponse)
        c_resp.status = 200
        c_resp.__aenter__.return_value = c_resp

        self.mock_session.get.side_effect = [a_resp, b_resp, c_resp]

        config = RetryPreference(allowed=5, pause_other_requests_while_retrying=True)
        state = HostState(config)
        instance_1 = self.instance(config, my_url, state)
        instance_2 = self.instance(config, my_url, state)

        task_1 = asyncio.create_task(instance_1.__aenter__())
        await flush_event_loop()
        a_resp.__aenter__.assert_called_once()
        b_resp.__aenter__.assert_called_once()
        self.assertTrue(state._currently_failing != None)
        task_2 = asyncio.create_task(instance_2.__aenter__())
        await flush_event_loop()
        c_resp.__aenter__.assert_not_called()

        self.assertNotEqual(instance_1.status, b_resp.status)
        self.assertNotEqual(instance_2.status, c_resp.status)

        b_event.set()
        await flush_event_loop()

        self.assertTrue(state._currently_failing == None)
        self.assertEqual(instance_1.status, 200)
        self.assertEqual(instance_2.status, 200)

    async def test_no_semaphore_on_error(self) -> None:
        a_resp = AsyncMock(spec=AbstractGetResponse)
        a_resp.__aenter__.side_effect = [ConnectionError()]

        b_event = asyncio.Event()
        b_resp = AsyncMock(spec=AbstractGetResponse)
        b_resp.status = 200
        async def b_resp_enter() -> AbstractGetResponse:
            await b_event.wait()
            return b_resp
        b_resp.__aenter__.side_effect = b_resp_enter

        c_resp = AsyncMock(spec=AbstractGetResponse)
        c_resp.status = 200
        c_resp.__aenter__.return_value = c_resp

        self.mock_session.get.side_effect = [a_resp, b_resp, c_resp]

        config = RetryPreference(allowed=5,
                                 pause_other_requests_while_retrying=False)
        state = HostState(config)
        instance_1 = self.instance(config, my_url, state)
        instance_2 = self.instance(config, my_url, state)

        task_1 = asyncio.create_task(instance_1.__aenter__())
        await flush_event_loop()
        a_resp.__aenter__.assert_called_once()
        b_resp.__aenter__.assert_called_once()
        self.assertTrue(state._currently_failing == None)
        task_2 = asyncio.create_task(instance_2.__aenter__())
        await flush_event_loop()

        c_resp.__aenter__.assert_called_once()
        self.assertNotEqual(instance_1.status, 200)
        self.assertEqual(instance_2.status, 200)

        b_event.set()
        await flush_event_loop()
        self.assertEqual(instance_1.status, 200)


class ResponseFactoryTestCase(IsolatedAsyncioTestCase):
    mock_state_discovery: AsyncMock
    mock_session: AsyncMock
    mock_clock: AsyncMock
    mock_state: AsyncMock

    async def asyncSetUp(self) -> None:
        self.mock_state = AsyncMock(spec=HostState)
        self.mock_state_discovery = AsyncMock(spec=HostStateDiscovery)
        self.mock_state_discovery.find.return_value = self.mock_state
        self.mock_session = AsyncMock(spec=AbstractClientSession)
        self.mock_clock = AsyncMock(spec=ClockService)

    def instance(self, conf) -> ResponseFactory:
        return ResponseFactory(config=conf,
                               state_discovery=self.mock_state_discovery,
                               session=self.mock_session,
                               clock=self.mock_clock)

    async def test_create_no_hosts(self) -> None:
        factory = self.instance(BackoffConfig(RetryPreference(allowed=2)))
        response = factory.create('http://www.google.com/asdfasf', None)
        self.assertEqual(response._config, RetryPreference(allowed=2))

    async def test_create_with_hosts(self) -> None:
        hosts = { 'www.google.com': HostOverride(allowed=3) }
        dpref = RetryPreference(allowed=2)
        factory = self.instance(BackoffConfig(RetryPreference(allowed=2), hosts))
        response = factory.create('http://www.google.com/asdfasf', None)
        self.assertEqual(response._config, RetryPreference(allowed=3))



