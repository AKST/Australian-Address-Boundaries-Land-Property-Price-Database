from dataclasses import dataclass
from logging import getLogger, Logger
from typing import Any, Dict, Self, AsyncIterator

from lib.service.clock import ClockService
from lib.service.http.util import url_host
from lib.service.http.client_session import *

from .config import BackoffConfig, RetryPreference
from .host_state import HostStateDiscovery, HostState

class ExpBackoffClientSession(AbstractClientSession):
    _logger = getLogger(f'{__name__}.ExpBackoffSession')
    _state: HostStateDiscovery
    _factory: 'ResponseFactory'
    _session: AbstractClientSession

    def __init__(self,
                 session: AbstractClientSession,
                 factory: 'ResponseFactory'):
        self._session = session
        self._factory = factory

    @staticmethod
    def create(config: BackoffConfig,
               session: AbstractClientSession | None = None):
        clock = ClockService()
        session = session or ClientSession.create()
        state_discovery = HostStateDiscovery()
        factory = ResponseFactory(session=session,
                                  state_discovery=state_discovery,
                                  clock=clock,
                                  config=config)
        return ExpBackoffClientSession(session, factory)

    def get(self, url, headers=None):
        return self._factory.create(url, headers)

    async def __aenter__(self):
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._session.__aexit__(exc_type, exc_value, traceback)
        return False

    @property
    def closed(self):
        return self._session.closed

class ExpBackoffGetResponse(AbstractGetResponse):
    _logger: Logger = getLogger(f'{__name__}.ExpBackoffGetResponse')

    url: str
    headers: Dict[str, str]
    status: int | None = None
    _config: RetryPreference

    _response: AbstractGetResponse | None = None
    _session: AbstractClientSession
    _clock: ClockService
    _state: HostState

    def __init__(self, url, headers, config, state, session, clock) -> None:
        self.url = url
        self.headers = headers
        self._config = config
        self._state = state

        self._session = session
        self._clock = clock

    async def __aenter__(self):
        attempt, connected = 0, False
        blocking = None

        try:
            while not connected:
                await self._state.wait_if_necessary(blocking)
                response = self._session.get(self.url, headers=self.headers)

                try:
                    value = await response.__aenter__()
                    # probalby not obvious but this also unblocks on '200'
                    if not self._config.should_retry(response.status, attempt):
                        break
                except ConnectionError as e:
                    if not self._config.can_retry_on_connection_error(attempt):
                        raise e

                await response.__aexit__(None, None, None)
                await self._clock.sleep(self._config.backoff_duration(attempt))
                attempt += 1

                if not blocking:
                    blocking = await self._state.block_other_requests_to_host()

            self.status = response.status
            self._response = response
            return self
        finally:
            if blocking:
                self._state.release(blocking)

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._response:
            await self._response.__aexit__(exc_type, exc_value, traceback)
        return False

    async def stream(self, chunk_size: int):
        if not self._response:
            raise ValueError('outside of context')

        async for chunk in self._response.stream(chunk_size):
            yield chunk

    async def text(self) -> str:
        if not self._response:
            raise ValueError('outside of context')
        return await self._response.text()

    async def json(self):
        if not self._response:
            raise ValueError('outside of context')
        return await self._response.json()

@dataclass
class ResponseFactory:
    config: BackoffConfig
    clock: ClockService
    session: AbstractClientSession
    state_discovery: HostStateDiscovery

    def create(self, url, headers):
        host = url_host(url)
        config = self.config.with_host(host)
        state = self.state_discovery.find(host, config)
        headers = headers or {}
        return ExpBackoffGetResponse(url,
                                     headers,
                                     config,
                                     state,
                                     self.session,
                                     self.clock)

