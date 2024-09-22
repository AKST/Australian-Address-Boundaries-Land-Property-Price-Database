from dataclasses import dataclass
from logging import getLogger, Logger
from typing import Any, Dict, Self

from lib.service.clock import ClockService
from lib.service.http.util import url_host
from lib.service.http.client_session import ClientSession, ConnectionError
from lib.service.http.client_session import AbstractClientSession, AbstractGetResponse

from .config import BackoffConfig, RetryPreference

class ExpBackoffClientSession(AbstractClientSession):
    _logger = getLogger(f'{__name__}.ExpBackoffSession')
    _factory: Any
    _session: AbstractClientSession

    def __init__(self, session: AbstractClientSession, factory: Any):
        self._session = session
        self._factory = factory

    @staticmethod
    def create(config: BackoffConfig,
               session: AbstractClientSession | None = None):
        clock = ClockService()
        session = session or ClientSession.create()
        factory = ResponseFactory(session=session, clock=clock, config=config)
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

    def __init__(self, url, headers, config, session, clock):
        self.url = url
        self.headers = headers
        self._config = config

        self._session = session
        self._clock = clock

    async def __aenter__(self):
        attempt, connected = 0, False

        while not connected:
            response = self._session.get(self.url, headers=self.headers)

            try:
                await response.__aenter__()
                if not self._config.should_retry(response.status, attempt):
                    break
            except ConnectionError as e:
                if not self._config.can_retry_on_connection_error(attempt):
                    raise e

            await response.__aexit__(None, None, None)
            await self._clock.sleep(self._config.backoff_duration(attempt))
            attempt += 1

        self.status = response.status
        self._response = response
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._response:
            await self._response.__aexit__(exc_type, exc_value, traceback)
        return False

    async def text(self):
        return await self._response.text()

    async def json(self):
        return await self._response.json()

@dataclass
class ResponseFactory:
    config: BackoffConfig
    clock: ClockService
    session: AbstractClientSession

    def create(self, url, headers):
        config = self.config.with_host(url_host(url))
        headers = headers or {}
        return ExpBackoffGetResponse(url,
                                     headers,
                                     config,
                                     self.session,
                                     self.clock)

