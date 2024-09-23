import asyncio
from typing import Dict

from .config import RetryPreference

class HostState:
    _config: RetryPreference
    _currently_failing: asyncio.Event | None = None

    def __init__(self, config: RetryPreference) -> None:
        self._config = config

    async def block_other_requests_to_host(self) -> asyncio.Event | None:
        if not self._config.pause_other_requests_while_retrying:
            return None

        if self._currently_failing is not None:
            await self._currently_failing.wait()
            return await self.block_other_requests_to_host()
        else:
            self._currently_failing = asyncio.Event()
            return self._currently_failing

    async def wait_if_necessary(self, source: asyncio.Event | None):
        if self._currently_failing is None:
            return

        if source != self._currently_failing:
            await self._currently_failing.wait()

    def release(self, source: asyncio.Event):
        if source == self._currently_failing:
            self._currently_failing.set()
            self._currently_failing = None

    @property
    def semaphore(self):
        return self._semaphore

class HostStateDiscovery:
    _states: Dict[str, 'HostState']

    def __init__(self) -> None:
        self._states = {}

    def find(self, host, pref: RetryPreference) -> 'HostState':
        if host not in self._states:
            self._states[host] = HostState(pref)
        return self._states[host]

