from dataclasses import dataclass, field
from functools import reduce
from logging import getLogger
import math
from typing import Self, Optional, List, Tuple, Dict

from lib.service.clock import ClockService
from .config import GisProjection

@dataclass
class ShardState:
    shard_size: int
    fetched_from_network: int = field(default=0)
    saved_to_database: int = field(default=0)

    def __add__(self, other: 'ShardState') -> 'ShardState':
        return ShardState(
            self.shard_size + other.shard_size,
            self.fetched_from_network + other.fetched_from_network,
            self.saved_to_database + other.saved_to_database,
        )

    def count(self):
        return f'(Saved {self.saved_to_database} Fetched {self.fetched_from_network} Total: {self.shard_size})'

    def progress(self):
        r, w, t = self.fetched_from_network, self.saved_to_database, self.shard_size
        fetch_p = math.floor(100*(r/t))
        saved_p = math.floor(100*(w/r)) if r > 0 else 0
        total_p = math.floor(100*(w/t))
        return f'Complete: {total_p}% Fetched: {fetch_p}% Saved: {saved_p}%'

_StateMap = Dict[str, Dict[str, ShardState]]

class GisPipelineTelemetry:
    _state_map: _StateMap
    _logger = getLogger(__name__)

    """
    This mostly exist for the purpose of tracking
    progress in the producer so people can actually
    see progress take place.
    """
    def __init__(self: Self,
                 clock: ClockService,
                 start_time: float,
                 counts: Optional[_StateMap]):
        self._clock = clock
        self._start_time = start_time
        self._state_map = counts or {}

    @staticmethod
    def create(clock: ClockService) -> 'GisPipelineTelemetry':
        start_time: float = clock.time()
        return GisPipelineTelemetry(clock, start_time, None)

    def init_clause(self: Self, p: GisProjection, clause: str, count):
        if p.id not in self._state_map:
            self._state_map[p.id] = {}
        self._state_map[p.id][clause] = ShardState(count)
        self._log_status(event="Queue")

    def add_projection(self: Self, p: GisProjection, ls: List[Tuple[int, str]]):
        self._state_map[p.id] = { clause: ShardState(count) for count, clause in ls }

    def record_fetched(self, p: GisProjection, clause: str, amount: int):
        state = self._state_map[p.id][clause]
        state.fetched_from_network += amount
        self._log_status(event="Fetch")

    def record_saved(self, p: GisProjection, clause: str, amount: int):
        state = self._state_map[p.id][clause]
        state.saved_to_database += amount
        self._log_status(event="Saved")

    def get_state(self, p: GisProjection, clause: str) -> ShardState:
        return self._state_map[p.id][clause]

    def get_total(self) -> ShardState:
        return reduce(lambda a, b: a + b, [
            shard_state
            for p_map in self._state_map.values()
            for shard_state in p_map.values()
        ])

    def _log_status(self: Self, event: str):
        total = self.get_total()
        t = int(self._clock.time() - self._start_time)
        th, tm, ts = t // 3600, t // 60 % 60, t % 60
        self._logger.info(f"{event} ({th}h {tm}m {ts}s) {total.progress()} ({total.count()})")
