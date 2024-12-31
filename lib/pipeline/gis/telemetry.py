from dataclasses import dataclass, field
from functools import reduce
from logging import getLogger
import math
from pprint import pformat
from typing import Self, Optional, List, Tuple, Dict

from lib.service.clock import ClockService
from .config import GisProjection, IngestionTaskDescriptor

def safe_div(a, b):
    return (a/b) if b > 0 else 0

@dataclass
class ShardStatistics:
    """
    These are statistics relating to progress of ingesting the contents of
    scraped data. Each step in the ingestion is tracked to identify if
    data is being lost at any stage or any step is being blocked. These
    steps should also be used to identify any resource contention in the
    pipeline itself.

    The total state of the pipeline can be gathered from adding all
    of the different steps in the pipeline together.
    """
    shard_size: int
    fetch_started: int
    fetch_completed: int
    save_queued: int
    save_started: int
    save_completed: int

    def finished(self) -> bool:
        return self.shard_size == self.save_completed

    def __add__(self, other: 'ShardStatistics') -> 'ShardStatistics':
        return ShardStatistics(
            shard_size=self.shard_size + other.shard_size,
            fetch_started=self.fetch_started + other.fetch_started,
            fetch_completed=self.fetch_completed + other.fetch_completed,
            save_queued=self.save_queued + other.save_queued,
            save_started=self.save_started + other.save_started,
            save_completed=self.save_completed + other.save_completed,
        )

    def chain(self) -> str:
        a_n = self.shard_size - self.fetch_started
        b_n = self.fetch_started - self.fetch_completed
        c_n = self.fetch_completed - self.save_queued
        d_n = self.save_queued - self.save_started
        e_n = self.save_started - self.save_completed
        f_n = self.save_completed

        return f'Queued ({a_n}, {safe_div(a_n, self.shard_size):.2f}%) ' \
               f'Fetching ({str(b_n).rjust(4)}, {safe_div(b_n, self.fetch_started):.2f}%) ' \
               f'Blocked ({str(c_n).rjust(4)}, {safe_div(c_n, self.fetch_completed):.2f}%) ' \
               f'Queued ({d_n}, {safe_div(d_n, self.save_queued):.2f}%) ' \
               f'Saving ({e_n}, {safe_div(e_n, self.save_started):.2f}%) '  \
               f'Done ({f_n}, {safe_div(f_n, self.shard_size):.2f}%)' \

    def count(self):
        return f'Total {self.shard_size} '\
               f'Fetched {self.fetch_completed} '\
               f'Saved {self.save_completed}'

_StateMap = Dict[str, Dict[str, ShardStatistics]]

class GisPipelineTelemetry:
    """
    This mostly exist for the purpose of tracking progress within the
    ingestion pipeline.
    """

    _logger = getLogger(__name__)
    _state_map: _StateMap
    _clock: ClockService

    """
    Total State is cached in order provide fast access.
    """
    total_state: ShardStatistics

    def __init__(self: Self,
                 clock: ClockService,
                 start_time: float,
                 state_map: Optional[_StateMap]):
        self._clock = clock
        self._start_time = start_time
        self._state_map = state_map or {}
        self.state = ShardStatistics(0, 0, 0, 0, 0, 0)

    @staticmethod
    def create(clock: ClockService) -> 'GisPipelineTelemetry':
        start_time: float = clock.time()
        return GisPipelineTelemetry(clock, start_time, None)

    def init_clause(self: Self, p: GisProjection, clause: str, count):
        if p.id not in self._state_map:
            self._state_map[p.id] = {}
        self._state_map[p.id][clause] = ShardStatistics(count, 0, 0, 0, 0, 0)
        self._log_status(event="Fetch Queue")

    def record_fetch_start(self, t_desc: IngestionTaskDescriptor.Fetch):
        state = self._state_map[t_desc.projection.id][t_desc.page_desc.where_clause]
        state.fetch_started += t_desc.page_desc.expected_results
        self._log_status(event="Fetch Start")

    def record_fetch_end(self, t_desc: IngestionTaskDescriptor.Fetch, amount: int):
        state = self._state_map[t_desc.projection.id][t_desc.page_desc.where_clause]
        state.fetch_completed += amount
        self._log_status(event="Fetch End")

    def record_save_queue(self, t_desc: IngestionTaskDescriptor.Save, amount: int):
        state = self._state_map[t_desc.projection.id][t_desc.page_desc.where_clause]
        state.save_queued += amount
        self._log_status(event="Save Queue")

    def record_save_start(self, t_desc: IngestionTaskDescriptor.Save, amount: int):
        state = self._state_map[t_desc.projection.id][t_desc.page_desc.where_clause]
        state.save_started += amount
        self._log_status(event="Save Start")

    def record_save_end(self, t_desc: IngestionTaskDescriptor.Save, amount: int):
        state = self._state_map[t_desc.projection.id][t_desc.page_desc.where_clause]
        state.save_completed += amount
        self._log_status(event="Save Done")

    def record_save_skip(self, t_desc: IngestionTaskDescriptor.Save, amount: int):
        state = self._state_map[t_desc.projection.id][t_desc.page_desc.where_clause]
        state.save_completed += amount
        self._log_status(event="Save Skip")

    def get_state(self, p: GisProjection, clause: str) -> ShardStatistics:
        return self._state_map[p.id][clause]

    def get_total(self) -> ShardStatistics:
        return reduce(lambda a, b: a + b, [
            shard_state
            for p_map in self._state_map.values()
            for shard_state in p_map.values()
        ])

    def _log_status(self: Self, event: str):
        total = self.get_total()
        t = int(self._clock.time() - self._start_time)
        th, tm, ts = t // 3600, t // 60 % 60, t % 60
        self.total_state = total
        self._logger.info(f"{event.rjust(11)} ({th}h {tm}m {ts}s) " \
                          f"{total.count()}\n{total.chain()}")
