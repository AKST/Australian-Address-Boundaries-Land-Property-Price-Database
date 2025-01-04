import abc
from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import Optional, Self, Tuple, TypeVar, Generic

from lib.service.clock import AbstractClockService
from lib.utility.format import fmt_time_elapsed
from .state import AbstractSample, SamplingConfig, Sample, SampleState

T = TypeVar('T', bound=AbstractSample)

@dataclass
class Sampler(SampleState[T]):
    clock: AbstractClockService
    config: SamplingConfig
    last_log: Sample[T] | None
    start_time: float
    logger: Optional[Logger] = field(default=None)

    @staticmethod
    def create(clock: AbstractClockService,
               config: SamplingConfig,
               logger: Logger | None,
               init: T) -> 'Sampler[T]':
        return Sampler(
            clock=clock,
            config=config,
            logger=logger,
            state=init,
            start_time=clock.time(),
            last_log=None,
        )

    def count(self: Self, delta: T) -> None:
        self.state = self.state + delta

    def log_if_necessary(self: Self):
        next_log = self._snapshot_log()
        if self.last_log == next_log and self.logger:
            self.logger.info(self.get_message(next_log))

    def get_message(self: Self, sample: Sample[T]) -> str:
        t = fmt_time_elapsed(self.start_time, self.clock.time(), format="hms")
        return f'({t}) {self.state}'

    def _snapshot_log(self: Self) -> Sample[T]:
        last_log = self.last_log
        this_time = self.clock.time()
        this_log = Sample.chain(this_time, self, self.last_log)

        if last_log is None:
            self.last_log = this_log
        elif this_time - last_log.observed > self.config.min_sample_delta:
            self.last_log = this_log
            self.last_log.truncate(self.config)
        return this_log

