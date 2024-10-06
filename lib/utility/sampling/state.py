import abc
from dataclasses import dataclass, field
from typing import Optional, Self, Tuple, TypeVar, Generic

class AbstractSample(abc.ABC):
    @classmethod
    def empty(cls):
        raise NotImplementedError()

    @abc.abstractmethod
    def __add__(self, other):
        pass

    @abc.abstractmethod
    def __truediv__(self, other):
        pass

    @abc.abstractmethod
    def __sub__(self, other):
        pass

T = TypeVar('T', bound=AbstractSample)

@dataclass
class SamplingConfig:
    min_sample_delta: float = field(default=1.0)
    max_sample_depth: int = field(default=1000)
    max_duration: float = field(default=60)

@dataclass
class SampleState(Generic[T]):
    state: T

    @classmethod
    def copy(Cls, instance, **kwargs):
        return Cls(state=instance.state, **kwargs)

@dataclass
class Sample(SampleState[T]):
    observed: float
    previous: Optional['Sample[T]'] = field(default=None)
    last: Optional['Sample[T]'] = field(default=None)

    @classmethod
    def copy(Cls, instance, **kwargs):
        return super().copy(
            instance,
            observed=instance.observed,
            previous=instance.previous,
            last=None,
            **kwargs)

    @classmethod
    def chain(cls,
              observed: float,
              state: 'SampleState[T]',
              previous: Optional['Sample[T]']) -> 'Sample[T]':
        last = None
        if previous:
            last = previous.last or previous
            previous = cls.copy(previous)

        return super().copy(
            instance=state,
            observed=observed,
            previous=previous,
            last=last,
        )

    def rpm(self: Self) -> T:
        last_t, last_s = (self.last.observed, self.last.state) if self.last else (self.observed, self.state)
        match float(self.observed - last_t):
            case 0.0: return type(self.state).empty()
            case division: return (self.state - last_s) / division

    def truncate(self: Self, config: SamplingConfig) -> None:
        sample_depth = 1
        last_it: Sample[T] = self
        curr_it: Sample[T] | None = self.previous

        while curr_it:
            sample_depth += 1
            too_old = (self.observed - curr_it.observed) > config.max_duration
            too_deep = sample_depth > config.max_sample_depth

            if too_old or too_deep:
                last_it.previous = None
                break
            last_it, curr_it = curr_it, curr_it.previous
        self.last = last_it


