from dataclasses import dataclass, field
from typing import Optional, Self, Tuple

from lib.utility.sampling import AbstractSample

@dataclass
class FloatSample(AbstractSample):
    value: float = field(default=0.0)

    @classmethod
    def empty(cls):
        return FloatSample()

    def __add__(self, other: 'FloatSample') -> 'FloatSample':
        return FloatSample(value=self.value + other.value)

    def __sub__(self, other: 'FloatSample') -> 'FloatSample':
        return FloatSample(value=self.value - other.value)

    def __truediv__(self, other) -> 'FloatSample':
        if isinstance(other, float):
            return FloatSample(value=self.value / other)
        if isinstance(other, int):
            return FloatSample(value=self.value / other)
        raise TypeError(f'cannot divide IngestionSample by {type(other)}')

    def round(self: Self, n: int):
        return FloatSample(round(self.value, n))
