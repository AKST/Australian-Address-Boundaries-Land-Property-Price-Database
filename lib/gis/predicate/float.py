from dataclasses import dataclass, field
import math
from itertools import chain
from typing import Tuple, Iterator, List

from .base import PredicateFunction, PredicateParam

@dataclass
class FloatPredicateFunction(PredicateFunction):
    default_range: Tuple[int, int]

    def __init__(self, field: str, default_range):
        super().__init__(field, 'float')
        self.default_range = default_range

    def default_param(self, scope):
        start, end = self.default_range
        return FloatRangeParam(start, end, scope=scope)

@dataclass
class FloatRangeParam(PredicateParam):
    start: float
    end: float

    def __init__(self, start, end, scope=None):
        super().__init__('float', scope=scope)
        self.start = start
        self.end = end

    def can_cache(self):
        return True

    def _scoped(self, start: float, end: float) -> 'FloatRangeParam':
        return FloatRangeParam(start, end, scope=self.scope)

    def apply(self, field: str) -> str:
        lower_b = f"{field} >= {self.start}"
        upper_b = f"{field} < {self.end}"
        query = ' AND '.join([lower_b, upper_b])
        return f'{self.scope} AND {query}' if self.scope else query

    def can_shard(self):
        return True

    def shard(self) -> Iterator['FloatRangeParam']:
        n = 12
        ls = math.log10(self.start) if self.start > 0 else 0.0
        le = math.log10(self.end)
        step = (le - ls) / (n - 1)

        points = round_items(
            chain(
                [self.start],
                ((10**(ls + i * step)) for i in range(1, n - 1)),
                [self.end],
            )
        )

        last = next(points)
        for item in points:
            yield self._scoped(last, item)
            last = item

def round_items(ls: Iterator[float]) -> Iterator[float]:
    def minimal_rounding(item, last):
        n = 0
        while True:
            factor = 10 ** n
            attempt = math.floor(item * factor) / factor
            if attempt > last:
                return attempt
            n = n + 1

    last = next(ls)
    last_raw = last
    for item in ls:
        yield last

        if item - last > 1:
            item_mod = float(math.floor(item))
        else:
            item_mod = minimal_rounding(item, last)
        last_raw, last = item, item_mod
    yield last_raw


