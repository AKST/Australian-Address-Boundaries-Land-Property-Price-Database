from dataclasses import dataclass, field
import math
from typing import Tuple

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

class FloatRangeParam(PredicateParam):
    start: float
    end: float

    def __init__(self, start, end, scope=None):
        super().__init__('float', scope=scope)
        self.start = start
        self.end = end

    def can_cache(self):
        return True

    def _scoped(self, start, end):
        return FloatRangeParam(start, end, scope=self.scope)

    def apply(self, field: str):
        lower_b = f"{field} >= {self.start}"
        upper_b = f"{field} < {self.end}"
        query = ' AND '.join([lower_b, upper_b])
        return f'{self.scope} AND {query}' if self.scope else query

    def can_shard(self):
        return True

    def shard(self):
        n, r = 12, self.end - self.start
        ls = math.log10(self.start) if self.start > 0 else 0.0
        le = math.log10(self.end)
        step = (le - ls) / (n - 1)
        points = [(10**(ls + i * step)) for i in range(n)]
        points = [self.start, *points[1:-1], self.end]
        yield from (self._scoped(points[i], points[i+1]) for i in range(0, len(points)-1))


