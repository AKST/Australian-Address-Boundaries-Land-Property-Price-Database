import calendar
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import math
from typing import Any, Tuple

@dataclass
class PredicateFunction:
    field: str
    kind: str

    def default_param(self, scope):
        raise ValueError(f"no default avalilable for {self.kind}")

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
class DatePredicateFunction(PredicateFunction):
    default_range: Tuple[int, int]
    
    def __init__(self, field: str, default_range):
        super().__init__(field, 'date')
        self.default_range = default_range

    def default_param(self, scope):
        start, end = self.default_range
        start, end = YearMonth(start, 1), YearMonth(end, 1)
        return DateRangeParam(start, end, scope=scope)
    
class PredicateParam:
    kind: str
    scope: str
    
    def __init__(self, kind, scope):
        self.kind = kind
        self.scope = scope
    
    def apply(self, field):
        raise NotImplementedError()

    def can_shard(self):
        raise NotImplementedError()

    def shard(self):
        raise NotImplementedError()

    def can_cache(self):
        raise NotImplementedError()

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
        r = self.end - self.start
        # if r < 1000:
        #     sub_division = 20
        #     points = [self.start + (i*(r/sub_division)) for i in range(0, sub_division-1)]
        #     points = [self.start, *points[1:], self.end]
        # else:
        n = 12
        ls = math.log10(self.start) if self.start > 0 else 0.0
        le = math.log10(self.end)
        step = (le - ls) / (n - 1)
        points = [(10**(ls + i * step)) for i in range(n)]
        points = [self.start, *points[1:-1], self.end]
        yield from (self._scoped(points[i], points[i+1]) for i in range(0, len(points)-1))
        
class DateRangeParam(PredicateParam):
    start: Any
    end: Any
    
    def __init__(self, start, end, scope=None):
        super().__init__('date', scope=scope)
        self.start = start
        self.end = end

    def _scoped(self, start, end):
        return DateRangeParam(start, end, scope=self.scope)

    def can_cache(self):
        today = YearMonth.from_date(datetime.now())
        return today > self.end
        
    def shard(self):
        if self.end > self.start.next_year(n=100):
            iterator = self.start.years_between(self.end, n=100)
            next_date = lambda d: d.next_year(n=100).min(self.end)
        elif self.end > self.start.next_year(n=5):
            iterator = self.start.years_between(self.end, n=5)
            next_date = lambda d: d.next_year(n=5).min(self.end)
        elif self.end > self.start.next_year():
            iterator = self.start.years_between(self.end)
            next_date = lambda d: d.next_year()
        elif self.end > self.start.next_month():
            iterator = self.start.months_between(self.end)
            next_date = lambda d: d.next_month()
        else:
            iterator = self.start.days_between(self.end)
            next_date = lambda d: d.next_day()
        
        for this_d in iterator:
            yield self._scoped(this_d, next_date(this_d))

    def apply(self, field: str):
        lower_b = f"{field} >= DATE '{str(self.start)}'"
        upper_b = f"{field} < DATE '{str(self.end)}'"
        query = ' AND '.join([lower_b, upper_b])
        return f'{self.scope} AND {query}' if self.scope else query

    def can_shard(self):
        return self.start.next_day() != self.end
    
@dataclass
class YearMonth:
    year: int
    month: int
    day: int = field(default=1)

    def __str__(self):
        return f'{self.year}-{self.month}-{self.day}'

    def min(self, other):
        return other if self > other else self

    @staticmethod
    def from_date(date):
        return YearMonth(date.year, date.month, date.day)

    def days_between(self, other):
        if self == other:
            return
        elif self > other:
            yield from other.days_between(self)
        elif self.next_month() != other:
            raise NotImplementedError()
        else:
            _, days = calendar.monthrange(self.year, self.month)
            yield from (YearMonth(self.year, self.month, d) for d in range(self.day, days + 1))
            yield from (YearMonth(other.year, other.month, d) for d in range(1, other.day))
        
    def months_between(self, other):
        if self == other:
            return
        elif self > other:
            yield from other.months_between(self)
        elif self.year == other.year:
            yield from (YearMonth(self.year, m) for m in range(self.month, other.month))
        else:
            yield from (YearMonth(self.year, m) for m in range(self.month, 13))
            yield from (YearMonth(y, m) for y in range(self.year+1, other.year) for m in range(1, 13))
            yield from (YearMonth(other.year, m) for m in range(1, other.month))
        
    def years_between(self, other, n=1):
        if self == other:
            return
        elif self > other:
            yield from other.years_between(self)
        else:
            yield from (YearMonth(y, 1) for y in range(self.year, other.year, n))
            yield YearMonth(other.year, 1)

    def __eq__(self, other):
        if isinstance(other, YearMonth):
            return self.year == other.year and self.month == other.month and self.day == other.day
        return False

    def __gt__(self, other):
        if isinstance(other, YearMonth):
            return self.year > other.year or (self.year == other.year and self.month > other.month)
        return False

    def __ge__(self, other):
        return self > other or self == other
        
    def as_date(self):
        return date(self.year, self.month, self.day)

    def next_day(self):
        return YearMonth.from_date(self.as_date() + timedelta(days=1))

    def next_month(self):
        if self.month == 12:
            return YearMonth(self.year+1, 1)
        return YearMonth(self.year, self.month+1)
        
    def next_year(self, n=1):
        return YearMonth(self.year + n, self.month)

