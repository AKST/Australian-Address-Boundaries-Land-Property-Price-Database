import calendar
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Tuple, Iterator

from lib.service.clock import ClockService

from .base import PredicateFunction, PredicateParam


@dataclass
class DateRangeParamFactory:
    clock: ClockService = field(repr=False)
    def create(self, start: 'YearMonth', end: 'YearMonth', scope: str) -> 'DateRangeParam':
        return DateRangeParam(start, end, scope=scope, clock=self.clock)

@dataclass
class DatePredicateFunction(PredicateFunction):
    default_range: Tuple[int, int]
    _factory: DateRangeParamFactory = field(repr=False)

    def __init__(self,
                 field: str,
                 default_range: Tuple[int, int],
                 factory: DateRangeParamFactory):
        super().__init__(field, 'date')
        self.default_range = default_range
        self._factory = factory

    @staticmethod
    def create(field: str, default_range: Tuple[int, int]):
        factory = DateRangeParamFactory(clock=ClockService())
        return DatePredicateFunction(field, default_range, factory)

    def default_param(self, scope: str):
        start, end = self.default_range
        return self._factory.create(YearMonth(start, 1),
                                    YearMonth(end, 1),
                                    scope=scope)

class DateRangeParam(PredicateParam):
    start: 'YearMonth'
    end: 'YearMonth'
    _clock: ClockService = field(repr=False)

    def __init__(self, start, end, clock: ClockService, scope=None):
        super().__init__('date', scope=scope)
        self.start = start
        self.end = end
        self._clock = clock

    def __repr__(self):
        return f'DateRangeParam({self.start}, {self.end}, {self.scope})'

    def _scoped(self, start: 'YearMonth', end: 'YearMonth'):
        return DateRangeParam(start, end, scope=self.scope, clock=self._clock)

    def can_cache(self) -> bool:
        today = YearMonth.from_date(self._clock.now())
        return today > self.end

    def shard(self) -> Iterator['DateRangeParam']:
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

    def apply(self, field: str) -> str:
        lower_b = f"{field} >= DATE '{str(self.start)}'"
        upper_b = f"{field} < DATE '{str(self.end)}'"
        query = ' AND '.join([lower_b, upper_b])
        return f'{self.scope} AND {query}' if self.scope else query

    def can_shard(self):
        return self.start.next_day() != self.end

    def __eq__(self, other):
        return isinstance(other, DateRangeParam) \
           and self.start == other.start \
           and self.end == other.end \
           and self.scope == other.scope

@dataclass
class YearMonth:
    year: int
    month: int
    day: int = field(default=1)

    def __str__(self):
        return f'{self.year}-{self.month}-{self.day}'

    def min(self, other: 'YearMonth') -> 'YearMonth':
        return other if self > other else self

    @staticmethod
    def from_date(date) -> 'YearMonth':
        return YearMonth(date.year, date.month, date.day)

    def days_between(self, other) -> Iterator['YearMonth']:
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

    def months_between(self, other: 'YearMonth') -> Iterator['YearMonth']:
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

    def years_between(self, other: 'YearMonth', n=1) -> Iterator['YearMonth']:
        if self == other:
            return
        elif self > other:
            yield from other.years_between(self)
        else:
            yield from (YearMonth(y, 1) for y in range(self.year, other.year, n))

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

    def as_date(self) -> date:
        return date(self.year, self.month, self.day)

    def next_day(self) -> 'YearMonth':
        return YearMonth.from_date(self.as_date() + timedelta(days=1))

    def next_month(self) -> 'YearMonth':
        if self.month == 12:
            return YearMonth(self.year+1, 1)
        return YearMonth(self.year, self.month+1)

    def next_year(self, n=1) -> 'YearMonth':
        return YearMonth(self.year + n, self.month)

