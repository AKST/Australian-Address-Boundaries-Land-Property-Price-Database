import calendar
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

class GisPredicate:
    def chunk(self, field):
        raise NotImplementedError()

@dataclass
class DateRange(GisPredicate):
    start: Any
    end: Any

    def to_str(self, field: str):
        lower_b = f"{field} >= DATE '{str(self.start)}'"
        upper_b = f"{field} < DATE '{str(self.end)}'"
        return ' AND '.join([lower_b, upper_b])
        

    def chunk(self):
        if self.start.next_month() == self.end:
            iterator = self.start.days_between(self.end)
            next_date = lambda d: d.next_day()
        else:
            iterator = self.start.months_between(self.end)
            next_date = lambda d: d.next_month()
        
        for this_d in iterator:
            yield DateRange(this_d, next_date(this_d))
    
@dataclass
class YearMonth:
    year: int
    month: int
    day: int = field(default=1)

    def __str__(self):
        return f'{self.year}-{self.month}-{self.day}'

    def days_between(self, other):
        if self == other:
            return
        elif self > other:
            return other.months_between(self)
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
            return other.months_between(self)
        elif self.year == other.year:
            yield from (YearMonth(self.year, m) for m in range(self.month, other.month))
        else:
            yield from (YearMonth(self.year, m) for m in range(self.month, 13))
            yield from (YearMonth(y, m) for y in range(self.year+1, other.year) for m in range(1, 13))
            yield from (YearMonth(other.year, m) for m in range(1, other.month))

    def __eq__(self, other):
        if isinstance(other, YearMonth):
            return self.year == other.year and self.month == other.month and self.day == other.day
        return False

    def __gt__(self, other):
        if isinstance(other, YearMonth):
            return self.year > other.year or (self.year == other.year and self.month > other.month)
        return False

    def next_day(self):
        current_date = date(self.year, self.month, self.day)
        next_day = current_date + timedelta(days=1)
        return YearMonth(next_day.year, next_day.month, next_day.day)

    def next_month(self):
        if self.month == 12:
            return YearMonth(self.year+1, 1)
        return YearMonth(self.year, self.month+1)