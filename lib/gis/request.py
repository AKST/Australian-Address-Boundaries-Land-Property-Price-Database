from collections import namedtuple
from dataclasses import dataclass
from typing import Set, Any, Optional, List, Tuple

SchemaField = namedtuple('SchemaField', ['category', 'name', 'priority'])

@dataclass
class GisSchema:
    url: str
    id_field: str
    result_limit: int
    fields: List[str]

@dataclass
class GisProjection:
    schema: GisSchema
    fields: str | List[str | Tuple[str, str]]
    epsg_crs: Any

    def get_fields(self):
        if isinstance(self.fields, str) and self.fields == '*':
            return (f.name for f in self.schema.fields)
        
        elif isinstance(self.fields, str):
            # if you set `fields` to a str, you should only set it to '*'
            raise ValueError('invalid projection')
        
        requirements = [((r, 3) if isinstance(r, str) else r) for r in self.fields]
        
        return (
            f.name
            for category, priority in requirements
            for f in self.schema.fields
            if f.category == category and f.priority <= priority
        )
    
@dataclass
class YearMonth:
    year: int
    month: int
    
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
            return self.year == other.year and self.month == other.month
        return False

    def __gt__(self, other):
        if isinstance(other, YearMonth):
            return self.year > other.year or (self.year == other.year and self.month > other.month)
        return False

    def next_month(self):
        if self.month == 12:
            return YearMonth(self.year+1, 1)
        return YearMonth(self.year, self.month+1)

@dataclass
class GisPredicate:
    start: YearMonth
    end: YearMonth

    def chunk_by_month(self):
        for this_m in self.start.months_between(self.end):
            next_m = this_m.next_month()
            yield ' AND '.join([
                f"lastupdate >= DATE '{this_m.year}-{this_m.month}-01'",
                f"lastupdate < DATE '{next_m.year}-{next_m.month}-01'",
            ])

@dataclass
class Bounds:
    xmin: float
    ymin: float
    ymax: float
    xmax: float

    def area(self):
        return self.x_range() * self.y_range()

    def x_range(self):
        return self.xmax - self.xmin

    def y_range(self):
        return self.ymax - self.ymin
    