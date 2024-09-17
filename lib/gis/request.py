from collections import namedtuple
from dataclasses import dataclass, field
from typing import Set, Any, Optional, List, Tuple

from .where_clause import YearMonth

@dataclass
class SchemaField:
    category: str
    name: str
    priority: int = field(default=1)

@dataclass
class GisSchema:
    url: str
    id_field: str
    result_limit: int
    fields: List[str]
    date_filter_column: str
    debug_plot_column: str

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
    