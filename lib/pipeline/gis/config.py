from collections import namedtuple
from dataclasses import dataclass, field
import geopandas as gpd
from typing import Optional, Set, Any, Iterator, Literal, List, Tuple, Self

from lib.utility.df import FieldFormat

from .predicate import YearMonth, PredicateFunction

FieldPriority = str | List[str | Tuple[str, int]]

class IngestionTaskDescriptor:
    @dataclass(frozen=True)
    class Fetch:
        projection: 'GisProjection'
        page_desc: 'FeaturePageDescription'

    @dataclass(frozen=True)
    class Save:
        projection: 'GisProjection'
        page_desc: 'FeaturePageDescription'
        df: gpd.GeoDataFrame

@dataclass(frozen=True)
class FeaturePageDescription:
    where_clause: str
    offset: int
    expected_results: int
    use_cache: bool

@dataclass(frozen=True)
class SchemaField:
    category: str
    name: str
    priority: int = field(default=1)
    """
    This is the field name once it's loaded into data.
    """
    rename: Optional[str] = field(default=None)
    format: Optional[FieldFormat] = field(default=None)

@dataclass(frozen=True)
class GisSchema:
    url: str
    id_field: str
    db_relation: Optional[str]
    result_limit: int
    result_depth: int
    fields: List[SchemaField]
    shard_scheme: List[PredicateFunction]
    debug_field: str

    @property
    def debug_plot_column(self: Self) -> str:
        return next(
            f.rename or f.name
            for f in self.fields
            if f.name == self.debug_field
        )

@dataclass(frozen=True)
class GisProjection:
    id: str
    schema: GisSchema
    fields: FieldPriority
    epsg_crs: int

    def partition_key(self: Self) -> str:
        fields = '-'.join(f'{f.name}' for f in self.get_fields())
        return f'{self.id}-{self.epsg_crs}-[{fields}]'

    def get_fields(self: Self) -> Iterator[SchemaField]:
        if isinstance(self.fields, str) and self.fields == '*':
            return (f for f in self.schema.fields)

        elif isinstance(self.fields, str):
            # if you set `fields` to a str, you should only set it to '*'
            raise ValueError('invalid projection')

        requirements = [((r, 3) if isinstance(r, str) else r) for r in self.fields]

        return (
            f
            for category, priority in requirements
            for f in self.schema.fields
            if f.category == category and f.priority <= priority
        )

@dataclass(frozen=True)
class Bounds:
    xmin: float
    ymin: float
    ymax: float
    xmax: float

    def area(self: Self):
        return self.x_range() * self.y_range()

    def x_range(self: Self):
        return self.xmax - self.xmin

    def y_range(self: Self):
        return self.ymax - self.ymin

