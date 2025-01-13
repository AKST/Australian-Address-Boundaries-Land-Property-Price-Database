from datetime import datetime
import geopandas as gpd
from logging import getLogger
import numpy
import pandas as pd
import warnings
from typing import (
    Dict,
    Literal,
    Optional,
    Tuple,
)

FieldFormat = Literal[
    'bool',
    'geometry',
    'timestamp_ms',
    'number',
    'text',
]

_FormatDict = Dict[str, FieldFormat]

_logger = getLogger(__name__)

def prepare_postgis_insert(
    df: gpd.GeoDataFrame,
    relation: str,
    epsg_crs: int,
    column_formats: _FormatDict,
    clone = True
) -> Tuple[gpd.GeoDataFrame, str]:
    def create_placeholder(col: Optional[FieldFormat], crs: int) -> str:
        return '%s'

    def apply_dt(x: Optional[int]) -> Optional[str]:
        max_ms = 2147483647000
        if x is None or x > max_ms or numpy.isnan(x):
            return None
        return datetime.fromtimestamp(x // 1000).strftime('%Y-%m-%d %H:%M:%S')

    def apply_geometry(g):
        if g is None:
            return None
        if not g.is_valid:
            g = g.buffer(0)
        return g.wkt

    try:
        placeholders = ", ".join(create_placeholder(column_formats.get(c, None), epsg_crs) for c in df.columns)
    except Exception as e:
        _logger.error(f'error message: {str(e)}')
        _logger.error(f'columns: {df.columns}')
        _logger.error(set(column_formats.keys()))
        raise e

    columns = ", ".join(df.columns)
    query = f"INSERT INTO {relation} ({columns}) VALUES ({placeholders})"

    copy = df.copy() if clone else df
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for k, fmt in column_formats.items():
            try:
                match fmt:
                    case 'bool':
                        copy[k] = copy[k].apply(lambda x: 'true' if x else 'false')
                    case 'geometry':
                        copy[k] = copy[k].apply(apply_geometry)
                    case 'timestamp_ms':
                        max_ms = 2147483647000
                        copy[k] = copy[k].apply(apply_dt)
                    case 'text':
                        copy[k] = copy[k].astype(object)
                    case 'number':
                        copy[k] = copy[k].astype(object)
                        copy[[k]] = copy[[k]].where(pd.notnull(copy[[k]]), None)
            except Exception as e:
                with pd.option_context('display.max_columns', None):
                    _logger.error(f"Failed to transform column '{k}' to '{fmt}'\n{copy[k]}\n{copy.head()}\n{copy.info()}")
                raise e
    return copy, query


