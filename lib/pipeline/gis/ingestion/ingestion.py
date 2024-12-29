from datetime import datetime
import geopandas as gpd
import pandas as pd
import warnings
from logging import getLogger
from shapely.geometry import shape
from typing import Any, Dict, List, Self, Tuple, Optional

from lib.service.database import DatabaseService, PgClientException, log_exception_info_df

from ..config import (
    GisProjection,
    FeaturePageDescription,
    SchemaFieldFormat,
)
from ..feature_server_client import FeatureServerClient

class GisIngestion:
    _logger = getLogger(f'{__name__}.GisStream')

    def __init__(self: Self, feature_server: FeatureServerClient, db: DatabaseService):
        self._db = db
        self._feature_server = feature_server

    async def consume(
        self: Self,
        projection: GisProjection,
        page_desc: FeaturePageDescription
    ) -> Tuple[FeaturePageDescription, gpd.GeoDataFrame]:
        page = await self._feature_server.get_page(projection, page_desc)
        df = build_df(projection, page)
        db_relation = projection.schema.db_relation

        if not db_relation:
            return page_desc, df

        df_copy, query = prepare_query(projection, df)
        async with self._db.async_connect() as conn:
            async with conn.cursor() as cur:
                rows = df_copy.to_records(index=False).tolist()
                try:
                    await cur.executemany(query, rows)
                except PgClientException as e:
                    self._logger.error(f"projection {projection}")
                    print([r[16] for r in rows])
                    log_exception_info_df(df_copy, self._logger, e)
                    raise e
            await conn.commit()
        return page_desc, df

_Formats = Dict[str, SchemaFieldFormat]

def create_placeholder(col: str, fmts: _Formats, crs: int) -> str:
    match format:
        case 'geometry':
            return f"ST_GeomFromText(%s, {crs})"
    return '%s'

def prepare_query(p: GisProjection, df: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, str]:
    fmts: _Formats = { (f.rename or f.name): f.format for f in p.schema.fields if f.format }
    fmts = { 'geometry': 'geometry', **fmts }

    relation = p.schema.db_relation
    placeholders = ", ".join(create_placeholder(c, fmts, p.epsg_crs) for c in df.columns)
    columns = ", ".join(df.columns)
    query = f"INSERT INTO {relation} ({columns}) VALUES ({placeholders})"

    copy = df.copy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for k, fmt in fmts.items():
            match fmt:
                case 'geometry':
                    copy[k] = copy[k].apply(lambda geom: geom if geom.is_valid else geom.buffer(0))
                    copy[k] = copy[k].apply(lambda geom: geom.wkt if geom is not None else None)
                case 'timestamp_ms':
                    # copy[k] = pd.to_datetime(copy[k], unit='ms')
                    # print(type(copy.info()))
                    # max_ms = (datetime.max - datetime(1970, 1, 1)).total_seconds()
                    max_ms = 2147483647000
                    copy[k] = copy[k].apply(lambda x: \
                        datetime.fromtimestamp(x // 1000).strftime('%Y-%m-%d %H:%M:%S') if x <= max_ms else None)

    return copy, query

def build_df(proj: GisProjection, page: List[Any]) -> gpd.GeoDataFrame:
    components: List[Tuple[Any, Dict[str, Any]]] = []

    for feature in page:
        obj_id = feature['attributes'][proj.schema.id_field]

        geometry = feature['geometry']
        if 'rings' in geometry:
            geom = shape({"type": "Polygon", "coordinates": geometry['rings']})
        elif 'paths' in geometry:
            geom = shape({"type": "LineString", "coordinates": geometry['paths']})
        else:
            geom = shape(geometry)

        components.append((geom, feature['attributes']))

    if not components:
        return gpd.GeoDataFrame()

    geometries, attributes = [list(c) for c in zip(*components)]
    return gpd.GeoDataFrame(
        attributes,
        geometry=geometries,
        crs=f"EPSG:{proj.epsg_crs}",
    ).rename(columns={
        f.name: f.rename
        for f in proj.get_fields()
        if f.rename
     })
