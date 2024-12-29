import asyncio
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
from ..telemetry import GisPipelineTelemetry

class GisIngestion:
    """
    This Chunk size column exists to
    """
    chunk_size: Optional[int]
    _queue: asyncio.Queue[Tuple[GisProjection, FeaturePageDescription]]
    _logger = getLogger(f'{__name__}.GisStream')
    _listeners = 0

    def __init__(self: Self,
                 feature_server: FeatureServerClient,
                 db: DatabaseService,
                 telemetry: GisPipelineTelemetry,
                 chunk_size: Optional[int] = None):
        self._db = db
        self._feature_server = feature_server
        self._queue = asyncio.Queue()
        self._telemetry = telemetry
        self.chunk_size = chunk_size

    async def ingest(self: Self, listeners: int):
        async def child_ingest():
            while self._listeners > 0:
                try:
                    proj, page = await asyncio.wait_for(self._queue.get(), timeout=0.5)
                    await self.consume(proj, page)
                except asyncio.TimeoutError:
                    continue

        while self._queue.empty():
            await asyncio.sleep(0.1)

        await asyncio.gather(*[
            child_ingest()
            for i in range(0, listeners)
        ])

    async def queue_page(self: Self, projection: GisProjection, page_desc: FeaturePageDescription):
        await self._queue.put((projection, page_desc))

    async def consume(self: Self, projection: GisProjection, page_desc: FeaturePageDescription):
        page = await self._feature_server.get_page(projection, page_desc)
        self._telemetry.record_fetched(projection, page_desc.where_clause, len(page))
        df = build_df(projection, page)
        db_relation = projection.schema.db_relation

        if not db_relation:
            return page_desc, df

        df_copy, query = prepare_query(projection, df)
        async with self._db.async_connect() as conn:
            async with conn.cursor() as cur:
                slice, rows = [], df_copy.to_records(index=False).tolist()
                cusor, size = 0, self.chunk_size or len(rows)
                try:
                    for cursor in range(0, len(rows), size):
                        slice = rows[cursor:cursor + size]
                        await cur.executemany(query, slice)
                except PgClientException as e:
                    for row in slice:
                        self._logger.error(f"Row {row[:-1]}")
                    log_exception_info_df(df_copy.iloc[cursor:cursor+size], self._logger, e)
                    raise e
            await conn.commit()
        self._telemetry.record_saved(projection, page_desc.where_clause, len(page))

    async def __aenter__(self):
        self._listeners += 1
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self._listeners -= 1

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
                    max_ms = 2147483647000
                    copy[k] = copy[k].apply(lambda x: \
                        datetime.fromtimestamp(x // 1000).strftime('%Y-%m-%d %H:%M:%S') if x <= max_ms else None)
                case 'nullable_num':
                    copy[k] = copy[k].astype(object)
                    copy[[k]] = copy[[k]].where(pd.notnull(copy[[k]]), None)

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
