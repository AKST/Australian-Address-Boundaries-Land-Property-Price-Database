import asyncio
from datetime import datetime
from dataclasses import dataclass
import geopandas as gpd
import numpy
import pandas as pd
import warnings
from logging import getLogger
from shapely.geometry import shape
from typing import Any, Dict, List, Self, Set, Tuple, Optional

from lib.service.database import DatabaseService, PgClientException, log_exception_info_df

from .config import (
    GisProjection,
    IngestionTaskDescriptor,
    FeaturePageDescription,
    SchemaFieldFormat,
)
from .feature_server_client import FeatureServerClient
from .telemetry import GisPipelineTelemetry


@dataclass(frozen=True)
class GisIngestionConfig:
    api_workers: int
    api_worker_backpressure: int
    db_workers: int
    chunk_size: Optional[int]

class GisIngestion:
    """
    This Chunk size column exists to
    """
    _logger = getLogger(f'{__name__}.GisIngestion')
    _stopped = False
    _listeners = 0
    _fetch_queue: asyncio.Queue[IngestionTaskDescriptor.Fetch]
    _save_queue: asyncio.Queue[IngestionTaskDescriptor.Save]
    """
    These are the tasks spawned by dispatch task.
    """
    _bg_ts: Set[asyncio.Task]

    def __init__(self: Self,
                 config: GisIngestionConfig,
                 feature_server: FeatureServerClient,
                 db: DatabaseService,
                 telemetry: GisPipelineTelemetry,
                 save_queue: asyncio.Queue[IngestionTaskDescriptor.Save]):
        self.config = config
        self._db = db
        self._telemetry = telemetry
        self._bg_ts = set()

        self._feature_server = feature_server
        self._fetch_queue = asyncio.Queue()
        self._save_queue = save_queue

    @staticmethod
    def create(config: GisIngestionConfig,
               feature_server: FeatureServerClient,
               db: DatabaseService,
               telemetry: GisPipelineTelemetry):
        # setting a max queue size here establishes some back
        # pressure to limit how much ends up getting queued
        save_queue = asyncio.Queue[IngestionTaskDescriptor.Save](
            maxsize=config.api_worker_backpressure)
        return GisIngestion(config, feature_server, db,
                            telemetry, save_queue)

    def stop(self: Self):
        self._stopped = True
        for t in self._bg_ts:
            if not t.done():
                t.cancel()
        self._bg_ts = set()

    def queue_page(self: Self, t_desc: IngestionTaskDescriptor.Fetch) -> None:
        self._bg_ts.add(asyncio.create_task(self._fetch_queue.put(t_desc)))

    async def ingest(self: Self, tg: asyncio.TaskGroup) -> None:
        async def ingest_db_work() -> None:
            while self.is_consuming():
                try:
                    t_desc = await asyncio.wait_for(self._save_queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                await self._save(t_desc)
                await asyncio.sleep(0)

        async def ingest_api_work() -> None:
            while self.is_consuming():
                try:
                    t_desc = await asyncio.wait_for(self._fetch_queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                await self._fetch(t_desc)
                await asyncio.sleep(0)

        try:
            tasks = [
                *[tg.create_task(ingest_db_work()) for i in range(0, self.config.db_workers)],
                *[tg.create_task(ingest_api_work()) for i in range(0, self.config.api_workers)],
            ]
            await asyncio.gather(*tasks)
        except Exception as e:
            for t in tasks:
                t.cancel()
            self.stop()
            raise e

    async def _fetch(self: Self, t_desc_fetch: IngestionTaskDescriptor.Fetch):
        projection, page_desc = t_desc_fetch.projection, t_desc_fetch.page_desc
        self._telemetry.record_fetch_start(t_desc_fetch)
        page = await self._feature_server.get_page(projection, page_desc)
        self._telemetry.record_fetch_end(t_desc_fetch, len(page))

        if len(page) != t_desc_fetch.page_desc.expected_results:
            e_size = t_desc_fetch.page_desc.expected_results
            # self._logger.warning(f"missing results, {len(page)} {e_size}")

        t_desc_save = IngestionTaskDescriptor.Save(
            projection, page_desc, build_df(projection, page))
        await self._save_queue.put(t_desc_save)
        self._telemetry.record_save_queue(t_desc_save, len(page))

    async def _save(self: Self, t_desc: IngestionTaskDescriptor.Save):
        self._telemetry.record_save_start(t_desc, len(t_desc.df))
        proj, page_desc, df = t_desc.projection, t_desc.page_desc, t_desc.df

        match proj.schema.db_relation:
            case None:
                self._telemetry.record_save_skip(t_desc, len(df))
                return
            case db_relation:
                pass

        df_copy, query = prepare_query(proj, df)
        async with self._db.async_connect() as conn:
            async with conn.cursor() as cur:
                slice, rows = [], df_copy.to_records(index=False).tolist()
                cusor, size = 0, self.config.chunk_size or len(rows)
                try:
                    for cursor in range(0, len(rows), size):
                        slice = rows[cursor:cursor + size]
                        await cur.executemany(query, slice)
                except PgClientException as e:
                    self.stop()
                    for row in slice:
                        self._logger.error(f"Row {row[:-1]}")
                    log_exception_info_df(df_copy.iloc[cursor:cursor+size], self._logger, e)
                    raise e
            await conn.commit()
        self._telemetry.record_save_end(t_desc, len(t_desc.df))

    def is_consuming(self: Self) -> bool:
        if self._stopped:
            return False

        done_ts = {t for t in self._bg_ts if t.done()}
        self._bg_ts -= done_ts

        return not self._save_queue.empty() \
            or not self._fetch_queue.empty() \
            or self._listeners > 0 \
            or bool(self._bg_ts)

    async def __aenter__(self):
        self._listeners += 1
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self._listeners -= 1

_logger = getLogger(f'{__name__}.util')
_Formats = Dict[str, SchemaFieldFormat]

def create_placeholder(col: str, fmts: _Formats, crs: int) -> str:
    match format:
        case 'geometry':
            return f"ST_GeomFromText(%s, {crs})"
    return '%s'

def prepare_query(p: GisProjection, df: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, str]:
    def apply_dt(x: Optional[int]) -> Optional[str]:
        max_ms = 2147483647000
        if x is None or x > max_ms or numpy.isnan(x):
            return None
        return datetime.fromtimestamp(x // 1000).strftime('%Y-%m-%d %H:%M:%S')

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
            try:
                match fmt:
                    case 'geometry':
                        copy[k] = copy[k].apply(lambda geom: geom if geom.is_valid else geom.buffer(0))
                        copy[k] = copy[k].apply(lambda geom: geom.wkt if geom is not None else None)
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
                    _logger.error(f"Failed to transform column '{k}' to '{fmt}'\n{p}\n{copy[k]}\n{copy.head()}\n{copy.info()}")
                raise e

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
