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
from lib.utility.df import prepare_postgis_insert, FieldFormat, fmt_head

from .config import (
    GisProjection,
    IngestionTaskDescriptor,
    FeaturePageDescription,
)
from .feature_server_client import FeatureServerClient
from .telemetry import GisPipelineTelemetry


@dataclass(frozen=True)
class GisIngestionConfig:
    api_workers: int
    api_worker_backpressure: int
    dry_run: bool
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

    async def ingest(self: Self) -> None:
        await asyncio.sleep(0)

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
            await asyncio.gather(*[
                *[ingest_db_work() for i in range(0, self.config.db_workers)],
                *[ingest_api_work() for i in range(0, self.config.api_workers)],
            ])
        except Exception as e:
            self.stop()
            raise e

    async def _fetch(self: Self, t_desc_fetch: IngestionTaskDescriptor.Fetch):
        projection, page_desc = t_desc_fetch.projection, t_desc_fetch.page_desc
        self._telemetry.record_fetch_start(t_desc_fetch)
        page = await self._feature_server.get_page(projection, page_desc)
        self._telemetry.record_fetch_end(t_desc_fetch, len(page))
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

        df_copy, query = prepare_query(db_relation, proj, df)
        if self.config.dry_run:
            self._logger.info(fmt_head(df))
            self.stop()
            raise asyncio.CancelledError('dryrun')

        async with self._db.async_connect() as conn:
            async with conn.cursor() as cur:
                slice, rows = [], df_copy.to_records(index=False).tolist()
                cursor, size = 0, self.config.chunk_size or len(rows)
                try:
                    for cursor in range(0, len(rows), size):
                        slice = rows[cursor:cursor + size]
                        await cur.executemany(query, slice)
                except PgClientException as e:
                    self.stop()
                    log_exception_info_df(df_copy.iloc[cursor:cursor+size], self._logger, e)
                    await self._feature_server.forget_page_cache(proj, page_desc)
                    raise e
                except Exception as e:
                    await self._feature_server.forget_page_cache(proj, page_desc)
                    raise e
            await conn.commit()
        self._telemetry.record_save_end(t_desc, len(t_desc.df))
        t_desc.df.drop(t_desc.df.index, inplace=True)

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
_Formats = Dict[str, FieldFormat]

def create_placeholder(col: str, fmts: _Formats, crs: int) -> str:
    match format:
        case 'geometry':
            return f"ST_GeomFromText(%s, {crs})"
    return '%s'

def prepare_query(db_relation: str, p: GisProjection, df: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, str]:
    try:
        return prepare_postgis_insert(df,
            relation=db_relation,
            epsg_crs=p.epsg_crs,
            column_formats={
                'geometry': 'geometry',
                **({
                    (f.rename or f.name): f.format
                    for f in p.get_fields() if f.format
                })
            },
            clone=True,
        )
    except:
        from pprint import pformat
        _logger.error(pformat(p))
        raise

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
