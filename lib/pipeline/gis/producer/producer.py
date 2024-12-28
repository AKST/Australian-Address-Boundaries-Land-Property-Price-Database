import asyncio
from dataclasses import dataclass
import geopandas as gpd
from logging import getLogger
import math
from random import shuffle
from shapely.geometry import shape
from typing import Any, AsyncIterator, Dict, List, Self, Tuple, Sequence, Set

from lib.pipeline.gis.config import (
    GisSchema,
    GisProjection,
    FeaturePageDescription,
)
from lib.pipeline.gis.feature_server_client import FeatureServerClient
from lib.pipeline.gis.predicate import PredicateParam
from lib.service.http import AbstractClientSession, CacheHeader
from lib.service.http.util import url_with_params
from lib.utility.concurrent import pipe, merge_async_iters

from .counts import ClauseCounts
from .feature_pagination_sharding import FeaturePaginationSharderFactory

StreamItem = Tuple[GisProjection, FeaturePageDescription, Any]

class GisProducer:
    _logger = getLogger(f'{__name__}.GisProducer')
    _streams: List['GisStream']

    def __init__(self: Self, stream_factory: 'GisStreamFactory'):
        self._stream_factory = stream_factory
        self._streams = []

    def queue(self: Self, proj: GisProjection):
        self._streams.append(self._stream_factory.create(proj))

    def progress(self, p: GisProjection, task: FeaturePageDescription):
        it = (s.progress_str(task) for s in self._streams if s.projection == p)
        return next(it, None)

    async def produce(self, params: Sequence[PredicateParam]) -> AsyncIterator[StreamItem]:
        async with asyncio.TaskGroup() as tg:
            iters = [ss.stream_pages(tg, params) for ss in self._streams]
            async for result in merge_async_iters(iters):
                yield result

class GisStream:
    _logger = getLogger(f'{__name__}.GisStream')
    _counts: ClauseCounts
    _seen: Set[Any]

    def __init__(self: Self,
                 projection: GisProjection,
                 sharder_factory: FeaturePaginationSharderFactory,
                 feature_server: FeatureServerClient):
        self.projection = projection
        self._feature_server = feature_server
        self._sharder_factory = sharder_factory
        self._counts = ClauseCounts()
        self._seen = set()

    def progress_str(self, task: FeaturePageDescription) -> str:
        amount, total = self._counts.progress(task.where_clause)
        amount = task.offset + task.expected_results
        percent = math.floor(100*(amount/total))
        return f'({amount}/{total}) {percent}% progress for {task.where_clause}'

    async def stream_pages(self: Self,
                      tg: asyncio.TaskGroup,
                      params: Sequence[PredicateParam]) -> AsyncIterator[StreamItem]:
        schema = self.projection.schema
        limit = schema.result_limit
        sharder = self._sharder_factory.create(tg, self._counts, self.projection)
        producer = lambda: sharder.shard(params)
        consumer = lambda shard: self._run_task(shard)
        async for page, gdf in pipe(producer, consumer, tg):
            if len(gdf) > 0:
                yield self.projection, page, gdf

    async def _run_task(self: Self, page_desc: FeaturePageDescription) -> Tuple[FeaturePageDescription, gpd.GeoDataFrame]:
        page = await self._feature_server.get_page(self.projection, page_desc)
        self._counts.inc(page_desc.where_clause, len(page))
        return page_desc, build_df(self.projection, self._seen, page)

def build_df(proj: GisProjection, seen: Set[Any], page: List[Any]) -> gpd.GeoDataFrame:
    components: List[Tuple[Any, Dict[str, Any]]] = []

    for feature in page:
        obj_id = feature['attributes'][proj.schema.id_field]
        if obj_id in seen:
            continue

        geometry = feature['geometry']
        if 'rings' in geometry:
            geom = shape({"type": "Polygon", "coordinates": geometry['rings']})
        elif 'paths' in geometry:
            geom = shape({"type": "LineString", "coordinates": geometry['paths']})
        else:
            geom = shape(geometry)

        seen.add(obj_id)
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

class GisStreamFactory:
    def __init__(self: Self,
                 feature_server: FeatureServerClient,
                 sharder_factory: FeaturePaginationSharderFactory):
        self._feature_server = feature_server
        self._sharder_factory = sharder_factory

    def create(self: Self, projection: GisProjection) -> GisStream:
        return GisStream(
            projection,
            self._sharder_factory,
            self._feature_server,
        )

