import asyncio
from dataclasses import dataclass
import geopandas as gpd
from logging import getLogger
import math
from random import shuffle
from typing import Any, AsyncIterator, List, Self, Tuple, Sequence

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
from .df_factory import DataframeFactory
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
            iters = [ss.run_all(tg, params) for ss in self._streams]
            async for result in merge_async_iters(iters):
                yield result


class GisStream:
    _logger = getLogger(f'{__name__}.GisStream')
    _counts: ClauseCounts

    def __init__(self: Self,
                 projection: GisProjection,
                 sharder_factory: FeaturePaginationSharderFactory,
                 df_factory: DataframeFactory,
                 feature_server: FeatureServerClient):
        self.projection = projection
        self._feature_server = feature_server
        self._sharder_factory = sharder_factory
        self._df_factory = df_factory
        self._counts = ClauseCounts()

    def progress_str(self, task: FeaturePageDescription) -> str:
        amount, total = self._counts.progress(task.where_clause)
        amount = task.offset + task.expected_results
        percent = math.floor(100*(amount/total))
        return f'({amount}/{total}) {percent}% progress for {task.where_clause}'

    async def run_all(self: Self,
                      tg: asyncio.TaskGroup,
                      params: Sequence[PredicateParam]) -> AsyncIterator[StreamItem]:
        schema = self.projection.schema
        limit = schema.result_limit

        sharder = self._sharder_factory.create(tg, self._counts, self.projection)

        producer = lambda: sharder.shard(params)
        consumer = lambda shard: self._run_task(shard)

        async for task, gdf in pipe(producer, consumer, tg):
            if len(gdf) > 0:
                yield self.projection, task, gdf

    async def _run_task(self: Self, page_desc: FeaturePageDescription) -> Tuple[FeaturePageDescription, gpd.GeoDataFrame]:
        page = await self._feature_server.get_page(self.projection, page_desc)
        self._counts.inc(page_desc.where_clause, len(page))
        return page_desc, self._df_factory.build(self.projection.epsg_crs, page)

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
            DataframeFactory.create(
                projection.schema.id_field,
                projection,
            ),
            self._feature_server,
        )

