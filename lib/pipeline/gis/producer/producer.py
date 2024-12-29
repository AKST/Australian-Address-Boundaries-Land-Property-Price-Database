import asyncio
from dataclasses import dataclass
import geopandas as gpd
from logging import getLogger
import math
from random import shuffle
from shapely.geometry import shape
from typing import Any, AsyncIterator, Dict, List, Self, Tuple, Sequence, Set

from lib.utility.concurrent import pipe, merge_async_iters

from ..config import GisSchema, GisProjection, FeaturePageDescription
from ..ingestion import GisIngestion
from ..predicate import PredicateParam
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
                 ingestion: GisIngestion):
        self.projection = projection
        self._ingestion = ingestion
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
        consumer = lambda shard: self._ingestion.consume(self.projection, shard)
        async for page_desc, gdf in pipe(producer, consumer, tg):
            self._logger.info(str(page_desc))
            self._counts.inc(page_desc.where_clause, len(gdf))
            if len(gdf) > 0:
                yield self.projection, page_desc, gdf

class GisStreamFactory:
    def __init__(self: Self,
                 sharder_factory: FeaturePaginationSharderFactory,
                 ingestion: GisIngestion) -> None:
        self._sharder_factory = sharder_factory
        self._ingestion = ingestion

    def create(self: Self, projection: GisProjection) -> GisStream:
        return GisStream(
            projection,
            self._sharder_factory,
            self._ingestion,
        )

