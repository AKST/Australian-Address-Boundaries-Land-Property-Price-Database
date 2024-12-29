import asyncio
from dataclasses import dataclass
import geopandas as gpd
from logging import getLogger
import math
from random import shuffle
from shapely.geometry import shape
from typing import Any, AsyncIterator, Dict, List, Self, Tuple, Sequence, Set

from ..config import GisSchema, GisProjection, FeaturePageDescription
from ..ingestion import GisIngestion
from ..predicate import PredicateParam
from .feature_pagination_sharding import FeaturePaginationSharderFactory

StreamItem = Tuple[GisProjection, FeaturePageDescription, Any]

# Todo rename pipeline?
class GisProducer:
    _logger = getLogger(f'{__name__}.GisProducer')
    _streams: List['GisStream']

    def __init__(self: Self, stream_factory: 'GisStreamFactory'):
        self._stream_factory = stream_factory
        self._streams = []

    def queue(self: Self, proj: GisProjection):
        self._streams.append(self._stream_factory.create(proj))

    async def scrape(self, params: Sequence[PredicateParam]):
        async with asyncio.TaskGroup() as tg:
            await asyncio.gather(*[s.scrape(params) for s in self._streams])

class GisStream:
    _logger = getLogger(f'{__name__}.GisStream')

    def __init__(self: Self,
                 projection: GisProjection,
                 sharder_factory: FeaturePaginationSharderFactory,
                 ingestion: GisIngestion):
        self.projection = projection
        self._ingestion = ingestion
        self._sharder_factory = sharder_factory

    async def scrape(self: Self, params: Sequence[PredicateParam]):
        async with asyncio.TaskGroup() as tg:
            sharder = self._sharder_factory.create(tg, self.projection)
            async with self._ingestion:
                async for page in sharder.shard(params):
                    await self._ingestion.queue_page(self.projection, page)

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

