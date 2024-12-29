import asyncio
from dataclasses import dataclass
import geopandas as gpd
from logging import getLogger
import math
from random import shuffle
from shapely.geometry import shape
from typing import Any, AsyncIterator, Dict, List, Self, Tuple, Sequence, Set

from .config import GisSchema, GisProjection, FeaturePageDescription
from .ingestion import GisIngestion
from .predicate import PredicateParam
from .feature_pagination_sharding import FeaturePaginationSharderFactory

StreamItem = Tuple[GisProjection, FeaturePageDescription, Any]

class GisPipeline:
    _logger = getLogger(f'{__name__}.GisProducer')

    def __init__(self: Self,
                 sharder_factory: FeaturePaginationSharderFactory,
                 ingestion: GisIngestion):
        self._ingestion = ingestion
        self._sharder_factory = sharder_factory

    async def produce(self, proj: GisProjection, params: Sequence[PredicateParam]) -> None:
        async with asyncio.TaskGroup() as tg:
            async with self._ingestion:
                sharder = self._sharder_factory.create(tg, proj)
                async for page in sharder.shard(params):
                    self._ingestion.dispatch_task(tg, proj, page)

