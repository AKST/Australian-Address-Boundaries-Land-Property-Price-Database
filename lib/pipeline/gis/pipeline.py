import asyncio
from dataclasses import dataclass
import geopandas as gpd
from logging import getLogger
import math
from random import shuffle
from shapely.geometry import shape
from typing import Any, AsyncIterator, Dict, List, Self, Tuple, Sequence, Set

from .config import (
    GisSchema,
    GisProjection,
    IngestionTaskDescriptor,
    FeaturePageDescription,
)
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

    async def start(self, projections: List[Tuple[GisProjection, Sequence[PredicateParam]]]):
        async with asyncio.TaskGroup() as tg:
            try:
                tasks = [
                    tg.create_task(self._scrap_projection(tg, proj, params))
                    for proj, params in projections
                ]
                await asyncio.sleep(0)
                await asyncio.gather(tg.create_task(self._ingestion.ingest(tg)), *tasks)
            except Exception as e:
                for t in tasks:
                    t.cancel()
                self._ingestion.stop()
                raise e

    async def _scrap_projection(self, tg, proj: GisProjection, params: Sequence[PredicateParam]) -> None:
        async with self._ingestion:
            sharder = self._sharder_factory.create(tg, proj)
            async for page in sharder.shard(params):
                task = IngestionTaskDescriptor.Fetch(proj, page)
                self._ingestion.queue_page(task)

