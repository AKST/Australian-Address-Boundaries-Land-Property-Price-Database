import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from logging import getLogger
import random
from typing import AsyncIterator, List, Optional, Self, Tuple, Sequence

from lib.pipeline.gis.config import GisProjection, FeaturePageDescription
from lib.pipeline.gis.feature_server_client import FeatureServerClient
from lib.pipeline.gis.predicate import PredicateFunction, PredicateParam

from ..telemetry import GisPipelineTelemetry

class FeaturePaginationSharderFactory:
    def __init__(
        self: Self,
        feature_server: FeatureServerClient,
        telemetry: GisPipelineTelemetry,
        shuffle: Callable[[List[PredicateParam]], None] = random.shuffle,
    ) -> None:
        self._feature_server = feature_server
        self._telemetry = telemetry
        self._shuffle = shuffle

    def create(self: Self, tg: asyncio.TaskGroup, proj: GisProjection):
        return RequestSharder(tg, proj, self._feature_server, self._telemetry, self._shuffle)

class RequestSharder:
    _logger = getLogger(f'{__name__}')

    def __init__(self: Self,
                 tg: asyncio.TaskGroup,
                 projection: GisProjection,
                 feature_server: FeatureServerClient,
                 telemetry: GisPipelineTelemetry,
                 shuffle: Callable[[List[PredicateParam]], None]):
        self._tg = tg
        self._projection = projection
        self._feature_server = feature_server
        self._telemetry = telemetry
        self._shuffle = shuffle

    async def shard(self: Self, params: Sequence[PredicateParam]) -> AsyncIterator[FeaturePageDescription]:
        shard_scheme = self._projection.schema.shard_scheme
        async for c in self._recursive_shard(None, shard_scheme, params, use_cache=True):
            yield c

    async def _recursive_shard(self: Self,
                               where_clause: Optional[str],
                               shard_functions: Sequence[PredicateFunction],
                               params: Sequence[PredicateParam],
                               use_cache: bool) -> AsyncIterator[FeaturePageDescription]:
        limit = self._projection.schema.result_limit
        shard_f, *shard_fs = shard_functions
        shard_p, *shard_ps = params if params else [shard_f.default_param(where_clause)]

        shard_count_queue = list(shard_p.shard())
        requires_extra_param = []

        self._shuffle(shard_count_queue)
        while shard_count_queue:
            counts = [
                self._shard_count(shard_param, shard_f.field, use_cache)
                for shard_param in shard_count_queue
            ]

            shard_count_queue = []

            for count, shard in await asyncio.gather(*counts):
                query = shard.apply(shard_f.field)
                _use_cache = use_cache and shard.can_cache()
                if count == 0:
                    continue
                elif count <= limit * 150:
                    self._telemetry.init_clause(self._projection, query, count)
                    for offset in range(0, count, limit):
                        expected = min(limit, offset % limit)
                        yield FeaturePageDescription(query, offset, expected, use_cache=_use_cache)
                elif shard.can_shard():
                    shard_count_queue.extend(list(shard.shard()))
                else:
                    requires_extra_param.append(shard)
            self._shuffle(shard_count_queue)
        self._shuffle(requires_extra_param)

        for shard in requires_extra_param:
            query = shard.apply(shard_f.field)
            _use_cache = use_cache and shard.can_cache()
            async for p in self._recursive_shard(query, shard_fs, shard_ps, use_cache=_use_cache):
                yield p

    async def _shard_count(self: Self,
                           shard_param: PredicateParam,
                           field: str,
                           use_cache: bool) -> Tuple[int, PredicateParam]:
        where_clause = shard_param.apply(field)
        use_cache = use_cache and shard_param.can_cache()
        return await self._tg.create_task(self._feature_server.get_where_count(
            projection=self._projection,
            where_clause=where_clause,
            use_cache=use_cache,
        )), shard_param
