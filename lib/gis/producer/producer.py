import asyncio
from dataclasses import dataclass
import geopandas as gpd
from logging import getLogger
import math
from random import shuffle
from typing import Any, AsyncIterator, List, Tuple

from lib.async_util import pipe, merge_async_iters
from lib.gis.predicate import PredicateParam
from lib.gis.request import GisSchema, GisProjection
from lib.service.http import AbstractClientSession
from lib.service.http.cache import CacheHeader
from lib.service.http.util import url_with_params

from .counts import ClauseCounts
from .component_factory import ComponentFactory
from .errors import GisNetworkError, GisTaskNetworkError, MissingResultsError

@dataclass
class ProjectionTask:
    where_clause: str
    offset: int
    expected_results: int
    use_cache: bool


StreamItem = Tuple[GisProjection, ProjectionTask, Any]


class GisProducer:
    _logger = getLogger(f'{__name__}.GisProducer')
    _streams: List['GisStream']

    def __init__(self, session: AbstractClientSession):
        self._session = session
        self._streams = []

    def queue(self, proj: GisProjection):
        self._streams.append(GisStream.create(self._session, proj))

    def progress(self, p: GisProjection, task: ProjectionTask):
        it = (s.progress_str(task) for s in self._streams if s.projection == p)
        return next(it, None)

    async def produce(self, params: List[PredicateParam]) -> AsyncIterator[StreamItem]:
        async with asyncio.TaskGroup() as tg:
            iters = [ss.run_all(tg, params) for ss in self._streams]
            async for result in merge_async_iters(iters):
                yield result


class GisStream:
    _logger = getLogger(f'{__name__}.GisStream')
    _counts: ClauseCounts

    def __init__(self,
                 projection: GisProjection,
                 factory: ComponentFactory,
                 session: AbstractClientSession):
        self.projection = projection
        self._session = session
        self._factory = factory
        self._counts = ClauseCounts()

    @staticmethod
    def create(session, projection: GisProjection):
        return GisStream(
            projection,
            ComponentFactory(projection.schema.id_field),
            session,
        )

    def progress_str(self, task: ProjectionTask) -> str:
        amount, total = self._counts.progress(task.where_clause)
        percent = math.floor(100*(amount/total))
        return f'({amount}/{total}) {percent}% progress for {task.where_clause}'

    async def run_all(self,
                      tg: asyncio.TaskGroup,
                      params: List[PredicateParam]) -> AsyncIterator[StreamItem]:
        schema = self.projection.schema
        limit = schema.result_limit

        producer = lambda: self._chunk_into_tasks(tg, params)
        consumer = lambda shard: self._run_task(shard)
        async for task, gdf in pipe(producer, consumer, tg):
            if len(gdf) > 0:
                yield self.projection, task, gdf

    async def _chunk_into_tasks(self,
                                tg: asyncio.TaskGroup,
                                params: List[PredicateParam]):
        """
        So some of the GIS servers I've interacted with have a habit of dying
        when you paginate on a query well beyond 50k records. So one of the
        goals here is create shards that are no more than 20K (or whatever
        I configure it too).
        """
        limit = self.projection.schema.result_limit

        async def shard_count(shard_param, field, use_cache):
            where_clause = shard_param.apply(field)
            use_cache = use_cache and shard_param.can_cache()
            task = self._get_count(where_clause, use_cache=use_cache)
            return await tg.create_task(task), shard_param

        async def chunks(where_clause, shard_functions, params, use_cache):
            shard_p, *shard_ps = params if params else [None]
            shard_f, *shard_fs = shard_functions

            if shard_p == None:
                shard_p = shard_f.default_param(where_clause)

            shard_count_queue = list(shard_p.shard())
            requires_extra_param = []

            shuffle(shard_count_queue)
            while shard_count_queue:
                counts = [
                    shard_count(shard_param, shard_f.field, use_cache)
                    for shard_param in shard_count_queue
                ]

                shard_count_queue = []

                for count, shard in await asyncio.gather(*counts):
                    query = shard.apply(shard_f.field)
                    _use_cache = use_cache and shard.can_cache()
                    if count == 0:
                        continue
                    elif count <= limit * 150:
                        self._counts.init_clause(query, count)
                        for offset in range(0, count, limit):
                            expected = min(limit, offset % limit)
                            yield ProjectionTask(query, offset, expected, use_cache=_use_cache)
                    elif shard.can_shard():
                        shard_count_queue.extend(list(shard.shard()))
                    else:
                        requires_extra_param.append(shard)
                shuffle(shard_count_queue)
            shuffle(requires_extra_param)

            for shard in requires_extra_param:
                query = shard.apply(shard_f.field)
                _use_cache = use_cache and shard.can_cache()
                async for p in chunks(query, shard_fs, shard_ps, use_cache=_use_cache):
                    yield p

        shard_scheme = self.projection.schema.shard_scheme
        async for c in chunks(None, shard_scheme, params, use_cache=True):
            yield c

    async def _run_task(self, task: ProjectionTask) -> Tuple[ProjectionTask, Any]:
        p = self.projection
        try:
            params = {
                'returnGeometry': True,
                'resultOffset': task.offset,
                'resultRecordCount': p.schema.result_limit,
                'where': task.where_clause,
                'geometryType': 'esriGeometryEnvelope',
                'outSR': p.epsg_crs,
                'outFields': ','.join(p.get_fields()),
                'f': 'json',
            }
            data = await self._get_json(params, use_cache=task.use_cache, cache_name='page')
        except GisNetworkError as e:
            self._logger.error(f'failed on schema {p.schema.url}')
            self._logger.error(f'failed on task {task}')
            self._logger.error(self.progress_str(task))
            raise GisTaskNetworkError(task, e.http_status, e.response)

        features = data.get('features', [])
        if len(features) < task.expected_results:
            self._logger.error("Potenial data loss has occured")
            raise MissingResultsError(
                f'{task.where_clause} OFFSET {task.offset}, '
                f'got {len(features)} wanted {task.expected_results}')

        self._counts.inc(task.where_clause, len(features))
        geometries, attributes = [], []
        for f in features:
            component = self._factory.from_feature(f)
            if component:
                geometries.append(component.geometry)
                attributes.append(component.attributes)

        gdf = gpd.GeoDataFrame(
            attributes,
            geometry=geometries,
            crs=f"EPSG:{p.epsg_crs}",
        ) if attributes else gpd.GeoDataFrame()
        return task, gdf

    async def _get_count(self, where_clause: str | None, use_cache: bool) -> int:
        params = { 'where': where_clause or '1=1', 'returnCountOnly': True, 'f': 'json' }
        response = await self._get_json(params, use_cache=use_cache, cache_name='count')
        try:
            count = response.get('count', 0)
            self._logger.debug(f'count for "{where_clause}" is {count}')
            return count
        except Exception as e:
            url = url_with_params(f'{self.projection.schema.url}/query', params)
            print(f'failed on {url}', e)
            raise e

    async def _get_json(self, params, use_cache, cache_name=None):
        url = url_with_params(f'{self.projection.schema.url}/query', params)
        async with self._session.get(url, headers={
            CacheHeader.EXPIRE: 'never' if use_cache else 'delta:days:2',
            CacheHeader.FORMAT: 'json',
            CacheHeader.LABEL: cache_name,
        }) as response:
            if response.status != 200:
                self._logger.error(f"Crashed at {url}")
                self._logger.error(response)
                raise GisNetworkError(response.status, response)
            return await response.json()

