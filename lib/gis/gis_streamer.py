from aiohttp import ClientSession
import asyncio
from collections import namedtuple
import geopandas as gpd
from itertools import tee
import json
from logging import getLogger
import math
from random import shuffle
from shapely.geometry import shape
from typing import Set, Any, Optional, List, Tuple, Dict

from lib.gis.predicate import PredicateParam
from lib.gis.request import GisSchema, GisProjection
from lib.http.cached_http import CacheExpire, CacheHeader
from lib.http.util import url_with_params

ProjectionTask = namedtuple('ProjectionChunkTask', [
    'where_clause',
    'offset',
    'expected_results',
    'use_cache',
])

class GisStreamer:
    _logger = getLogger(f'{__name__}.GisStreamer')
    
    def __init__(self, session: ClientSession):
        self._session = session
        self._sub_streams = []

    def queue(self, proj: GisProjection):
        self._sub_streams.append(GisSubStream.create(self._session, proj))

    def progress(self, p: GisProjection, task: ProjectionTask):
        return next((
            s.progress_str(task)
            for s in self._sub_streams
            if s.projection == p
        ), None)

    async def consume(self, params: List[PredicateParam]):
        async with asyncio.TaskGroup() as tg:
            iters = [ss.run_all(tg, params) for ss in self._sub_streams]
            async for result in self._merge_async_iters(iters):
                yield result
            
    async def _merge_async_iters(self, iters):
        # Create an asyncio.Task for each async generator
        tasks = {asyncio.create_task(it.__anext__()): it for it in iters}
    
        while tasks:
            # Wait for any of the tasks to complete
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    
            for task in done:
                iterator = tasks.pop(task)
    
                try:
                    # Yield the result from the completed task
                    result = task.result()
                    yield result
    
                    # Schedule the next item from the same iterator
                    tasks[asyncio.create_task(iterator.__anext__())] = iterator
                except StopAsyncIteration:
                    # When the iterator is exhausted, it is not rescheduled
                    continue

class GisSubStream:
    _logger = getLogger(f'{__name__}.GisSubStream')
    _counts = None
    
    def __init__(self,
                 projection: GisProjection,
                 factory,
                 session: ClientSession):
        self.projection = projection
        self._session = session
        self._factory = factory

    @staticmethod
    def create(session, projection: GisProjection):
        return GisSubStream(
            projection,
            _GisDfComponentFactory(projection.schema.id_field),
            session,
        )

    def progress_str(self, task: ProjectionTask):
        amount, total = self._counts.progress(task.where_clause)
        percent = math.floor(100*(amount/total))
        return f'({amount}/{total}) {percent}% progress for {task.where_clause}'
            
    async def run_all(self, tg: asyncio.TaskGroup, params: List[PredicateParam]):
        schema = self.projection.schema
        limit = schema.result_limit
        
        try:
            producer = lambda: self._chunk_into_tasks(tg, params)
            consumer = lambda shard: tg.create_task(self._run_task(shard)) 
            async for task, gdf in pipe(producer, consumer):
                if len(gdf) > 0:
                    yield self.projection, task, gdf
        except GisAbortError:
            self._logger("aborting tasks")

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
        self._counts = _ClauseCounts()

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
    
    async def _run_task(self, task: ProjectionTask):
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
            self._logger("Potenial data loss has occured")
            raise GisMissingResults(
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
        
        count = response.get('count', 0)
        self._logger.debug(f'count for "{where_clause}" is {count}')
        return count

    async def _get_json(self, params, use_cache, cache_name=None):
        url = url_with_params(f'{self.projection.schema.url}/query', params)
        async with self._session.get(url, headers={ 
            CacheHeader.DISABLED: 'False' if use_cache else 'True',
            CacheHeader.EXPIRE: CacheExpire.NEVER,
            CacheHeader.FORMAT: 'json',
            CacheHeader.NAME: cache_name,
        }) as response:
            json = await response.json()
            if response.status != 200:
                self._logger.error(f"Crashed at {url}")
                self._logger.error(response)
                raise GisNetworkError(response.status, response)
            return json

async def pipe(producer, consumer):
    pending_tasks = set()
    producer_coroutine = producer()
    
    try:
        while True:
            # Start tasks as shards become available
            shard_task = asyncio.create_task(producer_coroutine.__anext__())
            # Wait for either a shard to become available or a task to complete
            done, _ = await asyncio.wait(
                [shard_task] + list(pending_tasks),
                return_when=asyncio.FIRST_COMPLETED
            )

            if shard_task in done:
                try:
                    task = consumer(await shard_task)
                    pending_tasks.add(task)
                except StopAsyncIteration:
                    # Producer is exhausted
                    break
                except Exception as e:
                    raise e

            # Process any completed tasks
            completed_tasks = done - {shard_task}
            for task in completed_tasks:
                pending_tasks.remove(task)
                result = await task
                if result is not None:
                    yield result

    except StopAsyncIteration:
        pass

    # Process remaining pending tasks
    for task in asyncio.as_completed(pending_tasks):
        result = await task
        if result is not None:
            yield result

class _ClauseCounts:
    def __init__(self, counts=None):
        self._counts = counts or {}

    def init_clause(self, clause, count):
        self._counts[clause] = (0, count)
        
    @staticmethod
    def from_list(ls: List[Tuple[int, str]]):
        counts = { clause: (0, count) for count, clause in ls }
        return _ClauseCounts(counts)

    def inc(self, clause, amount):
        progress, total = self._counts[clause]
        self._counts[clause] = (progress + amount, total)

    def progress(self, clause):
        return self._counts[clause]

# These are the components required to assemble a dataframe
_GisDfComponents = namedtuple('GisDfComponents', ['geometry', 'attributes'])

class _GisDfComponentFactory:
    _seen: Set[Any] = set()
    _id_Field: str

    def __init__(self, id_field: str):
        self._id_field = id_field

    def from_feature(self, f) -> Optional[_GisDfComponents]:
        obj_id = f['attributes'][self._id_field]
        if obj_id in self._seen:
            return
            
        geometry = f['geometry']
        if 'rings' in geometry:
            geom = shape({"type": "Polygon", "coordinates": geometry['rings']})
        elif 'paths' in geometry:
            geom = shape({"type": "LineString", "coordinates": geometry['paths']})
        else:
            geom = shape(geometry)
            
        self._seen.add(obj_id)
        return _GisDfComponents(geom, f['attributes'])

class GisAbortError(Exception):
    """
    This gets fired if one of the tasks ends up with
    less results than expected. It's important to 
    raise expections here to flag data loss.
    """
    pass

class GisMissingResults(Exception):
    pass

class GisReaderError(Exception):
    offset: Any
    projection: GisProjection
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class GisNetworkError(GisReaderError):
    def __init__(self, http_status, response, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_status = http_status
        self.response = response    

class GisTaskNetworkError(GisNetworkError):
    def __init__(self, task, http_status, response, *args, **kwargs):
        super().__init__(http_status, response, *args, **kwargs)
        self.task = task