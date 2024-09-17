from aiohttp import ClientSession
import asyncio
from collections import namedtuple
import geopandas as gpd
from itertools import tee
import json
from logging import getLogger
import math
from shapely.geometry import shape
from typing import Set, Any, Optional, List, Tuple

from lib.gis.predicate import PredicateParam
from lib.gis.request import GisSchema, GisProjection

ProjectionTask = namedtuple('ProjectionChunkTask', ['where_clause', 'offset', 'expected_results'])
HostSemaphoreConfig = namedtuple('HostSemaphoreConfig', ['host', 'limit'])

class ThrottledSession:
    _logger = getLogger(f'{__name__}.ThrottledSession')
    
    def __init__(self, session: ClientSession, semaphores):
        self._semaphores = semaphores
        self._session = session

    @staticmethod
    def create(session: ClientSession, host_configs: List[HostSemaphoreConfig]):
        semaphores = { c.host: asyncio.Semaphore(c.limit) for c in host_configs }
        return ThrottledSession(session, semaphores)
    
    async def get_json(self, url: str, params: Any):
        from urllib.parse import urlparse, urlencode

        url_with_params = f"{url}?{urlencode(params)}"
        host = urlparse(url).netloc
        
        async with self._semaphores[host]:
            if self.closed:
                raise GisAbortError("http session has been closed")
        
            async with self._session.get(url_with_params) as response:
                if response.status != 200:
                    self._logger.error(f"Crashed at {url_with_params}")
                    self._logger.error(response)
                    raise GisNetworkError(response.status, response)
                r = await response.json()
                return r

    @property
    def closed(self):
        return self._session.closed
            

class GisStreamer:
    _logger = getLogger(f'{__name__}.GisStreamer')
    
    def __init__(self, session: ThrottledSession):
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
                 session: ThrottledSession):
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
            # producer = lambda: self._chunk_into_tasks(tg, params)
            # consumer = lambda shard: tg.create_task(self._run_task(shard)) 
            # async for task, gdf in pipe(producer, consumer):
            #     if len(gdf) > 0:
            #         yield self.projection, task_desc, gdf
                
            pending = set()
            async for shard in self._chunk_into_tasks(tg, params):
                pending.add(tg.create_task(self._run_task(shard)))
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task_desc, gdf = await task
                    if len(gdf) > 0:
                        yield self.projection, task_desc, gdf
                        
            for task in asyncio.as_completed(pending):
                task_desc, gdf = await task
                if len(gdf) > 0:
                    yield self.projection, task_desc, gdf
        except GisAbortError:
            self._logger("aborting tasks")

    async def _chunk_into_tasks(self, tg: asyncio.TaskGroup, params: List[PredicateParam]):
        limit = self.projection.schema.result_limit
        self._counts = _ClauseCounts()

        async def shard_count(shard, field):
            where_clause = shard.apply(field)
            count = await tg.create_task(self._get_count(where_clause))
            return count, shard

        async def chunks(where_clause, shard_functions, params):
            shard_p, *shard_ps = params if params else [None]
            shard_f, *shard_fs = shard_functions
            
            if shard_p == None:
                shard_p = shard_f.default_param(where_clause)

            shard_count_queue = list(shard_p.shard())
            requires_extra_param = []
            
            while shard_count_queue:
                counts = [shard_count(s, shard_f.field) for s in shard_count_queue]
                shard_count_queue = []
                
                for count, shard in await asyncio.gather(*counts):
                    query = shard.apply(shard_f.field)
                    if count == 0:
                        continue
                    elif count <= limit * 150:
                        self._counts.init_clause(query, count)
                        for offset in range(0, count, limit):
                            expected = min(limit, offset % limit)
                            yield ProjectionTask(query, offset, expected)
                    elif shard.can_shard():
                        shard_count_queue.extend(list(shard.shard()))
                    else:
                        requires_extra_param.append(shard)
 
            for shard in requires_extra_param:
                query = shard.apply(shard_f.field)
                async for p in chunks(query, shard_fs, shard_ps):
                    yield p

        async for c in chunks(None, self.projection.schema.shard_scheme, params):
            yield c
    
    async def _run_task(self, task: ProjectionTask):
        p = self.projection
        try:
            data = await self._get_json({
                'returnGeometry': True,
                'resultOffset': task.offset,
                'resultRecordCount': p.schema.result_limit,
                'where': task.where_clause,
                'geometryType': 'esriGeometryEnvelope',
                'outSR': p.epsg_crs,
                'outFields': ','.join(p.get_fields()),
                'f': 'json',
            })
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
            
    async def _get_count(self, where_clause: str | None) -> (int, str): 
        params = { 'where': where_clause or '1=1', 'returnCountOnly': True, 'f': 'json' }
        count = (await self._get_json(params)).get('count', 0)
        self._logger.debug(f'count for "{where_clause}" is {count}')
        return count

    async def _get_json(self, params):
        url = f'{self.projection.schema.url}/query'
        return await self._session.get_json(url, params)

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
                    shard = shard_task.result()
                    # Start processing the shard
                    task = asyncio.create_task(consumer(shard))
                    pending_tasks.add(task)
                except StopAsyncIteration:
                    # Producer is exhausted
                    break
                except Exception as e:
                    # Handle exceptions from the producer if necessary
                    continue

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