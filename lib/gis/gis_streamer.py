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

from lib.gis.where_clause import GisPredicate
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

    async def consume(self, predicate: GisPredicate):
        async with asyncio.TaskGroup() as tg:
            iters = [ss.run_all(tg, predicate) for ss in self._sub_streams]
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
            
    async def run_all(self, tg: asyncio.TaskGroup, predicate: GisPredicate):
        schema = self.projection.schema
        limit = schema.result_limit
        
        try:
            tasks = await self._chunk_into_tasks(tg, predicate)
            tasks = [tg.create_task(self._run_task(t)) for t in tasks]
            for task in asyncio.as_completed(tasks):
                task_desc, gdf = await task
                if len(gdf) > 0:
                    yield self.projection, task_desc, gdf
        except GisAbortError:
            self._logger("aborting tasks")
    
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

    async def _chunk_into_tasks(self, tg: asyncio.TaskGroup, predicate: GisPredicate):
        schema = self.projection.schema
        limit = schema.result_limit
        
        counts = await asyncio.gather(*[
            tg.create_task(self._get_count(chunk))
            for chunk in predicate.chunk()
        ])
        counts = [(count, clause.to_str(schema.date_filter_column)) for count, clause in counts]
        
        self._counts = _ClauseCounts.from_list(counts)
        return [
            ProjectionTask(clause, offset, (offset % limit))
            for count, clause in counts
            for offset in range(0, count, limit)
        ]
        # for count, chunk in counts:
        #     yield chunk
            
    async def _get_count(self, chunk: GisPredicate) -> (int, str): 
        where_clause = chunk.to_str(self.projection.schema.date_filter_column)
        params = { 'where': where_clause, 'returnCountOnly': True, 'f': 'json' }
        count = (await self._get_json(params)).get('count', 0)
        self._logger.debug(f'count for "{where_clause}" is {count}')
        return (count, chunk)

    async def _get_json(self, params):
        url = f'{self.projection.schema.url}/query'
        return await self._session.get_json(url, params)

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