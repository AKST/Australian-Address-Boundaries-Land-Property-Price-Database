from aiohttp import ClientSession
import asyncio
from collections import namedtuple
import geopandas as gpd
import json
from shapely.geometry import shape
from typing import Set, Any, Optional

from lib.gis.request import GisSchema, GisProjection, GisPredicate

ProjectionTask = namedtuple('ProjectionChunkTask', ['where_clause', 'offset', 'expected_results'])

class GisReader:
    def __init__(self, 
                 session: ClientSession,
                 semaphore: asyncio.Semaphore):
        self._session = session 
        self._semaphore = semaphore

    @staticmethod
    def create(session: ClientSession, max_concurrent: int):
        return GisReader(session, asyncio.Semaphore(max_concurrent))

    async def load_projection(self, projection: GisProjection, predicate: GisPredicate):
        schema = projection.schema
        
        counts = await asyncio.gather(*[
            self._get_count(schema.url, clause)
            for clause in predicate.chunk_by_month()
        ])
        
        chunks = [
            ProjectionTask(clause, offset, (offset % schema.result_limit))
            for count, clause in counts
            for offset in range(0, count, schema.result_limit)
        ]
        
        factory = _GisDfComponentFactory(projection.schema.id_field)

        try:
            async with asyncio.TaskGroup() as tg:
                tasks = (self._run_task(projection, t, factory) for t in chunks)
                tasks = list(map(tg.create_task, tasks))
                for task in asyncio.as_completed(tasks):
                    task_desc, gdf = await task
                    if len(gdf) > 0:
                        yield task_desc, gdf
        except GisAbortError:
            return

    
    async def _run_task(self, 
                        projection: GisProjection,
                        task: ProjectionTask,
                        factory):
        async with self._semaphore:
            if self._session.closed:
                raise GisAbortError("http session has been closed")
                
            data = await self._fetch_data(f'{projection.schema.url}/query', {
                'returnGeometry': True,
                'resultOffset': task.offset,
                'resultRecordCount': projection.schema.result_limit,
                'where': task.where_clause,
                'geometryType': 'esriGeometryEnvelope',
                'outSR': projection.epsg_crs,
                'outFields': ','.join(projection.get_fields()),
                'f': 'json',
            })
            
        features = data.get('features', [])
        if len(features) < task.expected_results:
            raise GisMissingResults(
                f'{task.where_clause} OFFSET {task.offset}, '
                f'got {len(features)} wanted {task.expected_results}')
            
        geometries, attributes = [], []
        for f in features:
            component = factory.from_feature(f)
            if component:
                geometries.append(component.geometry)
                attributes.append(component.attributes)
                
        gdf = gpd.GeoDataFrame(
            attributes,
            geometry=geometries,
            crs=f"EPSG:{projection.epsg_crs}",
        ) if attributes else gpd.GeoDataFrame()
        return task, gdf 
            
    async def _get_count(self, url: str, where_clause: str) -> (int, str):        
        params = { 'where': where_clause, 'returnCountOnly': True, 'f': 'json' }
        async with self._semaphore:
            if self._session.closed:
                raise GisAbortError("http session has been closed")
            count_resp = await self._fetch_data(f'{url}/query', params)
            return (count_resp['count'], where_clause)
        

    async def _fetch_data(self, url: str, params: Any):
        import urllib.parse
    
        url_with_params = f"{url}?{urllib.parse.urlencode(params)}"
        async with self._session.get(url_with_params) as response:
            if response.status != 200:
                raise GisNetworkError(response.state, response)
            return await response.json()

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