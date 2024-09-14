from aiohttp import ClientSession
from asyncio import Semaphore, as_completed, TaskGroup
from collections import namedtuple
from dataclasses import dataclass
import geopandas as gpd
import json
from shapely.geometry import shape
from typing import Set, Any, Optional, List, Tuple

from lib.gis.bounds import BoundsIterator
from lib.gis.schema import GisSchema

@dataclass
class GisProjection:
    schema: GisSchema
    fields: str | List[str | Tuple[str, str]]
    epsg_crs: Any

    def get_fields(self):
        if isinstance(self.fields, str) and self.fields == '*':
            return (f.name for f in self.schema.fields)
        
        elif isinstance(self.fields, str):
            # if you set `fields` to a str, you should only set it to '*'
            raise ValueError('invalid projection')
        
        requirements = [((r, 3) if isinstance(r, str) else r) for r in self.fields]
        
        return (
            f.name
            for category, priority in requirements
            for f in self.schema.fields
            if f.category == category and f.priority <= priority
        )
            

class GisReader:
    def __init__(self, 
                 session: ClientSession,
                 semaphore: Semaphore,
                 where_clause: List[str]):
        self._session = session 
        self._semaphore = semaphore
        self._where_clause = where_clause

    @staticmethod
    def create(session: ClientSession,
               max_concurrent: int,
               where_clause: List[str]):
        return GisReader(session, Semaphore(max_concurrent), where_clause)

    async def get_pages(self, bounds_iter: BoundsIterator, projection: GisProjection):
        factory = _GisDfComponentFactory(projection.schema.id_field)

        async def with_bounds(bounds):
            async with self._semaphore:
                offset = 0
                page_size = 100
                
                geometries, attributes = [], []
                while True:
                    if self._session.closed:
                        return
                        
                    data = await self._fetch_data(f'{projection.schema.url}/query', {
                        'where': ' AND '.join([
                            "enddate >= CURRENT_DATE",
                            *self._where_clause,
                        ]),
                        'geometry': json.dumps(bounds),
                        'geometryType': 'esriGeometryEnvelope',
                        'inSR': bounds_iter.epsg_crs,
                        'outSR': projection.epsg_crs,
                        'outFields': ','.join(projection.get_fields()),
                        'spatialRel': 'esriSpatialRelIntersects',
                        'resultOffset': offset,
                        'resultRecordCount': page_size,
                        'f': 'json',
                    })
        
                    features = data.get('features', [])
                    if features:
                        offset += len(features)
                    else:
                        break
                        
                    for f in features:
                        component = factory.from_feature(f)
                        if component:
                            geometries.append(component.geometry)
                            attributes.append(component.attributes)
                            
                if attributes:
                    return gpd.GeoDataFrame(
                        attributes,
                        geometry=geometries,
                        crs=f"EPSG:{projection.epsg_crs}",
                    )

        async with TaskGroup() as tg:
            tasks = [tg.create_task(with_bounds(chunk)) for chunk in bounds_iter.chunks()]
            for task in as_completed(tasks):
                result = await task
                if result is not None:
                    yield result
            

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


class GisReaderError(Exception):
    bounds: Any
    offset: Any
    projection: GisProjection
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class GisNetworkError(GisReaderError):
    def __init__(self, http_status, response, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_status = http_status
        self.response = response    