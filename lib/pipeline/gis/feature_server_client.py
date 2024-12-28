from logging import getLogger
from typing import Any, Dict, Optional, Self, List

from lib.service.http import AbstractClientSession, CacheHeader
from lib.service.http.util import url_with_params

from .config import GisProjection, FeaturePageDescription

class FeatureServerClient:
    """
    Client for GIS Feature Server
    """
    _logger = getLogger(f'{__name__}.GisApiClient')

    def __init__(self: Self, session: AbstractClientSession):
        self._session = session

    async def get_page(
            self: Self,
            projection: GisProjection,
            feature_page: FeaturePageDescription) -> List[Any]:
        try:
            params = {
                'returnGeometry': True,
                'resultOffset': feature_page.offset,
                'resultRecordCount': projection.schema.result_limit,
                'where': feature_page.where_clause,
                'geometryType': 'esriGeometryEnvelope',
                'outSR': projection.epsg_crs,
                'outFields': ','.join(f.name for f in projection.get_fields()),
                'f': 'json',
            }
            data = await self.get_json(
                projection.schema.url,
                params=params,
                use_cache=feature_page.use_cache,
                cache_name='page')
        except GisNetworkError as e:
            self._logger.error(f'failed on schema {projection.schema.url}')
            self._logger.error(f'failed on task {feature_page}')
            raise GisTaskNetworkError(feature_page, e.http_status, e.response)

        features = data.get('features', [])
        if len(features) < feature_page.expected_results:
            self._logger.error("Potenial data loss has occured")
            raise MissingResultsError(
                f'{feature_page.where_clause} OFFSET {feature_page.offset}, '
                f'got {len(features)} wanted {feature_page.expected_results}')

        return features

    async def get_where_count(self: Self,
                              projection: GisProjection,
                              where_clause: Optional[str],
                              use_cache: bool) -> int:
        url = projection.schema.url
        params = { 'where': where_clause or '1=1', 'returnCountOnly': True, 'f': 'json' }
        response = await self.get_json(url, params, use_cache=use_cache, cache_name='count')
        count = response.get('count', 0)
        self._logger.debug(f'count for "{where_clause}" is {count}')
        return count

    async def get_json(self: Self,
                       feature_url: str,
                       params: Dict[str, Any],
                       use_cache: bool,
                       cache_name=None):
        url = url_with_params(f'{feature_url}/query', params)
        try:
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
        except Exception as e:
            self._logger.error(f'failed on {url}')
            self._logger.exception(e)
            raise e

class GisNetworkError(Exception):
    def __init__(self: Self, http_status, response, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_status = http_status
        self.response = response

class GisTaskNetworkError(GisNetworkError):
    def __init__(self: Self, task: FeaturePageDescription, http_status, response, *args, **kwargs):
        super().__init__(http_status, response, *args, **kwargs)
        self.task = task

class MissingResultsError(Exception):
    pass
