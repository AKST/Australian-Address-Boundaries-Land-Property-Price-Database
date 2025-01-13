import asyncio
from dataclasses import dataclass
from logging import getLogger
from pprint import pformat
from typing import Any, Dict, Optional, Self, Set, List
from urllib.parse import urlencode

from lib.service.clock import ClockService
from lib.service.http import (
    AbstractClientSession,
    HttpLocalCache,
    CacheHeader,
    url_with_params,
)

from .config import GisProjection, FeaturePageDescription

@dataclass(frozen=True)
class FeatureExpBackoff:
    allowed_attempts: int

UrlParams = Dict[str, str | bool | int]

def get_page_clauses(schema_url: str, params: UrlParams, keys: Set[str]) -> List[str]:
    return [schema_url, *[
        urlencode({key: value})
        for key, value in params.items()
        if key in keys
    ]]

def get_page_url(schema_url: str, params: UrlParams) -> str:
    return url_with_params(f'{schema_url}/query', params)

def get_page_url_params(
        offset: int,
        projection: GisProjection,
        feature_page: FeaturePageDescription) -> UrlParams:
    return {
        'returnGeometry': True,
        'resultOffset': offset,
        'resultRecordCount': projection.schema.result_limit,
        'where': feature_page.where_clause,
        'geometryType': 'esriGeometryEnvelope',
        'outSR': projection.epsg_crs,
        'outFields': ','.join(f.name for f in projection.get_fields()),
        'f': 'json',
    }

def get_count_url_params(where_clause: Optional[str]) -> UrlParams:
    return { 'where': where_clause or '1=1', 'returnCountOnly': True, 'f': 'json' }

class FeatureServerClient:
    """
    Client for GIS Feature Server
    """
    _logger = getLogger(f'{__name__}.GisApiClient')

    def __init__(self: Self,
                 exp_backoff_cfg: FeatureExpBackoff,
                 clock: ClockService,
                 session: AbstractClientSession,
                 file_cache: HttpLocalCache):
        self.exp_backoff_cfg = exp_backoff_cfg
        self._clock = clock
        self._session = session
        self._http_file_cache = file_cache

    async def forget_page_cache(self: Self,
        projection: GisProjection,
        feature_page: FeaturePageDescription,
        forget_total_shard: bool = True,
    ) -> None:
        try:
            page_url_clauses = get_page_clauses(
                projection.schema.url,
                get_page_url_params(feature_page.offset, projection, feature_page),
                {'where'} if forget_total_shard else {'where', 'resultOffset'},
            )
            try:
                await self._http_file_cache.forget_by_clause(page_url_clauses)
            except:
                self._logger.error(page_url_clauses)
                raise
        except:
            self._logger.error(f"failed to clear cache for\n" \
                               f"{pformat(projection)}\n{pformat(feature_page)}")
            raise


    async def get_page(
            self: Self,
            projection: GisProjection,
            feature_page: FeaturePageDescription) -> List[Any]:
        try:
            url_params = get_page_url_params(
                feature_page.offset,
                projection,
                feature_page)

            allowed_attempts = self.exp_backoff_cfg.allowed_attempts
            while allowed_attempts > 0:
                data = await self.get_json(
                    projection.schema.url,
                    params=url_params,
                    use_cache=feature_page.use_cache,
                    cache_name='page')

                features = data.get('features', [])
                if len(features) < feature_page.expected_results:
                    attempt = self.exp_backoff_cfg.allowed_attempts - allowed_attempts
                    allowed_attempts -= 1

                    await self.forget_page_cache(projection, feature_page)
                    await self._clock.sleep(1 * (2 ** attempt))
                else:
                    break
        except GisNetworkError as e:
            self._logger.error(f'failed on schema {projection.schema.url}')
            self._logger.error(f'failed on task {feature_page}')
            raise GisTaskNetworkError(feature_page, e.http_status, e.response)

        if len(features) < feature_page.expected_results:
            self._logger.error(f"Potenial data loss has occured, response:\n{pformat(data)}")
            await self.forget_page_cache(projection, feature_page, forget_total_shard=True)
            raise MissingResultsError(
                f'{feature_page.where_clause} OFFSET {feature_page.offset}, '
                f'got {len(features)} wanted {feature_page.expected_results}')

        return features

    async def get_where_count(self: Self,
                              projection: GisProjection,
                              where_clause: Optional[str],
                              use_cache: bool) -> int:
        url = projection.schema.url
        params = get_count_url_params(where_clause)
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
        except asyncio.CancelledError:
            raise
        except:
            self._logger.error(f'failed on {url}')
            raise

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
