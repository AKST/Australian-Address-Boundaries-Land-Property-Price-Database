from datetime import datetime
from dataclasses import dataclass
from functools import reduce
import geopandas as gpd
from logging import getLogger
import math
import pandas as pd
import time
from typing import Any, List, Optional

from lib.pipeline.gis import (
    BACKOFF_CONFIG,
    FeaturePaginationSharderFactory,
    FeatureServerClient,
    FeatureExpBackoff,
    GisIngestion,
    GisIngestionConfig,
    GisPipeline,
    GisPipelineTelemetry,
    DateRangeParam,
    HOST_SEMAPHORE_CONFIG,
    ENSW_DA_PROJECTION,
    SNSW_LOT_PROJECTION,
    SNSW_PROP_PROJECTION,
    ENSW_ZONE_PROJECTION,
    YearMonth,
)
from lib.service.database import DatabaseService, DatabaseConfig
from lib.service.io import IoService
from lib.service.clock import ClockService
from lib.service.http import (
    CachedClientSession,
    ExpBackoffClientSession,
    HostSemaphoreConfig,
    HttpLocalCache,
    ThrottledClientSession,
)
from lib.service.http.middleware.exp_backoff import BackoffConfig, RetryPreference
from lib.tooling.schema import SchemaController, SchemaDiscovery, Command

from .config import GisTaskConfig

def http_limits_of(ss: List[HostSemaphoreConfig]) -> int:
    return reduce(lambda acc, it: acc + it.limit, ss, 0)

def _parse_date_range(s: Optional[str], clock: ClockService) -> Optional[DateRangeParam]:
    match s:
        case None:
            return None
        case '':
            return None
        case non_null_str:
            pass

    match [datetime.strptime(s, '%Y%m') for s in non_null_str.split(':')]:
        case [datetime(year=y1), datetime(year=y2)] if y1 > y2:
            raise ValueError('first year greater than second')
        case [datetime(month=m1), datetime(month=m2)] if m1 > m2:
            raise ValueError('first month greater than second')
        case [datetime(year=y1, month=m1), datetime(year=y2, month=m2)]:
            return DateRangeParam(YearMonth(y1, m1), YearMonth(y2, m2), clock)
        case other:
            raise ValueError('unknown date format')

async def stage_gis_api_data(
    io: IoService,
    db: DatabaseService,
    clock: ClockService,
    conf: GisTaskConfig.StageApiData,
) -> None:
    """
    The http client composes a cache layer and a throttling
    layer. Some GIS serves, such as the ones below may end
    up blocking you if you perform to much traffic at once.

      - Caching ensure requests that do not need to be
        repeated, are not repeated.

      - Expotenial backoff is here to retry when in the
        the event any requests to the GIS server fail.

      - Throttling of course ensure we do not have more
        active requests to one host at a time. The max
        active requests is set on a host basis.
    """

    http_file_cache = HttpLocalCache.create(io, 'gis')
    async with CachedClientSession.create(
        session=ExpBackoffClientSession.create(
            session=ThrottledClientSession.create(HOST_SEMAPHORE_CONFIG),
            config=BACKOFF_CONFIG,
        ),
        file_cache=http_file_cache,
        io_service=io,
    ) as session:
        feature_client = FeatureServerClient(
            FeatureExpBackoff(cfg.exp_backoff_attempts),
            clock, session, http_file_cache)
        telemetry = GisPipelineTelemetry.create(clock)

        if conf.dry_run:
            api_workers, db_workers = 1, 1
        else:
            api_workers = http_limits_of(HOST_SEMAPHORE_CONFIG)
            db_workers = conf.db_workers

        ingestion = GisIngestion.create(
            GisIngestionConfig(
                api_workers=api_workers,
                api_worker_backpressure=db_workers * 4,
                dry_run=conf.dry_run,
                db_workers=db_workers,
                chunk_size=None),
            feature_client,
            db,
            telemetry)
        sharder_factory = FeaturePaginationSharderFactory(feature_client, telemetry)
        pipeline = GisPipeline(sharder_factory, ingestion)

        await pipeline.start([
            (SNSW_PROP_PROJECTION, conf.gis_params),
            (SNSW_LOT_PROJECTION, conf.gis_params),
        ])

async def run_in_console(
    open_file_limit: int,
    db_config: DatabaseConfig,
    config: GisTaskConfig.StageApiData,
) -> None:
    io = IoService.create(open_file_limit)
    db = DatabaseService.create(db_config, config.db_workers)
    clock = ClockService()
    controller = SchemaController(io, db, SchemaDiscovery.create(io))
    if not config.dry_run:
        await controller.command(Command.Drop(ns='nsw_spatial'))
        await controller.command(Command.Create(ns='nsw_spatial'))
    await stage_gis_api_data(io, db, clock, config)

if __name__ == '__main__':
    import asyncio
    import argparse
    import logging
    import resource

    from lib.defaults import INSTANCE_CFG
    from lib.utility.logging import config_vendor_logging, config_logging

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--dry-run", action='store_true', required=False)
    parser.add_argument("--gis-range", type=str)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--skip-save", action='store_true', required=False)
    parser.add_argument("--db-connections", type=int, default=32)
    parser.add_argument("--exp-backoff-attempts", type=int, default=8)

    args = parser.parse_args()

    config_vendor_logging({'sqlglot', 'psycopg.pool'}, {'asyncio'})
    config_logging(worker=None, debug=args.debug)

    slim, hlim = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(slim * 0.8) - (args.db_connections + http_limits_of(HOST_SEMAPHORE_CONFIG))
    if file_limit < 1:
        raise ValueError(f"file limit of {file_limit} is just too small")


    instance_cfg = INSTANCE_CFG[args.instance]

    try:
        match _parse_date_range(args.gis_range, ClockService()):
            case other if other is not None:
                params = [other]
            case None:
                params = []
        cfg = GisTaskConfig.StageApiData(
            db_workers=args.db_connections,
            skip_save=args.skip_save,
            dry_run=args.dry_run,
            gis_params=params,
            exp_backoff_attempts=args.exp_backoff_attempts,
        )
        asyncio.run(
            run_in_console(
                open_file_limit=file_limit,
                db_config=instance_cfg.database,
                config=cfg,
            ),
        )
    except ExceptionGroup as eg:
        def is_cancelled_error(exc: BaseException) -> bool:
            return isinstance(exc, asyncio.CancelledError)

        cancelled_errors, remaining_eg = eg.split(is_cancelled_error)

        if remaining_eg:
            for e in remaining_eg.exceptions:
                logging.exception(e)
            raise remaining_eg
        else:
            raise eg
