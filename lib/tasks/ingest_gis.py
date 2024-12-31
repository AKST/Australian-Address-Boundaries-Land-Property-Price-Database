from datetime import datetime
from dataclasses import dataclass
from functools import reduce
import geopandas as gpd
from logging import getLogger
import math
import pandas as pd
import time
from typing import List, Optional

from lib.pipeline.gis import (
    FeaturePaginationSharderFactory,
    FeatureServerClient,
    GisIngestion,
    GisIngestionConfig,
    GisPipeline,
    GisPipelineTelemetry,
    DateRangeParam,
    BACKOFF_CONFIG,
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
from lib.service.http import CachedClientSession, ThrottledClientSession, ExpBackoffClientSession
from lib.service.http.middleware.cache import FileCacher
from lib.service.http.middleware.exp_backoff import BackoffConfig, RetryPreference
from lib.tooling.schema import SchemaController, SchemaDiscovery, Command

@dataclass
class IngestGisRunConfig:
    db_workers: int
    skip_save: bool
    gis_params: List[DateRangeParam]

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

async def run(
    io: IoService,
    db: DatabaseService,
    clock: ClockService,
    conf: IngestGisRunConfig,
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

    async with CachedClientSession.create(
        session=ExpBackoffClientSession.create(
            session=ThrottledClientSession.create(HOST_SEMAPHORE_CONFIG),
            config=BACKOFF_CONFIG,
        ),
        file_cache=FileCacher.create(io),
        io_service=io,
    ) as session:
        feature_client = FeatureServerClient(session)
        telemetry = GisPipelineTelemetry.create(clock)
        ingestion = GisIngestion.create(
            GisIngestionConfig(
                api_workers=16,
                api_worker_backpressure=conf.db_workers ** 2,
                db_workers=conf.db_workers,
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
    db_conns: int,
    skip_save: bool,
    gis_range: Optional[str],
) -> None:
    io = IoService.create(open_file_limit)
    db = DatabaseService.create(db_config, db_conns)
    clock = ClockService()
    controller = SchemaController(io, db, SchemaDiscovery.create(io))
    await controller.command(Command.Drop(ns='nsw_spatial'))
    await controller.command(Command.Create(ns='nsw_spatial'))

    match _parse_date_range(args.gis_range, ClockService()):
        case None:
            params = []
        case other:
            params = [other]
    await run(io, db, clock, IngestGisRunConfig(db_conns, skip_save, params))

if __name__ == '__main__':
    import asyncio
    import argparse
    import logging
    import resource

    from lib.service.database.defaults import DB_INSTANCE_MAP

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--gis-range", type=str)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--skip-save", action='store_true', required=False)
    parser.add_argument("--db-connections", type=int, default=16)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    slim, hlim = resource.getrlimit(resource.RLIMIT_NOFILE)
    http_limits = reduce(lambda acc, it: acc + it.limit, HOST_SEMAPHORE_CONFIG, 0)
    file_limit = int(slim * 0.8) - (args.db_connections + http_limits)
    print(file_limit)

    db_config = DB_INSTANCE_MAP[args.instance]

    try:
        asyncio.run(
            run_in_console(
                open_file_limit=file_limit,
                db_config=db_config,
                db_conns=args.db_connections,
                skip_save=args.skip_save,
                gis_range=args.gis_range,
            ),
        )
    except ExceptionGroup as e:
        import psutil

        process = psutil.Process()

        print(f'io open files {process.num_fds()}, limits {(slim, hlim)}')
        raise e
