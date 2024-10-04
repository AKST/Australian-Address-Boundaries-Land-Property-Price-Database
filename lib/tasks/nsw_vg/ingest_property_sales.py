import asyncio
from datetime import datetime
from functools import reduce
from logging import getLogger
from time import time
from typing import Self
from pathlib import Path
from pprint import pprint
import os

from lib.pipeline.nsw_vg.property_sales import PropertySaleProducer, PropertySaleDatFileMetaData
from lib.pipeline.nsw_vg.property_sales import SaleDataFileSummary, BasePropertySaleFileRow
from lib.pipeline.nsw_vg.property_sales.ingestion import CONFIG, PropertySalesIngestion
from lib.service.io import IoService
from lib.service.database import DatabaseService, DatabaseConfig

from ..fetch_static_files import Environment, get_session, initialise
from ..update_schema import update_schema, UpdateSchemaConfig

ZIP_DIR = './_out_zip'

class Counter:
    _logger = getLogger(f'{__name__}.count')

    def __init__(self: Self):
        self.start = time()
        self.total = 0

    def count(self: Self, task, item: SaleDataFileSummary):
        self.total = self.total + item.total_records
        t = int(time() - self.start)
        th, tm, ts = t // 3600, t // 60 % 60, t % 60
        self._logger.info(f'Parsed {item.total_records} ' \
                          f'(total {self.total}) ' \
                          f'({th}h {tm}m {ts}s) ' \
                          f'(published: {task.published_year} ' \
                          f'downloaded: {task.download_date})')

async def count_property_sales_rows(e: Environment, io: IoService) -> None:
    logger = getLogger(f'{__name__}.count')
    producer = PropertySaleProducer.create(ZIP_DIR, io)
    counter = Counter()

    try:
        all_links = [*e.sale_price_annual.links, *e.sale_price_weekly.links]
        async for task, item in producer.get_rows(all_links):
            if isinstance(item, SaleDataFileSummary):
                counter.count(task, item)
    except Exception as e:
        raise e

async def ingest_property_sales_rows(
    e: Environment,
    io: IoService,
    db: DatabaseService,
    batch_size: int
) -> None:
    logger = getLogger(f'{__name__}.ingest')
    producer = PropertySaleProducer.create(ZIP_DIR, io)
    ingestion = PropertySalesIngestion.create(db, CONFIG, batch_size)
    counter = Counter()

    try:
        all_links = [*e.sale_price_annual.links, *e.sale_price_weekly.links]
        async for task, item in producer.get_rows(all_links):
            await ingestion.queue(item)
            if isinstance(item, SaleDataFileSummary):
                counter.count(task, item)
        await ingestion.flush()
    except Exception as e:
        ingestion.abort()
        raise e


async def _count_main(file_limt: int) -> None:
    io = IoService.create(file_limit)
    async with get_session(io) as session:
        environment = await initialise(io, session)
    await count_property_sales_rows(environment, io)

async def _ingest_main(file_limt: int,
                       db_conf: DatabaseConfig,
                       batch_size: int,
                       update_conf: UpdateSchemaConfig) -> None:
    try:
        io = IoService.create(file_limit - 32)
        db = DatabaseService.create(db_conf, 32)
        await db.open()
        await update_schema(update_conf, db, io)

        async with get_session(io) as session:
            environment = await initialise(io, session)

        await ingest_property_sales_rows(environment, io, db, batch_size)
    finally:
        await db.close()


if __name__ == '__main__':
    import argparse
    import resource
    import logging

    from lib.service.database.defaults import instance_1_config, instance_2_config

    parser = argparse.ArgumentParser(description="Ingest NSW Land values")
    parser.add_argument("--debug", action='store_true', default=False)

    cmd_parser = parser.add_subparsers(dest='command')

    count_parser = cmd_parser.add_parser('count', help='counts rows')

    ingest_parser = cmd_parser.add_parser('ingest', help='ingests property sales data')
    ingest_parser.add_argument("--instance", type=int, required=True)
    ingest_parser.add_argument("--batch_size", type=int, default=250)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    logging.debug(args)

    db_conf_map = { 1: instance_1_config, 2: instance_2_config }

    match args.command:
        case 'count':
            asyncio.run(_count_main(file_limit))
        case 'ingest':
            db_connect_config = db_conf_map[args.instance]
            db_update_conf = UpdateSchemaConfig(
                packages=['nsw_vg'],
                apply=True,
                revert=True,
            )
            asyncio.run(_ingest_main(
                file_limit,
                db_connect_config,
                args.batch_size,
                db_update_conf,
            ))
        case other:
            parser.print_help()

