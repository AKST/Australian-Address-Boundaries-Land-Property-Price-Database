import asyncio
from datetime import datetime
from functools import reduce
from logging import getLogger
from time import time
from pathlib import Path
from pprint import pprint
import os

from lib.pipeline.nsw_vg.property_sales import PropertySaleProducer, PropertySaleDatFileMetaData
from lib.pipeline.nsw_vg.property_sales import SaleDataFileSummary, BasePropertySaleFileRow
from lib.pipeline.nsw_vg.property_sales.ingestion import CONFIG, PropertySalesIngestion
from lib.service.io import IoService
from lib.service.database import DatabaseService

from ..fetch_static_files import Environment, get_session, initialise
from ..update_schema import update_schema

ZIP_DIR = './_out_zip'

async def count_property_sales_rows(e: Environment, io: IoService) -> None:
    logger = getLogger(f'{__name__}.count')
    producer = PropertySaleProducer.create(ZIP_DIR, io)
    start = time()
    total = 0

    try:
        all_links = [*e.sale_price_annual.links, *e.sale_price_weekly.links]
        async for task, item in producer.get_rows(all_links):
            if isinstance(item, SaleDataFileSummary):
                total, t = total + item.total_records, int(time() - start)
                th, tm, ts = t // 3600, t // 60 % 60, t % 60
                logger.info(f'Parsed {item.total_records} ' \
                            f'(total {total}) ' \
                            f'({th}h {tm}m {ts}s) ' \
                            f'(published: {task.published_year} ' \
                            f'downloaded: {task.download_date})')
    except Exception as e:
        raise e

async def ingest_property_sales_rows(
    e: Environment,
    io: IoService,
    db: DatabaseService,
) -> None:
    logger = getLogger(f'{__name__}.ingest')
    producer = PropertySaleProducer.create(ZIP_DIR, io)
    ingestion = PropertySalesIngestion(db, CONFIG)
    start = time()
    total = 0

    try:
        all_links = [*e.sale_price_annual.links, *e.sale_price_weekly.links]
        async for task, item in producer.get_rows(all_links):
            await ingestion.queue_row(item)
    except Exception as e:
        raise e


async def _count_main(io) -> None:
    async with get_session(io) as session:
        environment = await initialise(io, session)
    await count_property_sales_rows(environment, io)

async def _ingest_main(io, db, update_conf) -> None:
    await update_schema(update_conf, db, io)
    async with get_session(io) as session:
        environment = await initialise(io, session)
    await ingest_property_sales_rows(environment, io, db)


if __name__ == '__main__':
    import argparse
    import resource
    import logging

    from lib.service.database.defaults import instance_1_config, instance_2_config
    from ..update_schema import UpdateSchemaConfig

    parser = argparse.ArgumentParser(description="Ingest NSW Land values")
    parser.add_argument("--debug", action='store_true', default=False)

    cmd_parser = parser.add_subparsers(dest='command')

    count_parser = cmd_parser.add_parser('count', help='counts rows')

    ingest_parser = cmd_parser.add_parser('ingest', help='ingests property sales data')
    ingest_parser.add_argument("--instance", type=int, required=True)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    logging.debug(args)

    db_conf_map = { 1: instance_1_config, 2: instance_2_config }
    io = IoService.create(file_limit)

    match args.command:
        case 'count':
            asyncio.run(_count_main(io))
        case 'ingest' if args.instance not in db_conf_map:
            raise ValueError('unknown db instance')
        case 'ingest':
            db_connect_config = db_conf_map[args.instance]
            db_update_conf = UpdateSchemaConfig(
                packages=['nsw_vg'],
                apply=True,
                revert=True,
            )

            db = DatabaseService(db_connect_config)
            asyncio.run(_ingest_main(io, db, db_update_conf))
        case other:
            parser.print_help()

