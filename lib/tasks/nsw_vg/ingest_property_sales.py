import asyncio
from datetime import datetime
from dataclasses import dataclass
from functools import reduce
from logging import getLogger
from time import time
from typing import Self, Optional, Tuple
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

async def ingest_property_sales_rows(*args, **kwargs):
    await Main.main(*args, **kwargs)

@dataclass
class PropertySaleIngestionDbConfig:
    pool_size: int
    batch_size: int
    config: DatabaseConfig
    update: UpdateSchemaConfig

@dataclass
class PropertySaleIngestionConfig:
    file_limit: int
    db: Optional[PropertySaleIngestionDbConfig]

class Main:
    logger = getLogger(f'{__name__}.ingest')
    total: int
    start: float
    producer: PropertySaleProducer
    ingestion: Optional[PropertySalesIngestion]

    def __init__(self, producer, ingestion) -> None:
        self.total = 0
        self.start = time()
        self.producer = producer
        self.ingestion = ingestion

    async def run(self, e: Environment) -> None:
        try:
            async for task, item in self.producer.get_rows([
                *e.sale_price_annual.links,
                *e.sale_price_weekly.links,
            ]):
                if self.ingestion:
                    await self.ingestion.queue(item)

                if isinstance(item, SaleDataFileSummary):
                    self._count(task, item)

            if self.ingestion:
                await self.ingestion.flush()
        except Exception as e:
            if self.ingestion:
                self.ingestion.abort()
            raise e

    def _count(self, task, item: SaleDataFileSummary):
        self.total += item.total_records
        t = int(time() - self.start)
        th, tm, ts = t // 3600, t // 60 % 60, t % 60
        self.logger.info(f'Parsed {item.total_records} ' \
                         f'(total {self.total}) ' \
                         f'({th}h {tm}m {ts}s) ' \
                         f'(published: {task.published_year} ' \
                         f'downloaded: {task.download_date})')

    @staticmethod
    async def main(
            e: Environment,
            io: IoService,
            db_conf: Optional[Tuple[DatabaseService, int]]):
        if db_conf:
            db, batch_size = db_conf
            ingestion = PropertySalesIngestion.create(db, CONFIG, batch_size)
        else:
            ingestion = None
        producer = PropertySaleProducer.create(ZIP_DIR, io)

        task = Main(producer, ingestion)
        await task.run(e)

    @staticmethod
    async def cli_main(c: PropertySaleIngestionConfig):
        try:
            db: Optional[DatabaseService] = None
            fl = c.file_limit - c.db.pool_size if c.db else c.file_limit
            io = IoService.create(fl)

            async with get_session(io) as session:
                environment = await initialise(io, session)

            if c.db:
                db = DatabaseService.create(c.db.config, c.db.pool_size)
                await db.open()
                await update_schema(c.db.update, db, io)

            await Main.main(
                environment,
                io,
                (db, c.db.batch_size) if c.db and db else None,
            )
        finally:
            if db:
                await db.close()

if __name__ == '__main__':
    import argparse
    import resource
    import logging

    from lib.service.database.defaults import instance_1_config, instance_2_config

    parser = argparse.ArgumentParser(description="Ingest NSW Land values")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--file-limit", type=int)

    cmd_parser = parser.add_subparsers(dest='command')

    count_parser = cmd_parser.add_parser('count', help='counts rows')

    ingest_parser = cmd_parser.add_parser('ingest', help='ingests property sales data')
    ingest_parser.add_argument("--instance", type=int, required=True)
    ingest_parser.add_argument("--batch_size", type=int, default=50)
    ingest_parser.add_argument("--db_pool_size", type=int, default=24)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    if args.file_limit:
        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        soft_limit_2 = min(args.file_limit, hard_limit)
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft_limit_2, hard_limit))
        file_limit = int(soft_limit_2 * 0.8)
        logging.debug('updated the soft file (%s) limit to (%s)' % (
            soft_limit,
            soft_limit_2,
        ))
    else:
        file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        file_limit = int(file_limit * 0.8)

    logging.debug(args)

    db_conf_map = { 1: instance_1_config, 2: instance_2_config }

    match args.command:
        case 'count':
            db = None
        case 'ingest':
            db = PropertySaleIngestionDbConfig(
                batch_size=args.batch_size,
                pool_size=args.db_pool_size,
                config=db_conf_map[args.instance],
                update=UpdateSchemaConfig(
                    packages=['nsw_vg'],
                    apply=True,
                    revert=True,
                ),
            )
        case other:
            parser.print_help()

    config = PropertySaleIngestionConfig(file_limit, db)
    asyncio.run(Main.cli_main(config))

