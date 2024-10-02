import asyncio
from datetime import datetime
from functools import reduce
from logging import getLogger
from time import time
from pathlib import Path
from pprint import pprint
import os

from lib.nsw_vg.property_sales import PropertySaleProducer, PropertySaleDatFileMetaData
from lib.nsw_vg.property_sales import SaleDataFileSummary, BasePropertySaleFileRow
from lib.service.io import IoService
from lib.tasks.fetch_static_files import Environment

ZIP_DIR = './_out_zip'

async def count_property_sales_rows(e: Environment, io: IoService) -> None:
    logger = getLogger(f'{__name__}.ingest')
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

if __name__ == '__main__':
    import argparse
    import resource
    import logging

    from lib.tasks.fetch_static_files import get_session, initialise

    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    async def main() -> None:
        io = IoService.create(file_limit)
        async with get_session(io) as session:
            environment = await initialise(io, session)
        await count_property_sales_rows(environment, io)

    asyncio.run(main())

