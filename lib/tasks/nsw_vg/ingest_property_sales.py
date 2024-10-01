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
LIMIT = 256 * 4

class State:
    _logger = getLogger(f'{__name__}.State')
    _start: float
    records = 0

    def __init__(self):
        self._start = time()

    def elapsed(self):
        t = int(time() - self._start)
        th, tm, ts = t // 3600, t // 60 % 60, t % 60
        return f'{th}h {tm}m {ts}s'

    def acknowledge_summary(self, row: SaleDataFileSummary, task: PropertySaleDatFileMetaData):
        self.records += row.total_records
        message = f'Parsed {row.total_records} ' \
                  f'(total {self.records}) ({self.elapsed()}) ' \
                  f'(published: {task.published_year} downloaded: {task.download_date})'
        self._logger.info(message)

    def acknowledge(self, row: BasePropertySaleFileRow):
        self.records += 1
        message = f'Parsed total {self.records} ' \
                  f'({self.elapsed()}) ' \
                  f'{repr(row)}'
        self._logger.info(message)

async def ingest_property_sales(env: Environment, io: IoService) -> None:
    logger = getLogger(f'{__name__}.ingest')

    producer = PropertySaleProducer.create(ZIP_DIR, io)

    try:
        task, item = None, None
        state = State()
        all_links = [*env.sale_price_annual.links, *env.sale_price_weekly.links]

        async for task, item in producer.get_rows(all_links):
            if isinstance(item, SaleDataFileSummary):
                state.acknowledge_summary(item, task)
    except Exception as e:
        raise e

if __name__ == '__main__':
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
        io_service = IoService.create(file_limit)
        async with get_session(io_service) as session:
            environment = await initialise(io_service, session)
        await ingest_property_sales(environment, io_service)

    asyncio.run(main())

