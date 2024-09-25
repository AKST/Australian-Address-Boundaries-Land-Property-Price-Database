import asyncio
from datetime import datetime
from functools import reduce
from logging import getLogger
from pathlib import Path
from pprint import pprint
import os

from lib.nsw_vg.property_sales import PropertySaleProducer
from lib.service.io import IoService
from .fetch_static_files import Environment

ZIP_DIR = './_out_zip'
LIMIT = 256 * 4

class State:
    _logger = getLogger(f'{__name__}.State')
    records = 0

    def acknowledge(self, year: int, amount: int):
        self.records += amount
        self._logger.info(f'Parsed {amount} for {year} (total {self.records})')

async def ingest(environment: Environment,
                 io: IoService,
                 limit: int) -> None:
    producer = PropertySaleProducer.create(ZIP_DIR, io, limit)

    try:
        task, item = None, None
        state = State()

        for d in [environment.sale_price_annual, environment.sale_price_weekly]:
            async for task, item in producer.get_rows(d.links):
                if hasattr(item, 'total_records'):
                    state.acknowledge(item.parent.year, item.total_records)
    except Exception as e:
        print(task, item)
        raise e

if __name__ == '__main__':
    import resource
    import logging

    from .fetch_static_files import get_session, initialise

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    async def main() -> None:
        io_service = IoService.create(file_limit)
        async with get_session(io_service) as session:
            environment = await initialise(io_service, session)
        await ingest(environment, io_service, file_limit)

    asyncio.run(main())

