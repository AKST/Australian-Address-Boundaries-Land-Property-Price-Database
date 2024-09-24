import asyncio
from datetime import datetime
from functools import reduce
from logging import getLogger
from pathlib import Path
from pprint import pprint
import os

from lib.nsw_vg.property_sales import SaleDataFileSummary, get_data_from_targets
from lib.service.io import IoService
from .fetch_static_files import Environment

class State:
    _logger = getLogger(f'{__name__}.State')
    this_year = -1
    last_year = -1
    records = 0

    def increment(self, n: int):
        self.records += n

    def acknowledge_year(self, this_year: int):
        self.this_year, self.last_year = this_year, self.this_year

    def log_status(self):
        if self.this_year != self.last_year and self.last_year != -1:
            self._logger.info(f'Parsed {self.last_year}, parsed {self.records} records so far')

async def ingest(environment: Environment, io: IoService) -> None:
    try:
        item = None
        state = State()

        for d in [environment.sale_price_annual, environment.sale_price_weekly]:
            for item in get_data_from_targets('./_out_zip', d.links):
                if hasattr(item, 'total_records'):
                    state.acknowledge_year(item.parent.year)
                    state.increment(item.total_records)
                    state.log_status()
        state.log_status()
    except Exception as e:
        print(item)
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
        await ingest(environment, io_service)

    asyncio.run(main())

