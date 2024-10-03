import asyncio
from logging import getLogger
from typing import Self

from lib.service.database import DatabaseService
from lib.pipeline.nsw_vg.property_sales.file_format import data as t

from .config import IngestionConfig

class PropertySalesIngestion:
    _logger = getLogger(f'{__name__}.PropertySalesIngestion')
    _config: IngestionConfig
    _db: DatabaseService

    def __init__(self: Self, db: DatabaseService, config: IngestionConfig) -> None:
        self._db = db
        self._config = config

    async def queue_row(self: Self, row: t.BasePropertySaleFileRow) -> None:
        table_conf = self._config.get_config(row)

        if table_conf is None:
            return

        self._logger.debug(f'inserting row for {table_conf.table_symbol}')


