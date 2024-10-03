import asyncio
from logging import getLogger
from typing import Any, Dict, List, Self, Set, Tuple, Type

from lib.service.database import DatabaseService
from lib.pipeline.nsw_vg.property_sales.file_format import data as t

from .config import IngestionConfig, IngestionTableConfig

def insert_queue(name: str, row: t.BasePropertySaleFileRow) -> Tuple[str, List[Any]]:
    columns = row.db_columns()
    values_str = ', '.join(['%s'] * len(columns))
    column_str = ', '.join(columns)
    query = f"INSERT INTO {name} ({column_str}) VALUES ({values_str})"
    return query, columns

RowBatchState = Dict[Type[t.BasePropertySaleFileRow], List[t.BasePropertySaleFileRow]]

class PropertySalesIngestion:
    _logger = getLogger(f'{__name__}.PropertySalesIngestion')

    batch_size: int
    _config: IngestionConfig
    _tasks: Set[asyncio.Task]
    _state: RowBatchState
    _db: DatabaseService

    def __init__(self: Self,
                 db: DatabaseService,
                 config: IngestionConfig,
                 batch_size: int,
                 state: RowBatchState) -> None:
        self.batch_size = batch_size
        self._db = db
        self._state = state
        self._tasks = set()
        self._config = config

    @staticmethod
    def create(db: DatabaseService, config: IngestionConfig) -> 'PropertySalesIngestion':
        return PropertySalesIngestion(db, config, 50, {
            t.SaleRecordFileLegacy: [],
            t.SalePropertyDetails1990: [],
            t.SaleRecordFile: [],
            t.SalePropertyDetails: [],
            t.SalePropertyLegalDescription: [],
            t.SaleParticipant: [],
        })

    def abort(self: Self) -> None:
        self._maintain_runnning()
        for t in self._tasks:
            t.cancel()

    async def flush(self: Self) -> None:
        for t, queued in self._state.items():
            if queued:
                table_conf = self._config.get_config(queued[0])
                self._dispatch(table_conf, queued)

        self._state = {t: [] for t in self._state.keys()}
        await asyncio.gather(*self._tasks)
        self._maintain_runnning()

    def queue(self: Self, row: t.BasePropertySaleFileRow) -> None:
        self._maintain_runnning()

        if isinstance(row, t.SaleDataFileSummary):
            return

        if type(row) not in self._state:
            self._logger.error(f'key: {type(row)}')
            self._logger.error(f'state: {list(self._state.keys())}')
            raise ValueError('unexpected row type')

        queued = self._state[type(row)]
        queued.append(row)

        if len(queued) < self.batch_size:
            return

        table_conf = self._config.get_config(row)

        self._dispatch(table_conf, queued)
        self._state[type(row)] = []

    def _dispatch(self: Self, c: IngestionTableConfig, q: List[t.BasePropertySaleFileRow]) -> None:
        self._logger.debug(f'inserting {len(q)} for {c.table_symbol}')
        sql, columns = insert_queue(c.table_symbol, q[0])
        values = [[getattr(row, name) for name in columns] for row in q]
        task = asyncio.create_task(self._worker(sql, values, c.table_symbol))
        self._tasks.add(task)

    def _maintain_runnning(self) -> None:
        completed = {t for t in self._tasks if t.done()}
        self._task = self._tasks - completed

    async def _worker(self, sql: str, values: List[List[str]], name: str) -> None:
        async with await self._db.async_connect() as c, c.cursor() as cursor:
            await cursor.executemany(sql, values)
        self._logger.debug(f'inserted {len(values)} for {name}')

