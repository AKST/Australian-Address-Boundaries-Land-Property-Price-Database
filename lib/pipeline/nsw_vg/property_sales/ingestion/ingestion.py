import asyncio
from asyncio import TaskGroup
from logging import getLogger
from typing import Any, Dict, List, Self, Set, Tuple, Type

from lib.service.database import DatabaseService
from lib.pipeline.nsw_vg.property_sales import data as t

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
    _tg: TaskGroup

    def __init__(self: Self,
                 db: DatabaseService,
                 tg: TaskGroup,
                 config: IngestionConfig,
                 batch_size: int,
                 state: RowBatchState) -> None:
        self.batch_size = batch_size
        self._db = db
        self._tg = tg
        self._state = state
        self._tasks = set()
        self._config = config

    @staticmethod
    def create(db: DatabaseService,
               tg: TaskGroup,
               config: IngestionConfig,
               batch_size: int) -> 'PropertySalesIngestion':
        return PropertySalesIngestion(db, tg, config, batch_size, {
            t.SaleRecordFileLegacy: [],
            t.SalePropertyDetails1990: [],
            t.SaleRecordFile: [],
            t.SalePropertyDetails: [],
            t.SalePropertyLegalDescription: [],
            t.SaleParticipant: [],
        })

    def abort(self: Self) -> None:
        for t in self._tasks:
            t.cancel()

        completed = {t for t in self._tasks if t.done()}
        self._task = self._tasks - completed

    async def flush(self: Self) -> int:
        size = 0
        for t, queued in self._state.items():
            size += len(queued)
            if queued:
                table_conf = self._config.get_config(queued[0])
                self._dispatch(table_conf, queued)


        self._state = {t: [] for t in self._state.keys()}
        await asyncio.gather(*self._tasks)
        await self._maintain_running()
        return size

    async def queue(self: Self, row: t.BasePropertySaleFileRow) -> int:
        await self._maintain_running()

        if isinstance(row, t.SaleDataFileSummary):
            return 0

        if type(row) not in self._state:
            self._logger.error(f'key: {type(row)}')
            self._logger.error(f'state: {list(self._state.keys())}')
            raise ValueError('unexpected row type')

        queued = self._state[type(row)]
        queued.append(row)

        if len(queued) < self.batch_size:
            return 0

        table_conf = self._config.get_config(row)

        self._dispatch(table_conf, queued)
        self._state[type(row)] = []
        return self.batch_size

    def _dispatch(self: Self, c: IngestionTableConfig, q: List[t.BasePropertySaleFileRow]) -> None:
        sql, columns = insert_queue(c.table_symbol, q[0])
        values = [[getattr(row, name) for name in columns] for row in q]
        task = self._tg.create_task(self._worker(sql, values, c.table_symbol))
        self._tasks.add(task)

    async def _maintain_running(self: Self) -> None:
        completed = {t for t in self._tasks if t.done()}
        self._task = self._tasks - completed
        for t in completed:
            await t

    async def _worker(self: Self, sql: str, rows: List[List[str]], name: str):
        try:
            async with self._db.async_connect() as c, c.cursor() as cursor:
                match len(rows):
                    case 0: pass
                    case 1: await cursor.execute(sql, rows[0])
                    case n: await cursor.executemany(sql, rows)
            self._logger.debug(f'inserted {len(rows)} for {name}')
        except Exception as e:
            self._logger.error(f'failed on "{sql}"')
            if len(rows) > 1:
                # `executemany` has terribel error messages so lets
                # narrow in on whatever caused the issue
                for row in rows:
                    await self._worker(sql, [row], name)
            else:
                self._logger.error(rows)
                self._logger.exception(e)
                raise e

