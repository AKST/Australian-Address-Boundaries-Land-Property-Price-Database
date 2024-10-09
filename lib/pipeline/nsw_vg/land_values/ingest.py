import asyncio
import csv
from datetime import datetime
from io import StringIO
from logging import getLogger
from typing import List, Self

from lib.pipeline.nsw_vg.discovery import NswVgTarget
from lib.service.database import DatabaseService
from lib.service.io import IoService

from .data import RawLandValueRow

class NswVgLandValuesRawIngestion:
    root_dir: str
    chunk_size: int
    _logger = getLogger(f'{__name__}.NswVgLandValuesRawIngestion')
    _semaphore: asyncio.Semaphore
    _db: DatabaseService
    _io: IoService
    _tg: asyncio.TaskGroup
    _parse_tasks: List[asyncio.Task]
    _database_tasks: List[asyncio.Task]

    _parsed = 0
    _ingested = 0


    def __init__(self: Self,
                 semaphore,
                 root_dir,
                 chunk_size,
                 io,
                 db,
                 tg) -> None:
        self.root_dir = root_dir
        self.chunk_size = chunk_size
        self._semaphore = semaphore
        self._database_tasks = []
        self._parse_tasks = []
        self._io = io
        self._db = db
        self._tg = tg

    async def ingest_raw_target(self: Self, target: NswVgTarget) -> None:
        fpath = f'{self.root_dir}/{target.zip_dst}'
        files = [
            f'{self.root_dir}/{target.zip_dst}/{f}'
            for f in sorted(await self._io.ls_dir(fpath))
        ]
        self._parse_tasks = [
            self._tg.create_task(self.ingest_raw_file(f, target.datetime))
            for f in files
        ]
        try:
            await asyncio.gather(*self._parse_tasks)
            await asyncio.gather(*self._database_tasks)
        except Exception as e:
            raise e

    async def ingest_raw_file(self: Self,
                              file_path: str,
                              source_date: datetime) -> None:
        if not file_path.endswith("csv"):
            return

        file_data = await self._tg.create_task(self.__get_data(file_path))
        quasi_file = StringIO(file_data)

        queue = []
        for index, row_raw in enumerate(csv.DictReader(quasi_file)):
            self._parsed += 1

            row = RawLandValueRow.from_row(
                row_raw,
                index + 1,
                file_path,
                source_date,
            )

            queue.append(row)

            if len(queue) >= self.chunk_size:
                task = self._tg.create_task(self.__ingest(file_path, queue))
                self._database_tasks.append(task)
                await asyncio.sleep(0.001)
                queue = []

        if queue:
            task = self._tg.create_task(self.__ingest(file_path, queue))
            self._database_tasks.append(task)

        self._logger.info(f'Parsed {file_path}')

    async def __ingest(self: Self, f: str, rows: List[RawLandValueRow]) -> None:
        columns = rows[0].db_columns()
        column_str = ', '.join(columns)
        values_str = ', '.join(['%s'] * len(columns))
        values = [[getattr(row, name) for name in columns] for row in rows]

        try:
            async with (
                self._semaphore,
                self._db.async_connect(timeout=None) as c,
                c.cursor() as cursor
            ):
                await cursor.executemany(f"""
                    INSERT INTO nsw_vg_raw.land_value_row ({column_str})
                    VALUES ({values_str})
                """, values)
                self._ingested += len(values)

                progress = self._ingested / self._parsed
                self._logger.info(f'written {progress:.2%} ({self._ingested} / {self._parsed})')
        except Exception as e:
            self._logger.exception(e)
            self._logger.error(f'failed to ingest {f}')
            await self.__kill()
            raise e

    async def __get_data(self: Self, f: str) -> str:
        try:
            return await self._io.f_read(f, encoding='utf-8')
        except UnicodeDecodeError:
            return await self._io.f_read(f, encoding='ISO-8859-1')

    async def __kill(self: Self) -> None:
        for t in self._parse_tasks:
            t.cancel()
        for t in self._database_tasks:
            t.cancel()


