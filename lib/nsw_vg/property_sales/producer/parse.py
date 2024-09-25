import asyncio
from datetime import datetime
from logging import getLogger
from typing import Any, Dict, Iterator, List, Optional, Tuple, Self
import os
import re

from lib.nsw_vg.property_sales import types as ps
from lib.service.io import IoService
from .task import PropertySaleIngestionTask

FactoryMap = Dict[str, Any]
Syntax = Dict[str, int]

SYNTAX_1990: Syntax = { 'A': 5, 'B': 21, 'Z': 3 }
SYNTAX_2002: Syntax = { 'A': 4, 'B': 24, 'C': 6, 'D': 11, 'Z': 5 }
SYNTAX_2012: Syntax = { 'A': 5, 'B': 24, 'C': 6, 'D': 11, 'Z': 5 }
SYNTAX_2021: Syntax = { 'A': 5, 'B': 24, 'C': 6, 'D': 12, 'Z': 5 }

class PropertySalesRowParserFactory:
    _io: IoService
    _semaphore: asyncio.Semaphore

    def __init__(self, io: IoService, semaphore: asyncio.Semaphore):
        self._io = io
        self._semaphore = semaphore

    def create_parser(self, task: PropertySaleIngestionTask) -> 'PropertySalesParser':
        return PropertySalesParser(task, self._io, self._semaphore)

class PropertySalesParser:
    task: PropertySaleIngestionTask

    _logger = getLogger(f'{__name__}.PropertySalesParser')
    _io: IoService
    _semaphore: asyncio.Semaphore

    def __init__(self,
                 task: PropertySaleIngestionTask,
                 io: IoService,
                 semaphore: asyncio.Semaphore) -> None:
        self.task = task
        self._io = io
        self._semaphore = semaphore

    async def get_data_from_file(self: Self):
        reader = None

        try:
            parse_kind = get_columns_and_syntax(self.task.download_date, self.task.target_year)
            if parse_kind is None:
                return

            columns, syntax = parse_kind
            async with self._semaphore:
                buffer = await self._io.f_read(self.task.dat_path)
                reader = PropertySalesRowReader(buffer, syntax)

                for row in get_data_from_file(self.task, reader, columns):
                    yield row
        except Exception as e:
            self._logger.error(self.task.dat_path, reader and reader.debug_info())
            raise e

class PropertySalesRowReader:
    _logger = getLogger(f'{__name__}.PropertySalesRowReader')
    _buffer: str
    _index: int
    _semi_colons: Syntax

    def __init__(self,
                 buffer: str,
                 semicolons: Syntax) -> None:
        self._semicolons = semicolons
        self._buffer = buffer
        self._index = 0

    def get_rows(self) -> Iterator[Tuple[str, List[str]]]:
        while self._index < len(self._buffer):
            mode = self._buffer[self._index:self._index + 1].strip()
            if mode not in self._semicolons:
                raise ValueError(f"Unexpected mode: {mode}@{self._index}")

            row_end = self._find_nth_semicolon_2(self._semicolons[mode])
            if row_end == -1:
                raise ValueError('Unexpected end of data')

            yield mode, self._buffer[self._index + 2: row_end].split(';')
            if mode == 'Z':
                break

            self._index = row_end + 2

    def _find_nth_semicolon_2(self, n: int) -> int:
        position = self._index
        semicolon_count = 0
        while semicolon_count < n:
            found_index = self._buffer.find(';', position)
            if found_index == -1:
                return -1
            position = found_index + 1
            semicolon_count += 1
        return found_index

    def debug_info(self) -> str:
        return f"""
          index @ {self._index}
          semicolumns @ {self._semicolons}
          REMAINING: {self._buffer[self._index:self._index + 400]}
        """

def get_data_from_file(task: PropertySaleIngestionTask, reader: PropertySalesRowReader, columns: FactoryMap):
    file_path = task.dat_path
    year = task.target_year
    kind, row = None, None

    try:
        a = None
        b = None
        c = None

        for kind, row in reader.get_rows():
            if kind == 'A':
                a = columns['A'].from_row(row, file_path=file_path, year=year)
                yield a
            elif kind == 'B':
                b = columns['B'].from_row(row, parent=a)
                yield b
            elif kind == 'C':
                c = columns['C'].from_row(row, parent=b)
                yield c
            elif kind == 'D':
                yield columns['D'].from_row(row, parent=c)
            elif kind == 'Z':
                yield columns['Z'].from_row(row, parent=a)
            else:
                raise ValueError(f"Unexpected record type: {kind}")
    except Exception as e:
        print(kind, row, file_path, reader.debug_info())
        raise e

def get_columns_and_syntax(download_date: Optional[datetime], year: int) -> Tuple[FactoryMap, Syntax] | None:
    if year <= 2001 and not download_date:
        return ps.t_1990, SYNTAX_1990
    elif year == 2001:
        if not download_date:
            raise TypeError('expected date')
        elif download_date.year > 2001:
            return ps.t_2002, SYNTAX_2002
        elif download_date.month < 6:
            return ps.t_1990, SYNTAX_1990
        elif download_date.month < 9:
            return None
        else:
            return ps.t_2002, SYNTAX_2002
    elif year < 2012:
        return ps.t_2002, SYNTAX_2002
    elif year == 2012:
        if not download_date:
            raise TypeError('expected date')
        elif download_date.month < 3:
            return ps.t_2002, SYNTAX_2002
        elif download_date.month == 3 and download_date.day < 13:
            return ps.t_2002, SYNTAX_2002
        else:
            return ps.t_current, SYNTAX_2012
    elif year == 2021:
        if not download_date:
            raise TypeError('expected date')
        elif download_date.month == 8 and download_date.day == 23:
            return ps.t_current, SYNTAX_2021
        else:
            return ps.t_current, SYNTAX_2012
    else:
        return ps.t_current, SYNTAX_2012



