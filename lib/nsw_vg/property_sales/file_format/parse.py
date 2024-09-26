import asyncio
from datetime import datetime
from logging import getLogger
from typing import Any, Dict, AsyncIterator, List, Optional, Tuple, Self, Type
import os
import re

from lib.service.io import IoService

from .factories import AbstractFormatFactory
from .input_data import PropertySaleDatFileMetaData
from .syntax import get_columns_and_syntax, Syntax
from .text_source import AbstractTextSource, StringTextSource

class PropertySalesRowParserFactory:
    _io: IoService

    def __init__(self: Self, io):
        self._io = io

    def create_parser(self: Self, file_data: PropertySaleDatFileMetaData) -> Optional['PropertySalesParser']:
        date = file_data.download_date
        year = file_data.published_year
        path = file_data.file_path

        Factory, syntax = get_columns_and_syntax(date, year)
        if Factory is None or syntax is None:
            return None

        # if date and (year == 2001 and 6 < date.month < 9):
        #     return None

        # if not date:
        #     return None

        # if not (year == 2001 and 6 < date.month < 9):
        #     return None

        factory = Factory.create(year=year, file_path=path)
        return PropertySalesParser(file_data, syntax, factory, self._io)

class PropertySalesParser:
    file_data: PropertySaleDatFileMetaData
    factory: AbstractFormatFactory
    syntax: Syntax

    _logger = getLogger(f'{__name__}.PropertySalesParser')
    _io: IoService

    def __init__(self,
                 file_data: PropertySaleDatFileMetaData,
                 syntax: Syntax,
                 factory: AbstractFormatFactory,
                 io: IoService) -> None:
        self.file_data = file_data
        self.factory = factory
        self.syntax = syntax
        self._io = io

    async def get_data_from_file(self: Self):
        reader = None

        try:
            buffer = await self._io.f_read(self.file_data.file_path)
            source = StringTextSource(self.file_data.file_path, buffer)
            reader = PropertySalesRowReader(source, self.syntax)
            async for row in get_data_from_file(reader, self.factory):
                yield row
        except Exception as e:
            self._logger.error(f'File: {self.file_data.file_path}')
            self._logger.error(f'Target Year: {self.file_data.published_year}')
            self._logger.error(f'Download Date: {self.file_data.download_date}')
            self._logger.error(f'Factory: {type(self.factory)}')
            self._logger.error(f'Syntax: {self.syntax}')
            if reader:
                reader_debug_info = await reader.debug_info()
                self._logger.error(f'Debug: {reader_debug_info}')
            raise e

class PropertySalesRowReader:
    _logger = getLogger(f'{__name__}.PropertySalesRowReader')
    _source: AbstractTextSource
    _index: int
    _semi_colons: Syntax

    def __init__(self,
                 source: AbstractTextSource,
                 semicolons: Syntax) -> None:
        self._semicolons = semicolons
        self._source = source
        self._index = 0

    async def get_rows(self: Self) -> AsyncIterator[Tuple[str, List[str]]]:
        row = ''

        while self._index < self._source.size():
            mode = await self._source.read(self._index, self._index + 1)
            mode = mode.strip()

            if mode not in self._semicolons:
                raise ValueError(f'Unexpected mode: "{mode}"@{self._index}' \
                                 f'last row: "{row}"')

            row_end = await self._find_nth_semicolon(self._semicolons[mode])
            if row_end == -1:
                raise ValueError('Unexpected end of data')

            row = await self._source.read(self._index + 2, row_end)

            yield mode, row.split(';')

            if mode == 'Z':
                break

            self._index = row_end + 2

    async def _find_nth_semicolon(self: Self, n: int) -> int:
        position = self._index
        semicolon_count = 0
        while semicolon_count < n:
            found_index = await self._source.find_index(';', position, self._index)
            if found_index == -1:
                return -1
            position = found_index + 1
            semicolon_count += 1
        return found_index

    async def debug_info(self: Self) -> str:
        remaining = await self._source.read(self._index, self._index + 400)
        return f"index @ {self._index}\n" \
               f"semicolumns @ {self._semicolons}\n" \
               f"REMAINING: {remaining}"

async def get_data_from_file(reader: PropertySalesRowReader, factory: AbstractFormatFactory):
    kind, row = None, None
    a, b, c = None, None, None

    try:
        async for kind, row in reader.get_rows():
            if kind == 'A':
                a = factory.create_a(row)
                yield a
            elif kind == 'B':
                b = factory.create_b(row, a_record=a)
                yield b
            elif kind == 'C':
                c = factory.create_c(row, b_record=b)
                yield c
            elif kind == 'D':
                yield factory.create_d(row, c_record=c)
            elif kind == 'Z':
                yield factory.create_z(row, a_record=a)
            else:
                raise ValueError(f"Unexpected record type: {kind}")
    except Exception as e:
        raise ParserError(kind, row, a, b, c, remaining=reader.debug_info()) from e

class ParserError(Exception):
    last_kind: str
    last_row: Dict[str, str]
    last_a_record: Any
    last_b_record: Any
    last_c_record: Any
    remaining: str

    def __init__(self, kind, row, a, b, c, remaining):
        super().__init__('parsing error')
        self.last_kind = kind
        self.last_row = row
        self.last_a_record = a
        self.last_b_record = b
        self.last_c_record = c
        self.remaining = remaining

