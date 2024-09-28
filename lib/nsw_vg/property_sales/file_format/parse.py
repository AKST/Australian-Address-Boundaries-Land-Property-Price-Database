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
from .text_source import AbstractTextSource

class PropertySalesRowParserFactory:
    Source: Type[AbstractTextSource]
    chunk_size: int
    _io: IoService

    def __init__(self: Self,
                 io: IoService,
                 Source: Type[AbstractTextSource],
                 chunk_size: int = 8 * 2 ** 10):
        self._io = io
        self.Source = Source
        self.chunk_size = chunk_size

    async def create_parser(self: Self, file_data: PropertySaleDatFileMetaData) -> Optional['PropertySalesParser']:
        date = file_data.download_date
        year = file_data.published_year
        path = file_data.file_path

        # if year != 2001 or not date or date.month != 7:
        #     return None

        Factory, syntax = get_columns_and_syntax(date, year)
        if Factory is None or syntax is None:
            return None

        factory = Factory.create(year=year, file_path=path)
        f_path = file_data.file_path
        source = await self.Source.create(
            file_data.file_path,
            self._io,
            chunk_size=self.chunk_size,
        )
        reader = PropertySalesRowReader(source, syntax)
        return PropertySalesParser(file_data, factory, reader)

class PropertySalesParser:
    file_data: PropertySaleDatFileMetaData
    constructors: AbstractFormatFactory

    _reader: 'PropertySalesRowReader'
    _logger = getLogger(f'{__name__}.PropertySalesParser')

    def __init__(self: Self,
                 file_data: PropertySaleDatFileMetaData,
                 constructors: AbstractFormatFactory,
                 reader: 'PropertySalesRowReader') -> None:
        self.file_data = file_data
        self.constructors = constructors
        self._reader = reader

    async def get_data_from_file(self: Self):
        kind, row = None, None
        a, b, c, d = None, None, None, None

        try:
            async for kind, row in self._reader.get_rows():
                if kind == 'A':
                    a = self.constructors.create_a(row)
                    yield a
                elif kind == 'B':
                    b = self.constructors.create_b(row, a_record=a)
                    yield b
                elif kind == 'C':
                    c = self.constructors.create_c(row, b_record=b)
                    yield c
                elif kind == 'D':
                    d = self.constructors.create_d(row, c_record=c)
                    yield d
                elif kind == 'Z':
                    yield self.constructors.create_z(row, a_record=a)
                else:
                    raise ValueError(f"Unexpected record type: {kind}")
        except Exception as e:
            self._logger.error(f'Source: {self.file_data.file_path}')
            self._logger.error(f'Target Year: {self.file_data.published_year}')
            self._logger.error(f'Download Date: {self.file_data.download_date}')
            self._logger.error(f'Factory: {type(self.constructors)}')
            self._logger.error(f'Syntax: {self._reader._semi_colons}')
            self._logger.error(f'last row: {row}')
            self._logger.error(f'last kind: {kind}')
            self._logger.error(f'last a row: {a}')
            self._logger.error(f'last b row: {b}')
            self._logger.error(f'last c row: {c}')
            self._logger.error(f'last d row: {d}')

            # reader_debug_info = await self._reader.debug_info()
            # self._logger.error(f'Debug: {reader_debug_info}')
            self._logger.exception(e)
            raise e

class PropertySalesRowReader:
    _logger = getLogger(f'{__name__}.PropertySalesRowReader')
    _source: AbstractTextSource
    _index: int
    _semi_colons: Syntax

    __last_row: str = ''

    def __init__(self,
                 source: AbstractTextSource,
                 semicolons: Syntax) -> None:
        self._semi_colons = semicolons
        self._source = source
        self._index = 0

    async def get_rows(self: Self) -> AsyncIterator[Tuple[str, List[str]]]:
        while self._index < self._source.size():
            row_s, mode = await self._get_mode()
            row_e, row = await self._get_row(mode, row_s)

            yield mode, row

            if mode == 'Z':
                break

            self._index = row_e + 2

    async def _get_row(self, mode: str, row_start: int) -> Tuple[int, List[str]]:
        syntax = self._semi_colons[mode]
        if isinstance(syntax, list):
            raise TypeError('not implmented')

        row_end = await self._find_nth_semicolon(syntax)

        if row_end == -1:
            raise ValueError(
                f'Unexpected end of data: "{mode}"@{self._index}, ' \
                f'last row: "{self.__last_row}", ' \
                f'source: {self._source.source_name}'
            )

        row_s = await self._source.read(row_start+2, row_end)
        self.__last_row = row_s

        return row_end, row_s.split(';')

    async def _get_mode(self) -> Tuple[int, str]:
        mode_end = await self._find_nth_semicolon(1)
        mode = await self._source.read(self._index, mode_end)
        mode = mode.strip()

        if mode not in self._semi_colons:
            raise ValueError(f'Unexpected mode: "{mode}"@{self._index}, ' \
                             f'last row: "{self.__last_row}", ' \
                             f'source: {self._source.source_name}')

        return mode_end - 1, mode

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
               f"semicolumns @ {self._semi_colons}\n" \
               f"REMAINING: {remaining}"

