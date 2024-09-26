import abc
from typing import Self

from lib.service.io import IoService

class AbstractTextSource(abc.ABC):
    @property
    @abc.abstractmethod
    def source_name(self) -> str:
        pass

    @abc.abstractmethod
    def size(self) -> int:
        pass

    @abc.abstractmethod
    async def read(self, start: int, end: int) -> str:
        pass

    @abc.abstractmethod
    async def find_index(self, search: str, offset: int, retained_index: int) -> int:
        pass

class StringTextSource(AbstractTextSource):
    _text: str
    _source_name: str

    def __init__(self: Self, source_name: str, text: str) -> None:
        self._source_name = source_name
        self._text = text

    @property
    def source_name(self: Self) -> str:
        return self._source_name

    def size(self: Self) -> int:
        return len(self._text)

    async def read(self: Self, start: int, end: int) -> str:
        return self._text[start:end]

    async def find_index(self: Self, search: str, offset: int, retained_index: int) -> int:
        return self._text.find(';', offset)

class BufferedFileReaderTextSource(AbstractTextSource):
    _buffer: str
    _offset: int
    _chunk_size: int
    _file_path: str
    _file_size: int
    _io_service: IoService

    def __init__(self, file_path: str, io_service: IoService, chunk_size: int = 4096) -> None:
        self._file_path = file_path
        self._chunk_size = chunk_size
        self._buffer = ""
        self._offset = 0
        self._io_service = io_service

    @property
    def source_name(self) -> str:
        return self._file_path

    def size(self) -> int:
        return self._file_size

    async def _load_chunk(self, start: int) -> None:
        """Loads a chunk into the buffer starting from a specific position using IoService."""
        self._offset = start
        self._buffer = await self._io_service.f_read_slice(self._file_path, start, self._chunk_size)

    async def read(self, start: int, end: int) -> str:
        if start < self._offset or end > self._offset + len(self._buffer):
            # If requested range is outside the current buffer, load a new chunk
            await self._load_chunk(start)

        relative_start = start - self._offset
        relative_end = end - self._offset

        return self._buffer[relative_start:relative_end]

    async def find_index(self, search: str, offset: int, retained_index: int) -> int:
        if offset < self._offset:
            raise ValueError("Offset is smaller than current buffer start. This should not happen in linear reading.")

        relative_offset = offset - self._offset
        found_index = self._buffer.find(search, relative_offset)

        if found_index == -1:
            # Load the next chunk and try again from the retained_index
            await self._load_chunk(self._offset + len(self._buffer))
            found_index = self._buffer.find(search, 0)

        if found_index == -1:
            return -1  # not found
        return self._offset + found_index

    @classmethod
    async def create(cls, file_path: str, io_service: IoService, chunk_size: int = 4096) -> "BufferedFileReaderTextSource":
        instance = cls(file_path, io_service, chunk_size)
        instance._file_size = await instance._io_service.f_size(file_path)
        await instance._load_chunk(0)  # Load the initial chunk
        return instance

