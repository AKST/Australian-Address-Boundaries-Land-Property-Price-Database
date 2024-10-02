import abc
from typing import Self

from lib.service.io import IoService

class AbstractTextSource(abc.ABC):
    @classmethod
    async def create(cls, file_path: str, io_service: IoService, **kwargs):
        raise NotImplementedError('not implemented')

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

    @classmethod
    async def create(cls, file_path: str, io_service: IoService, **kwargs):
        text = await io_service.f_read(file_path)
        return cls(file_path, text)

class BufferedFileReaderTextSource(AbstractTextSource):
    """
    Turns out this ain't a ton more efficent than the
    string text source defined above. I hate my life
    for having written this. But now I know more
    about unicode I guess...
    """
    _offset: int = 0
    _buffer: bytes
    _chunk_size: int
    _file_path: str
    _file_size: int
    _io_service: IoService
    _max_chunk_size: int = 64 * 1024 * 1024  # 64 MB maximum buffer size


    def __init__(self,
                 buffer: bytes,
                 chunk_size: int,
                 file_path: str,
                 file_size: int,
                 io_service: IoService) -> None:
        self._buffer = buffer
        self._chunk_size = chunk_size
        self._file_path = file_path
        self._file_size = file_size
        self._io_service = io_service

    @property
    def source_name(self) -> str:
        return self._file_path

    def size(self) -> int:
        return self._file_size  # Size in bytes

    async def read(self, start: int, end: int) -> str:
        data_chunks = []

        while start < end:
            if start < self._offset or start >= self._offset + len(self._buffer):
                required_size = end - start
                await self._load_chunk(start, required_size)

            relative_s = start - self._offset
            relative_e = min(end - self._offset, len(self._buffer))
            data_chunk = self._buffer[relative_s:relative_e]
            data_chunks.append(data_chunk)
            start += relative_e - relative_s

        data_bytes = b''.join(data_chunks)
        return data_bytes.decode('utf-8')

    async def find_index(self, search: str, offset: int, retained_index: int) -> int:
        search_b = search.encode('utf-8')

        if offset < self._offset:
            raise ValueError("Offset is smaller than current buffer start. This should not happen in linear reading.")

        while True:
            if offset < self._offset or offset >= self._offset + len(self._buffer):
                await self._load_chunk(retained_index)

            relative_offset = offset - self._offset
            found_index = self._buffer.find(search_b, relative_offset)

            if found_index != -1:
                return self._offset + found_index

            if self._offset + len(self._buffer) >= self._file_size:
                return -1

            additional_size = min(self._chunk_size, self._file_size - (self._offset + len(self._buffer)))
            if additional_size == 0:
                return -1

            self._chunk_size = min(self._chunk_size + additional_size, self._max_chunk_size)
            await self._load_chunk(retained_index)

    async def _load_chunk(self, start: int, required_size: int | None = None) -> None:
        if required_size and required_size > self._chunk_size:
            while self._chunk_size < required_size and self._chunk_size < self._max_chunk_size:
                self._chunk_size *= 2
            self._chunk_size = min(self._chunk_size, self._max_chunk_size)

        self._offset = start
        if start >= self._file_size:
            # If the start is beyond file size, set empty buffer
            self._buffer = b''
        else:
            read_size = min(self._chunk_size, self._file_size - start)
            extra_bytes = 4  # Max UTF-8 character size
            read_size_with_extra = min(read_size + extra_bytes, self._file_size - start)
            self._buffer = b''
            self._buffer = await self._io_service.f_read_slice(self._file_path, start, read_size_with_extra)
            self._buffer = self._truncate_buffer_to_valid_utf8(self._buffer)

    @classmethod
    async def create(cls,
                     file_path: str,
                     io_service: IoService,
                     **kwargs) -> "BufferedFileReaderTextSource":
        chunk_size = kwargs.get('chunk_size', 8 * 2 ** 8)
        file_size = await io_service.f_size(file_path)
        buffer_size = min(file_size, chunk_size)

        extra_bytes = 4  # Max UTF-8 character size
        buffer_size_with_extra = min(buffer_size + extra_bytes, file_size)
        buffer = await io_service.f_read_slice(file_path, 0, buffer_size_with_extra)

        buffer = cls._truncate_buffer_to_valid_utf8(buffer)
        return cls(buffer, chunk_size, file_path, file_size, io_service)

    @classmethod
    def _truncate_buffer_to_valid_utf8(cls, buffer: bytes) -> bytes:
        if len(buffer) == 0:
            return buffer

        try:
            buffer.decode('utf-8')
            return buffer
        except UnicodeDecodeError as e:
            return b'' if e.start == 0 else buffer[:e.start]

