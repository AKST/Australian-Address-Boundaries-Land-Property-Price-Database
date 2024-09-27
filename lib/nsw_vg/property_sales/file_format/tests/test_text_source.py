import asyncio
import pytest
from unittest.mock import AsyncMock

from lib.service.io import IoService

from ..text_source import *


@pytest.fixture
def ascii_text() -> str:
    def create():
        yield 'A;aaaa;bbbb;cccc;\n'
        for b in range(0, 400):
            yield f'\nB;{b};aaaa;bbbb;cccc;dddd;'
            for c in range(0, 2):
                yield f'\nC;{c};aaaa;bbbb;cccc;dddd;eeee;'
                for d in range(0, 4):
                    yield f'\nD;{c};aaaa;bbbb;cccc;dddd;eeee;ffff;'
    return ''.join(create())

@pytest.fixture
def unicode_text() -> str:
    def create():
        yield 'A;aaaa;b🙂bb;cccc;\n'
        for b in range(0, 400):
            yield f'\nB;{b};aaaa;bb🌞b;cccc;dddd;'
            for c in range(0, 2):
                yield f'\nC;{c};aa🏛️a;bbbb;cccc;dddd;eeee;'
                for d in range(0, 4):
                    yield f'\nD;{c};aaaa;bbbb;cccc;dddd;eeee;ffff;'
    return ''.join(create())

@pytest.fixture
def mock_ascii_io_service(ascii_text: str):
    b_source = ascii_text.encode('utf-8')
    io_service = AsyncMock(spec=IoService)
    async def mock_f_read_slice(file_path, start, size) -> bytes:
        return b_source[start:start + size]
    io_service.f_read_slice.side_effect = mock_f_read_slice
    io_service.f_size.return_value = len(b_source)
    return io_service

@pytest.fixture
def mock_unicode_io_service(unicode_text: str):
    b_source = unicode_text.encode('utf-8')
    io_service = AsyncMock(spec=IoService)
    async def mock_f_read_slice(file_path, start, size) -> bytes:
        return b_source[start:start + size]
    io_service.f_read_slice.side_effect = mock_f_read_slice
    io_service.f_size.return_value = len(b_source)
    return io_service

@pytest.fixture
async def a_buffered_source(mock_ascii_io_service) -> BufferedFileReaderTextSource:
    return await BufferedFileReaderTextSource.create(
        file_path="mock_file.txt",
        io_service=mock_ascii_io_service,
        chunk_size=128*2 ** 4,
    )

@pytest.fixture
async def u_buffered_source(mock_unicode_io_service) -> BufferedFileReaderTextSource:
    return await BufferedFileReaderTextSource.create(
        file_path="mock_file.txt",
        io_service=mock_unicode_io_service,
        chunk_size=128*2 ** 4,
    )

@pytest.fixture
def a_string_source(ascii_text: str):
    return StringTextSource(source_name="in_memory", text=ascii_text)

@pytest.fixture
def u_string_source(unicode_text: str):
    return StringTextSource(source_name="in_memory", text=unicode_text)

@pytest.mark.asyncio
@pytest.mark.parametrize("chunk_size", [
    50,
    *[8 * 2 ** n for n in range(0, 6)]
])
async def test_a_read_slice(chunk_size: int, ascii_text: str, a_string_source, a_buffered_source):
    string_source = a_string_source
    buffered_source = await a_buffered_source
    chunks = [
        (n - chunk_size, n)
        for n in range(chunk_size, chunk_size, len(ascii_text))
    ]

    for start, end in chunks:
        string_text = await string_source.read(start, start + 1)
        buffered_text = await buffered_source.read(start, start + 1)
        assert string_text == buffered_text, f"Mismatch between StringTextSource and BufferedFileReaderTextSource for range {start}-{end}"

        string_text = await string_source.read(start, end)
        buffered_text = await buffered_source.read(start, end)
        assert string_text == buffered_text, f"Mismatch between StringTextSource and BufferedFileReaderTextSource for range {start}-{end}"

@pytest.mark.asyncio
async def test_read_inc(ascii_text: str, a_buffered_source, a_string_source):
    string_source = a_string_source
    buffered_source = await a_buffered_source
    for n in range(0, len(ascii_text)):
        string_text = await string_source.read(n, n+1)
        buffered_text = await buffered_source.read(n, n+1)
        assert string_text == buffered_text, f"'{string_text}' != '{buffered_text}'"

@pytest.mark.asyncio
@pytest.mark.parametrize("depth", list(range(1, 10)))
async def test_unicode_find(depth: int,
                            u_buffered_source,
                            u_string_source):
    async def find_nth(source, start):
        position = start
        count = 0
        while count < depth:
            found_index = await source.find_index(';', position, start)
            if found_index == -1:
                return -1
            position = found_index + 1
            count += 1
        return found_index

    string_source = u_string_source
    buffered_source = await u_buffered_source
    s_last_index = 0
    b_last_index = 0

    while b_last_index < buffered_source.size():
        s_end = await find_nth(string_source, s_last_index)
        b_end = await find_nth(buffered_source, b_last_index)

        if s_end == -1 or s_end == -1:
            break

        string_text = await string_source.read(s_last_index, s_end)
        buffered_text = await buffered_source.read(b_last_index, b_end)
        assert string_text == buffered_text, f"@ {s_last_index},{b_last_index} " \
                                             f"{string_text} != {buffered_text}"

        if s_last_index == s_end or b_last_index == b_end:
            print(string_text, buffered_text, s_last_index, s_end, b_last_index, b_end)
            raise Exception('weird')

        s_last_index = s_end + 1
        b_last_index = b_end + 1

