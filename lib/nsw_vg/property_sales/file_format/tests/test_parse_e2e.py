import pytest
from datetime import datetime

from lib.service.io import IoService
from ..text_source import *
from ..parse import PropertySalesRowParserFactory
from ..input_data import PropertySaleDatFileMetaData

@pytest.mark.asyncio
@pytest.mark.parametrize("file_path,published_year,download_date", [
    ('./_fixtures/ps_2021_20210823.dat', 2021, datetime(2021, 8, 23)),
    ('./_fixtures/ps_2011_20111003.dat', 2011, datetime(2011, 10, 3)),
    ('./_fixtures/ps_2004_20040916.dat', 2004, datetime(2004, 9, 16)),
    ('./_fixtures/ps_2001_20010822.dat', 2001, datetime(2001, 8, 22)),
    # ('./_fixtures/ps_2001_20010720.dat', 2001, datetime(2001, 7, 20)),
    ('./_fixtures/ps_1990_fake.dat', 1990, None),
])
async def test_end_to_end(file_path: str,
                             published_year: int,
                             download_date: datetime | None):
    io = IoService.create(1)
    s_factory = PropertySalesRowParserFactory(io,
                                              StringTextSource)
    b_factory = PropertySalesRowParserFactory(io,
                                              BufferedFileReaderTextSource,
                                              chunk_size=8 * 2 ** 5)
    file_size = await io.f_size(file_path)
    file_data = PropertySaleDatFileMetaData(file_path=file_path, published_year=published_year, download_date=download_date, size=file_size)
    s_parser = await s_factory.create_parser(file_data)
    b_parser = await b_factory.create_parser(file_data)

    if s_parser is None or b_parser is None:
        raise TypeError()

    s_items = [it async for it in s_parser.get_data_from_file()]
    b_items = [it async for it in b_parser.get_data_from_file()]
    assert s_items == b_items

