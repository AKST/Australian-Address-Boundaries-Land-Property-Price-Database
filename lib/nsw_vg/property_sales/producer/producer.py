import asyncio
from datetime import datetime
import re
from typing import AsyncIterator, List, Optional

from lib.nsw_vg.discovery import NswVgTarget
from lib.service.io import IoService
from .parse import PropertySalesRowParserFactory
from .task import PropertySaleIngestionTask

class PropertySaleProducer:
    _factory: PropertySalesRowParserFactory
    _parent_dir: str
    _io: IoService

    def __init__(self,
                 parent_dir: str,
                 io: IoService,
                 factory: PropertySalesRowParserFactory):
        self._parent_dir = parent_dir
        self._io = io
        self._factory = factory

    @staticmethod
    def create(parent_dir: str,
               io: IoService,
               concurrent_limit: int) -> 'PropertySaleProducer':
        semaphore = asyncio.Semaphore(concurrent_limit)
        factory = PropertySalesRowParserFactory(io, semaphore)
        return PropertySaleProducer(parent_dir, io, factory)

    async def get_rows(self, targets: List[NswVgTarget]):
        async for task in self.get_tasks(targets):
            parser = self._factory.create_parser(task)
            async for row in parser.get_data_from_file():
                yield task, row

    async def get_tasks(self, targets: List[NswVgTarget]) -> AsyncIterator[PropertySaleIngestionTask]:
        for target in targets:
            zip_path =  f'{self._parent_dir}/{target.zip_dst}'
            async for dat_path in self._io.grep_dir(zip_path, '*.DAT'):
                if dat_path.endswith('-checkpoint.DAT'):
                    continue
                if not await self._io.is_file(dat_path):
                    continue

                yield PropertySaleIngestionTask(
                    target=target,
                    dat_path=dat_path,
                    download_date=get_download_date(dat_path),
                )


def get_download_date(file_path: str) -> Optional[datetime]:
    if re.search(r"_\d{8}\.DAT$", file_path):
        file_date = file_path[file_path.rfind('_')+1:file_path.rfind('.')]
        return datetime.strptime(file_date, "%d%m%Y")
    else:
        return None
