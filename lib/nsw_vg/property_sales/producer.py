import asyncio
from asyncio import TaskGroup, Queue, Semaphore
from datetime import datetime
from logging import getLogger
import re
from typing import Any, AsyncIterator, List, Optional, Self, Tuple

from lib.nsw_vg.discovery import NswVgTarget
from lib.service.io import IoService
from lib.utility.concurrent import pipe, merge_async_iters

from .file_format.parse import PropertySalesRowParserFactory
from .file_format.text_source import StringTextSource, BufferedFileReaderTextSource
from .file_format import PropertySaleDatFileMetaData

ProducerPair = Tuple[PropertySaleDatFileMetaData, Any]

class PropertySaleProducer:
    _logger = getLogger(f'{__name__}.PropertySaleProducer')
    _factory: PropertySalesRowParserFactory
    _semaphore: Semaphore
    _parent_dir: str
    _io: IoService

    def __init__(self: Self,
                 parent_dir: str,
                 io: IoService,
                 factory: PropertySalesRowParserFactory,
                 semaphore: Semaphore) -> None:
        self._semaphore = semaphore
        self._parent_dir = parent_dir
        self._io = io
        self._factory = factory

    @staticmethod
    def create(parent_dir: str,
               io: IoService,
               concurrent_limit: int) -> 'PropertySaleProducer':
        semaphore = Semaphore(concurrent_limit)
        factory = PropertySalesRowParserFactory(io, BufferedFileReaderTextSource)
        return PropertySaleProducer(parent_dir, io, factory, semaphore)

    async def get_rows(self: Self, targets: List[NswVgTarget]):
        queue: Queue[ProducerPair | None] = Queue()
        count = 0

        async def row_worker(tg: asyncio.TaskGroup, file: PropertySaleDatFileMetaData):
            async with self._semaphore:
                parser = await tg.create_task(self._factory.create_parser(file))
                if parser is None:
                    return

                async for row in parser.get_data_from_file():
                    await tg.create_task(queue.put((file, row)))

        async def queue_work(tg: TaskGroup):
            self._logger.info("queuing files for worker")
            running = []

            async for f in self.get_files(tg, targets):
                task = tg.create_task(row_worker(tg, f))
                running.append(task)

            self._logger.info("finished queuing all files")
            await asyncio.gather(*running)
            await queue.put(None)

        async with TaskGroup() as tg:
            worker_task = tg.create_task(queue_work(tg))

            while True:
                result = await tg.create_task(queue.get())
                if result is None:
                    break
                yield result

            await worker_task

    async def get_files(self: Self, g: TaskGroup, ts: List[NswVgTarget]) -> AsyncIterator[PropertySaleDatFileMetaData]:
        async def with_target(target):
            zip_path =  f'{self._parent_dir}/{target.zip_dst}'
            async for dat_path in self._io.grep_dir(zip_path, '*.DAT'):
                yield target, dat_path

        async def with_path(pair: Tuple[NswVgTarget, str]) -> PropertySaleDatFileMetaData | None:
            target, dat_path = pair
            if dat_path.endswith('-checkpoint.DAT'):
                return None
            if not await g.create_task(self._io.is_file(dat_path)):
                return None

            date = target.datetime
            if date is None:
                raise TypeError('target is missing date')

            fsize = await g.create_task(self._io.f_size(dat_path))

            return PropertySaleDatFileMetaData(
                file_path=dat_path,
                published_year=date.year,
                download_date=get_download_date(dat_path),
                size=fsize,
            )

        producer = lambda: merge_async_iters([with_target(t) for t in ts])
        async for task in pipe(producer, with_path):
            if task is not None:
                yield task


def get_download_date(file_path: str) -> Optional[datetime]:
    if re.search(r"_\d{8}\.DAT$", file_path):
        file_date = file_path[file_path.rfind('_')+1:file_path.rfind('.')]
        return datetime.strptime(file_date, "%d%m%Y")
    else:
        return None
