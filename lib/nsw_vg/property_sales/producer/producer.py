import asyncio
from asyncio import TaskGroup, Queue, Semaphore
from datetime import datetime
from logging import getLogger
import re
from typing import Any, AsyncIterator, List, Optional, Tuple

from lib.nsw_vg.discovery import NswVgTarget
from lib.service.io import IoService
from lib.utility.concurrent import pipe, merge_async_iters
from .parse import PropertySalesRowParserFactory
from .task import PropertySaleIngestionTask

ProducerPair = Tuple[PropertySaleIngestionTask, Any]

class PropertySaleProducer:
    _logger = getLogger(f'{__name__}.PropertySaleProducer')
    _factory: PropertySalesRowParserFactory
    _semaphore: Semaphore
    _parent_dir: str
    _io: IoService

    def __init__(self,
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
        factory = PropertySalesRowParserFactory(io)
        return PropertySaleProducer(parent_dir, io, factory, semaphore)

    async def get_rows(self, targets: List[NswVgTarget]):
        queue: Queue[ProducerPair | None] = Queue()
        count = 0

        async def queue_work(tg: TaskGroup):
            self._logger.info("queuing tasks for worker")

            tasks = [task async for task in self.get_tasks(tg, targets)]
            count = len(tasks)

            running = [
                tg.create_task(row_worker(tg, task))
                for task in sorted(tasks, key=lambda t: t.size)
            ]

            self._logger.info("finished queuing all tasks")
            await asyncio.gather(*running)
            await queue.put(None)

        async def row_worker(tg: asyncio.TaskGroup, task: PropertySaleIngestionTask):
            parser = self._factory.create_parser(task)
            async with self._semaphore:
                async for row in parser.get_data_from_file():
                    await tg.create_task(queue.put((task, row)))

        async with TaskGroup() as tg:
            worker_task = tg.create_task(queue_work(tg))

            while True:
                result = await tg.create_task(queue.get())
                if result is None:
                    break
                yield result

            await worker_task

    async def get_tasks(self, g: TaskGroup, ts: List[NswVgTarget]) -> AsyncIterator[PropertySaleIngestionTask]:

        async def with_target(target):
            zip_path =  f'{self._parent_dir}/{target.zip_dst}'
            async for dat_path in self._io.grep_dir(zip_path, '*.DAT'):
                yield target, dat_path

        async def with_path(pair: Tuple[NswVgTarget, str]) -> PropertySaleIngestionTask | None:
            target, dat_path = pair
            if dat_path.endswith('-checkpoint.DAT'):
                return None
            if not await g.create_task(self._io.is_file(dat_path)):
                return None

            fsize = await g.create_task(self._io.f_size(dat_path))

            return PropertySaleIngestionTask(
                target=target,
                dat_path=dat_path,
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
