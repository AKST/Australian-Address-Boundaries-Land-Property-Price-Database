import asyncio
from asyncio import TaskGroup
from datetime import datetime
from logging import getLogger
import re
from typing import Any, Coroutine, List, Self, Tuple, Optional, TypeVar

from lib.pipeline.nsw_vg.discovery import NswVgTarget
from lib.pipeline.nsw_vg.property_sales.data import PropertySaleDatFileMetaData
from lib.service.io import IoService

from .child_client import NswVgPsChildClient
from .config import ParentConfig

T = TypeVar('T')

class NswVgPsIngestionCoordinator:
    config: ParentConfig

    _logger = getLogger(f'{__name__}.NswVgPsIngestionCoordinator')
    _children: List[NswVgPsChildClient]
    _io: IoService
    _tg: TaskGroup

    def __init__(self: Self,
                 config: ParentConfig,
                 children: List[NswVgPsChildClient],
                 task_group: TaskGroup,
                 io: IoService) -> None:
        self.config = config
        self._children = children
        self._tg = task_group
        self._io = io

    async def process(self: Self, targets: List[NswVgTarget]):
        q_task, queue = self._file_queue(targets)
        while True:
            file = await self._t(queue.get())
            if file is None:
                break

            self._find_next_child().parse(file)

        await asyncio.gather(q_task, *[
            self._t(c.wait_till_done())
            for c in self._children
        ])

    def _find_next_child(self: Self) -> NswVgPsChildClient:
        return min(self._children, key=lambda c: c.status().queued)

    def _file_queue(
        self: Self,
        targets: List[NswVgTarget],
    ) -> Tuple[asyncio.Task, asyncio.Queue[PropertySaleDatFileMetaData | None]]:
        """
        He we are queueing as many parse tasks as possible
        with as much throughput as possible and inserting
        them all into a queue.
        """
        queue = asyncio.Queue[PropertySaleDatFileMetaData | None]()
        tasks: List[asyncio.Task] = []

        async def find_files(t: NswVgTarget):
            if not self.config.valid_publish_date(t.datetime.year):
                self._logger.debug(f'skipping target {t}')
                return

            self._logger.debug(f'queuing files from {t}')
            zip_path =  f'{self.config.target_root_dir}/{t.zip_dst}'
            async for path in self._io.grep_dir(zip_path, '*.DAT'):
                download_date = get_download_date(path)
                if self.config.valid_download_date(download_date):
                    tasks.append(self._t(queue_task(t, path)))
                else:
                    self._logger.debug(f'skipping file {path}')

        async def queue_task(t: NswVgTarget, path: str):
            if path.endswith('-checkpoint.DAT'):
                return

            if not await self._t(self._io.is_file(path)):
                self._logger.debug(f'file not found, skipping {path}')
                return

            download_date = get_download_date(path)

            await queue.put(PropertySaleDatFileMetaData(
                file_path=path,
                published_year=t.datetime.year,
                download_date=get_download_date(path),
                size=await self._t(self._io.f_size(path)),
            ))

        async def run_all() -> None:
            self._logger.debug(f'queuing files')
            await asyncio.gather(*[find_files(t) for t in targets])
            await asyncio.gather(*tasks)
            await queue.put(None)
            self._logger.debug(f'All files queued')

        return asyncio.create_task(run_all()), queue

    def _t(self: Self, t: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
        return self._tg.create_task(t)

def get_download_date(file_path: str) -> Optional[datetime]:
    if re.search(r"_\d{8}\.DAT$", file_path):
        file_date = file_path[file_path.rfind('_')+1:file_path.rfind('.')]
        return datetime.strptime(file_date, "%d%m%Y")
    else:
        return None
