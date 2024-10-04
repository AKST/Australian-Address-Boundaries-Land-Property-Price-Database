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

T = TypeVar('T')

class NswVgPsParentProc:
    _logger = getLogger(f'{__name__}.NswVgPsParentProc')
    _children: List[NswVgPsChildClient]
    _target_root_dir: str
    _io: IoService
    _tg: TaskGroup

    def __init__(self: Self,
                 children: List[NswVgPsChildClient],
                 target_root_dir: str,
                 task_group: TaskGroup,
                 io: IoService) -> None:
        self._children = children
        self._target_root_dir = target_root_dir
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
            self._t(c.wait_till_finish())
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
            zip_path =  f'{self._target_root_dir}/{t.zip_dst}'
            async for path in self._io.grep_dir(zip_path, '*.DAT'):
                tasks.append(self._t(queue_task(t, path)))

        async def queue_task(t: NswVgTarget, path: str):
            if path.endswith('-checkpoint.DAT'):
                return

            if not await self._t(self._io.is_file(path)):
                self._logger.debug(f'file not found, skipping {path}')
                return

            await queue.put(PropertySaleDatFileMetaData(
                file_path=path,
                published_year=t.datetime.year,
                download_date=get_download_date(path),
                size=await self._t(self._io.f_size(path)),
            ))

        async def run_all() -> None:
            await asyncio.gather(*[find_files(t) for t in targets])
            await asyncio.gather(*tasks)
            await queue.put(None)

        return asyncio.create_task(run_all()), queue

    def _t(self: Self, t: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
        return self._tg.create_task(t)

def get_download_date(file_path: str) -> Optional[datetime]:
    if re.search(r"_\d{8}\.DAT$", file_path):
        file_date = file_path[file_path.rfind('_')+1:file_path.rfind('.')]
        return datetime.strptime(file_date, "%d%m%Y")
    else:
        return None
