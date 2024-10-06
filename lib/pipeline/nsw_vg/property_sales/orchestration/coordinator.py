import asyncio
from asyncio import TaskGroup
from datetime import datetime
from logging import getLogger
import multiprocessing
import queue
import re
from typing import Any, Coroutine, List, Self, Tuple, Optional, TypeVar

from lib.pipeline.nsw_vg.discovery import NswVgTarget
from lib.pipeline.nsw_vg.property_sales.data import PropertySaleDatFileMetaData
from lib.service.io import IoService
from lib.utility.concurrent import merge_async_iters
from lib.utility.sampling import Sampler

from .child_client import NswVgPsChildClient
from .config import ParentConfig
from .messages import ParentMessage
from .telemetry import IngestionSample

T = TypeVar('T')

class NswVgPsIngestionCoordinator:
    config: ParentConfig

    _recv_queue: multiprocessing.Queue
    _logger = getLogger(f'{__name__}.NswVgPsIngestionCoordinator')
    _telemetry: Sampler[IngestionSample]
    _children: List[NswVgPsChildClient]
    _io: IoService
    _tg: TaskGroup

    def __init__(self: Self,
                 config: ParentConfig,
                 telemetry: Sampler[IngestionSample],
                 q_recv: multiprocessing.Queue,
                 children: List[NswVgPsChildClient],
                 task_group: TaskGroup,
                 io: IoService) -> None:
        self.config = config
        self._telemetry = telemetry
        self._children = children
        self._recv_queue = q_recv
        self._tg = task_group
        self._io = io

    async def process(self: Self, targets: List[NswVgTarget]):
        m_task = self._t(self._listen_to_children())
        try:
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
        except Exception as e:
            for p in self._children:
                p.terminate()
        finally:
            m_task.cancel()

    async def _listen_to_children(self: Self):
        async def next_message():
            """
            It is important to add this timeout otherwise we can
            block the parent thread from exiting.
            """
            try:
                task = asyncio.to_thread(self._recv_queue.get, timeout=0.1)
                return await task
            except queue.Empty:
                return None

        while True:
            match await next_message():
                case None:
                    continue
                case ParentMessage.Update(sender, value):
                    self._telemetry.count(value)
                    self._telemetry.log_if_necessary()
                case other:
                    self._logger.warn(f'unknown message {other} from {sender}')

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
                return

            zip_path =  f'{self.config.target_root_dir}/{t.zip_dst}'
            async for path in self._io.grep_dir(zip_path, '*.DAT'):
                download_date = get_download_date(path)
                if self.config.valid_download_date(download_date):
                    tasks.append(self._t(queue_task(t, path)))

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
