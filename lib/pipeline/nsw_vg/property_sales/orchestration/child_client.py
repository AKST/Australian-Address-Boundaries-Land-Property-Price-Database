import abc
import asyncio
from dataclasses import dataclass
from logging import getLogger
import multiprocessing
from typing import Any, Self, Set


from ..data import PropertySaleDatFileMetaData
from .config import ChildMessage

@dataclass
class NswVgPsChildStatus:
    queued: int

class NswVgPsChildClient:
    """
    This instance is created on the parent process
    """
    _logger = getLogger(f'{__name__}.NswVgPsChildClient')
    _q_child: multiprocessing.Queue[ChildMessage.Message]
    _p_child: multiprocessing.Process
    _workload: int = 0
    _e_closed: asyncio.Event

    def __init__(self,
                 q_child: multiprocessing.Queue[ChildMessage.Message],
                 p_child: multiprocessing.Process) -> None:
        self._q_child = q_child
        self._p_child = p_child

    def parse(self: Self, file: PropertySaleDatFileMetaData):
        self._q_child.put(ChildMessage.Parse(file))

    def done(self: Self):
        self._q_child.put(ChildMessage.Done())

    def kill(self: Self):
        self._q_child.put(ChildMessage.Kill())

    def status(self: Self) -> NswVgPsChildStatus:
        return NswVgPsChildStatus(queued=self._workload)

    def close(self: Self):
        self._e_closed.set()

    async def wait_till_finish(self: Self):
        t_join = asyncio.create_task(asyncio.to_thread(self._p_child.join))
        t_event = asyncio.create_task(self._e_closed.wait())
        tasks = { t_join, t_event }
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

