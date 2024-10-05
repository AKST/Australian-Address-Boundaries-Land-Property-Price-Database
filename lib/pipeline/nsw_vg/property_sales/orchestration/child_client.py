import abc
import asyncio
from dataclasses import dataclass
from logging import getLogger
import multiprocessing
from typing import Any, Self, Set


from ..data import PropertySaleDatFileMetaData
from .messages import ChildMessage

@dataclass
class NswVgPsChildStatus:
    queued: int

class NswVgPsChildClient:
    """
    This instance is created on the parent process
    """
    _logger = getLogger(f'{__name__}.NswVgPsChildClient')
    _q_child: multiprocessing.Queue
    _p_child: multiprocessing.Process
    _workload: int = 0

    def __init__(self,
                 q_child: multiprocessing.Queue,
                 p_child: multiprocessing.Process) -> None:
        self._q_child = q_child
        self._p_child = p_child

    def parse(self: Self, file: PropertySaleDatFileMetaData):
        self._workload += file.size
        self._q_child.put(ChildMessage.Parse(file))

    async def wait_till_done(self: Self):
        self._logger.debug(f'pid {self._p_child.pid} waiting to finish')
        self._q_child.put(ChildMessage.RequestClose())
        await asyncio.create_task(asyncio.to_thread(self._p_child.join))
        self._logger.debug(f'pid {self._p_child.pid} finished')

    def kill(self: Self):
        self._p_child.terminate()

    def status(self: Self) -> NswVgPsChildStatus:
        return NswVgPsChildStatus(queued=self._workload)

