import abc
import asyncio
from dataclasses import dataclass
from logging import getLogger
import multiprocessing
from typing import Any, Self, Set


from ..data import PropertySaleDatFileMetaData
from .messages import ChildMessage, ParentMessage

@dataclass
class NswVgPsChildStatus:
    queued: int

class NswVgPsChildClient:
    """
    This instance is created on the parent process
    """
    _logger = getLogger(f'{__name__}.NswVgPsChildClient')
    _q_send: multiprocessing.Queue
    _p_child: multiprocessing.Process
    _workload: int = 0

    def __init__(self,
                 q_send: multiprocessing.Queue,
                 p_child: multiprocessing.Process) -> None:
        self._q_send = q_send
        self._p_child = p_child

    def parse(self: Self, file: PropertySaleDatFileMetaData):
        self._workload += file.size
        self._q_send.put(ChildMessage.Parse(file))

    async def wait_till_done(self: Self):
        self._logger.debug(f'pid {self._p_child.pid} waiting to finish')
        self._q_send.put(ChildMessage.RequestClose())
        await asyncio.create_task(asyncio.to_thread(self._p_child.join))
        self._logger.debug(f'pid {self._p_child.pid} finished')
        match self._p_child.exitcode:
            case 0: return
            case 137: return
            case 143: return
            case n: raise Exception(f'child process failed with {n}')

    def kill(self: Self):
        if self._p_child.is_alive():
            self._p_child.kill()

    def terminate(self: Self):
        if self._p_child.is_alive():
            self._p_child.terminate()

    def status(self: Self) -> NswVgPsChildStatus:
        return NswVgPsChildStatus(queued=self._workload)

