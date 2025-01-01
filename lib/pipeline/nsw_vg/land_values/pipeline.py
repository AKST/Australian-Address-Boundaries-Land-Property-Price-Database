import asyncio
from dataclasses import dataclass, field
from logging import getLogger
from multiprocessing import Process, Queue as MpQueue
import queue
from typing import List, Self

from .config import NswVgLvChildMsg, NswVgLvParentMsg
from .discovery import CsvDiscovery
from .telemetry import NswVgLvTelemetry

class NswVgLvPipeline:
    _logger = getLogger(f'{__name__}.NswVgLvPipeline')
    _workers: List['WorkerClient']
    _recv_q: MpQueue

    def __init__(self: Self,
                 recv_queue: MpQueue,
                 telemetry: NswVgLvTelemetry,
                 discovery: CsvDiscovery):
        self._recv_q = recv_queue
        self._discovery = discovery
        self._telemetry = telemetry
        self._workers = []

    def add_worker(self: Self, worker: 'WorkerClient'):
        self._workers.append(worker)
        self._telemetry.establish_worker(worker.id)

    async def start(self: Self):
        recv_t = asyncio.create_task(self._start_listening())
        try:
            async for file in self._discovery.files():
                worker = self._next_worker()
                worker.send(NswVgLvChildMsg.Ingest(file))
                self._telemetry.record_work_allocation(worker.id, file.size)
            await asyncio.gather(*[w.join() for w in self._workers])
        except Exception as e:
            self.kill()
            raise e
        finally:
            recv_t.cancel()

    def kill(self: Self):
        for worker in self._workers:
            worker.kill()

    async def _start_listening(self: Self):
        async def next_message():
            """
            It is important to add this timeout otherwise we can
            block the parent thread from exiting.
            """
            try:
                return await asyncio.to_thread(self._recv_q.get, timeout=0.1)
            except queue.Empty:
                return None

        while True:
            message = await next_message()
            self._logger.debug(f"message received {message}")
            match message:
                case None:
                    continue
                case NswVgLvParentMsg.FileRowsParsed(id, file, rows):
                    self._telemetry.record_file_parse(file, rows)
                case NswVgLvParentMsg.FileRowsSaved(id, file, rows):
                    self._telemetry.record_file_saved(file, rows)
                case other:
                    self._logger.warn(f'unknown message {other}')

    def _next_worker(self: Self):
        return min(self._workers, key=lambda c: c.workload)

@dataclass
class WorkerClient:
    id: int
    process: Process
    send_q: MpQueue
    workload: int = field(default = 0)

    def send(self: Self, msg: NswVgLvChildMsg.Base):
        self.workload += msg.workload()
        self.send_q.put(msg)

    async def join(self: Self):
        self.send(NswVgLvChildMsg.RequestClose())
        await asyncio.create_task(asyncio.to_thread(self.process.join))
        match self.process.exitcode:
            case 0: return
            case 137: return
            case 143: return
            case n: raise Exception(f'child process failed with {n}')

    def kill(self: Self):
        if self.process.is_alive():
            self.process.kill()
