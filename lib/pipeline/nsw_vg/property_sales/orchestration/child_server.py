import abc
import asyncio
from logging import getLogger
import multiprocessing
import queue
from typing import Any, Coroutine, Self, Set, TypeVar, Optional

from lib.utility.sampling import Sampler

from ..file_format import PropertySalesRowParserFactory
from ..data import BasePropertySaleFileRow, PropertySaleDatFileMetaData, SaleDataFileSummary
from ..ingestion import PropertySalesIngestion
from .messages import ChildMessage, ParentMessage
from .telemetry import IngestionSample

T = TypeVar("T")

class ParentClient:
    logger = getLogger(f'{__name__}.ParentClient')

    pid: int
    parsed: int = 0
    ingested: int = 0
    q_send: multiprocessing.Queue
    threshold: int

    def __init__(self: Self,
                 pid: int,
                 q_send: multiprocessing.Queue,
                 threshold: int):
        self.pid = pid
        self.threshold = threshold
        self.q_send = q_send
        self.parsed = 0

    def on_parsed(self: Self) -> None:
        self.parsed += 1
        if self.parsed >= self.threshold:
            self.flush()

    def flush(self: Self) -> None:
        sample = IngestionSample(parsed=self.parsed, ingested=self.ingested)
        self.parsed = 0
        self.ingested = 0
        self._put(sample)

    def on_ingest(self: Self, size: int) -> None:
        self.ingested += size
        if self.ingested >= self.threshold:
            self.flush()

    def _put(self: Self, sample: IngestionSample):
        try:
            message = ParentMessage.Update(self.pid, sample)
            self.q_send.put(message, timeout=0.5)
        except queue.Full:
            self.logger.error("failed to send message")


class NswVgPsChildServer:
    """
    This instance is created on a child process
    """
    logger = getLogger(f'{__name__}.NswVgPsChildServer')
    tg: asyncio.TaskGroup

    q_rows: asyncio.Queue[BasePropertySaleFileRow | None]
    p_parent: ParentClient
    t_ingest: asyncio.Task | None
    t_parser: Set[asyncio.Task]
    closing = False

    parser_factory: PropertySalesRowParserFactory
    ingestion: PropertySalesIngestion

    def __init__(self,
                 tg: asyncio.TaskGroup,
                 ingestion: PropertySalesIngestion,
                 parser_factory: PropertySalesRowParserFactory,
                 p_parent: ParentClient) -> None:
        self.tg = tg
        self.q_rows = asyncio.Queue()
        self.p_parent = p_parent
        self.t_parser = set()
        self.t_ingest = None
        self.parser_factory = parser_factory
        self.ingestion = ingestion

    async def flush(self: Self):
        if self.t_parser:
            await asyncio.gather(*self.t_parser)
            return await self.flush()

        self.p_parent.flush()
        await self.q_rows.put(None)
        if self.t_ingest:
            await self.t_ingest

    async def on_message(self: Self, message: ChildMessage.Message) -> None:
        match message:
            case ChildMessage.Parse(file):
                task = self._t(self._start_parser(file))
                self.t_parser.add(task)
            case ChildMessage.RequestClose():
                self.logger.debug(f'been informed there is no more work')
                self.closing = True
                await self.flush()
            case other:
                self.logger.error(f'unknown message {message}')
                await self.kill()

    def start_ingestion(self: Self) -> None:
        if not self.t_ingest:
            self.t_ingest = self._t(self._ingest())
        else:
            self.logger.warn("attempted to start ingestion while already running")

    async def kill(self: Self):
        for task in self.t_parser:
            task.cancel()
        if self.t_ingest is not None:
            self.t_ingest.cancel()
        self.closing = True
        self.t_parser = set()
        self.t_ingest = None
        await self.q_rows.put(None)

    async def _ingest(self: Self) -> None:
        try:
            while True:
                row = await self._t(self.q_rows.get())

                if row is None:
                    break

                count = await self._t(self.ingestion.queue(row))
                self.p_parent.on_ingest(count)

            count = await self._t(self.ingestion.flush())
            self.p_parent.on_ingest(count)
            self.logger.debug(f'ending ingestion')
        except Exception as e:
            self.logger.error('ingestion failed')
            self.logger.exception(e)
            await self.kill()
            raise e

    async def _start_parser(self: Self, file: PropertySaleDatFileMetaData) -> None:
        try:
            parser = await self._t(self.parser_factory.create_parser(file))
            async for row in parser.get_data_from_file():
                if not isinstance(row, SaleDataFileSummary):
                    self.p_parent.on_parsed()
                await self._t(self.q_rows.put(row))
        except Exception as e:
            self.logger.error('threw while parsing')
            self.logger.exception(e)
            await self.kill()
            raise e
        finally:
            self.t_parser -= { asyncio.current_task() }

    def _t(self: Self, t: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
        return self.tg.create_task(t)

