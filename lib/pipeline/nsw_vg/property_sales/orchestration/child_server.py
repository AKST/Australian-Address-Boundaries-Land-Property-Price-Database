import abc
import asyncio
from logging import getLogger
import multiprocessing
from typing import Any, Coroutine, Self, Set, TypeVar

from ..file_format import PropertySalesRowParserFactory
from ..data import BasePropertySaleFileRow, PropertySaleDatFileMetaData
from ..ingestion import PropertySalesIngestion
from .config import ChildMessage

T = TypeVar("T")

class NswVgPsChildServer:
    """
    This instance is created on a child process
    """
    logger = getLogger(f'{__name__}.NswVgPsChildServer')
    tg: asyncio.TaskGroup

    q_rows: asyncio.Queue[BasePropertySaleFileRow | None]
    t_ingest: asyncio.Task | None
    t_parser: Set[asyncio.Task]
    done: bool = False

    parser_factory: PropertySalesRowParserFactory
    ingestion: PropertySalesIngestion

    def __init__(self,
                 tg: asyncio.TaskGroup,
                 ingestion: PropertySalesIngestion,
                 parser_factory: PropertySalesRowParserFactory) -> None:
        self.tg = tg
        self.q_rows = asyncio.Queue()
        self.t_parser = set()
        self.parser_factory = parser_factory
        self.ingestion = ingestion

    async def on_message(self: Self, message: ChildMessage.Message) -> None:
        self.logger.debug(f'Recevied {message}')
        match message:
            case ChildMessage.Parse(file):
                task = self._t(self.parser_factory.create_parser(file))
                self.t_parser.add(task)
            case ChildMessage.NoMoreWork():
                self.done = True
                await self._signal_done_if_idle()
            case ChildMessage.Kill():
                await self.kill()
            case other:
                self.logger.error(f'unknown message {message}')
                await self.kill()

    async def start_ingestion(self: Self) -> None:
        if not self.t_ingest:
            self.t_ingest = self._t(self._ingest())
        else:
            self.logger.warn("attempted to start ingestion while already running")

    async def kill(self: Self):
        for task in self.t_parser:
            task.cancel()
        if self.t_ingest is not None:
            self.t_ingest.cancel()
        self.done = True
        self.t_parser = set()
        self.t_ingest = None
        await self.q_rows.put(None)

    async def _ingest(self: Self) -> None:
        while self.t_parser or not self.done:
            row = await self._t(self.q_rows.get())
            if row is None:
                break

            await self._t(self.ingestion.queue(row))
        await self._t(self.ingestion.flush())

    async def _start_parser(self: Self, file: PropertySaleDatFileMetaData) -> None:
        try:
            parser = await self._t(self.parser_factory.create_parser(file))
            async for row in parser.get_data_from_file():
                await self._t(self.q_rows.put(row))
        except Exception as e:
            self.logger.error('threw while parsing')
            self.logger.exception(e)
            await self.kill()
            raise e
        finally:
            self.t_parser -= { asyncio.current_task() }

        await self._signal_done_if_idle()

    async def _signal_done_if_idle(self: Self):
        if not self.t_parser and self.q_rows.empty():
            await self.q_rows.put(None)

    def _t(self: Self, t: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
        return self.tg.create_task(t)

