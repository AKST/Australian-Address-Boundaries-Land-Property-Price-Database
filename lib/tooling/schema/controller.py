from logging import getLogger
from typing import List, Set, Self, Type

from lib.service.io import IoService
from lib.service.database import DatabaseService

from lib.tooling.schema import codegen
from .config import schema_ns
from .discovery import SchemaDiscovery
from .type import *

class SchemaController:
    _logger = getLogger(f'{__name__}.SchemaController')
    _io: IoService
    _db: DatabaseService
    _discovery: SchemaDiscovery

    def __init__(self: Self,
                 io: IoService,
                 db: DatabaseService,
                 discovery: SchemaDiscovery) -> None:
        self._db = db
        self._io = io
        self._discovery = discovery

    async def command(self: Self, command: Command.BaseCommand) -> None:
        self._logger.info(command)
        match command:
            case Command.Create() as command:
                await self.create(command)
            case Command.Drop() as command:
                await self.drop(command)
            case Command.Truncate() as command:
                await self.truncate(command)
            case Command.AddForeignKeys() as command:
                await self.add_foreign_keys(command)
            case Command.RemoveForeignKeys() as command:
                await self.remove_foreign_keys(command)
            case other:
                raise TypeError(f'unknown command {other}')

    async def create(self: Self, command: Command.Create) -> None:
        file_list = await self._discovery.files(command.ns, command.range, load_syn=True)

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            for file in file_list:
                if file.contents is None:
                    raise TypeError()

                for operation in codegen.create(
                    file.contents,
                    command.omit_foreign_keys,
                ):
                    self._logger.debug(operation)
                    try:
                        await cursor.execute(operation)
                    except Exception as e:
                        self._logger.error(f"failed on {operation}")
                        raise e

    async def drop(self: Self, command: Command.Drop) -> None:
        file_list = await self._discovery.files(command.ns, command.range, load_syn=True)

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            for file in reversed(file_list):
                if file.contents is None:
                    raise TypeError()

                for operation in codegen.drop(file.contents, command.cascade):
                    self._logger.debug(operation)
                    await cursor.execute(operation)

    async def truncate(self: Self, command: Command.Truncate) -> None:
        file_list = await self._discovery.files(command.ns, command.range, load_syn=True)

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            for file in reversed(file_list):
                if file.contents is None:
                    raise TypeError()

                for operation in codegen.truncate(file.contents, command.cascade):
                    self._logger.debug(operation)
                    await cursor.execute(operation)

    async def add_foreign_keys(self: Self, command: Command.AddForeignKeys) -> None:
        file_list = await self._discovery.files(command.ns, command.range, load_syn=True)

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            for file in file_list:
                if file.contents is None:
                    raise TypeError()

                for operation in codegen.add_foreign_keys(file.contents):
                    self._logger.debug(operation)
                    await cursor.execute(operation)

    async def remove_foreign_keys(self: Self, command: Command.RemoveForeignKeys) -> None:
        file_list = await self._discovery.files(command.ns, command.range, load_syn=True)

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            for file in file_list:
                if file.contents is None:
                    raise TypeError()

                fk_map = await codegen.make_fk_map(file.contents, cursor)
                for operation in codegen.remove_foreign_keys(file.contents, fk_map):
                    self._logger.debug(operation)
                    await cursor.execute(operation)

