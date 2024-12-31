import asyncio
from logging import getLogger, Logger
from os import path
from typing import (
    AsyncIterator,
    List,
    Sequence,
    TypeVar,
)

from lib.service.io import IoService
from lib.service.http import AbstractClientSession, CacheHeader
from .config import Target

CHUNKSIZE_16KB = 16384

_T = TypeVar('_T', bound=Target)

class StaticEnvironmentInitialiser:
    _logger = getLogger(f'{__name__}.StaticEnvironmentInitialiser')

    _io: IoService
    _session: AbstractClientSession
    _targets: List[Target]
    _directories: List[str]

    def __init__(self, targets, dirs, io, session) -> None:
        self._targets = targets
        self._directories = dirs
        self._io = io
        self._session = session

    @staticmethod
    def create(io: IoService, session: AbstractClientSession):
        return StaticEnvironmentInitialiser([], [], io, session)

    def queue_directory(self, directory: str):
        self._directories.append(directory)

    def queue_target(self, target: Target):
        self._targets.append(target)

    async def with_targets(self, targets: Sequence[_T]) -> AsyncIterator[_T]:
        async def install(t: _T) -> _T:
            await self._install_target(t)
            return t

        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(install(t)) for t in targets]
            for target in asyncio.as_completed(tasks):
                yield await target

    async def initalise_environment(self) -> None:
        await asyncio.gather(*map(self._initialise_dir, self._directories))
        await asyncio.gather(*map(self._install_target, self._targets))

    async def _initialise_dir(self, dir_name: str) -> None:
        if not await self._io.is_dir(dir_name):
            await self._io.mk_dir(dir_name)

    async def _install_target(self, target: Target) -> None:
        self._logger.info(f'Checking Target "{target.web_dst}"')
        w_out = '_out_web/%s' % target.web_dst
        z_out = target.zip_dst and '_out_zip/%s' % target.zip_dst

        if not await self._io.is_file(w_out):
            self._logger.info(f'Downloading "{target.url} to {w_out}"')
            headers = { CacheHeader.DISABLED: 'True' }
            if target.token:
                headers['Authorization'] = f'Basic {target.token}'
            async with self._session.get(target.url, headers) as resp:
                await self._io.f_write_chunks(w_out, resp.stream(CHUNKSIZE_16KB))

        if z_out and not await self._io.is_dir(z_out):
            self._logger.info(f'Creating zip output dir "{z_out}"')
            await self._io.mk_dir(z_out)

        if z_out and await self._io.is_directory_empty(z_out):
            self._logger.info(f'Extracting contents into "{z_out}"')
            try:
                await self._io.extract_zip(w_out, z_out)
                await self._recursively_unzip_child_zips(z_out)
            except Exception as e:
                self._logger.error(f'failed to unzip, {w_out} to {z_out}')
                await self._io.f_delete(w_out)
                raise e

    async def _recursively_unzip_child_zips(self, directory: str):
        """
        This walks the directory repeatedly until there are no
        zip files within the directory. While it walks it, it
        unzips any zip file it comes across and then removes it.

        After it walking it recursively calls itself. It will
        terminate once it stops finding zips.
        """
        zip_found = False

        async for root, _, files in self._io.walk_dir(directory):
            for file in files:
                if file.endswith('.zip'):
                    zip_found = True
                    zip_path = path.join(root, file)
                    dst_path = path.join(root, path.splitext(file)[0])
                    await self._io.extract_zip(zip_path, dst_path)
                    await self._io.f_delete(zip_path)

        if zip_found:
            await self._recursively_unzip_child_zips(directory)


