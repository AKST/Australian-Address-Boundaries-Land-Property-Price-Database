import asyncio
from typing import Self, AsyncIterator, Sequence

from lib.pipeline.nsw_vg.discovery import NswVgTarget, LandValueDiscovery
from lib.service.io import IoService
from lib.service.static_environment import StaticEnvironmentInitialiser

from ..discovery import NswVgPublicationDiscovery, NSWVG_LV_DISCOVERY_CFG
from .config import NswVgLvTaskDesc

class NswVgLvCsvDiscovery:
    def __init__(self: Self,
                 root_dir: str,
                 io: IoService,
                 web_discovery: NswVgPublicationDiscovery,
                 static_environment: StaticEnvironmentInitialiser) -> None:
        self.root_dir = root_dir
        self._io = io
        self._web_discovery = web_discovery
        self._env = static_environment

    async def files(self: Self, target_ls: Sequence[NswVgTarget]) -> AsyncIterator[NswVgLvTaskDesc.Parse]:
        targets = await self._web_discovery.load_links(NSWVG_LV_DISCOVERY_CFG)
        async for target in self._env.with_targets(targets):
            root = f'{self.root_dir}/{target.zip_dst}'
            for f in sorted(await self._io.ls_dir(root)):
                file_path = f'{self.root_dir}/{target.zip_dst}/{f}'
                yield NswVgLvTaskDesc.Parse(file_path, target)

