import asyncio
from dataclasses import dataclass
from typing import Self, AsyncIterator, Sequence, Literal

from lib.pipeline.nsw_vg.discovery import NswVgTarget, LandValueDiscovery
from lib.service.io import IoService
from lib.service.static_environment import StaticEnvironmentInitialiser

from ..discovery import NswVgPublicationDiscovery, NSWVG_LV_DISCOVERY_CFG
from .config import NswVgLvTaskDesc
from .telemetry import NswVgLvTelemetry

DiscoveryMode = Literal['latest', 'all']

@dataclass(frozen=True)
class Config:
    kind: DiscoveryMode
    root_dir: str


class CsvDiscovery:
    def __init__(self: Self,
                 config: Config,
                 io: IoService,
                 telemetry: NswVgLvTelemetry,
                 web_discovery: NswVgPublicationDiscovery,
                 static_environment: StaticEnvironmentInitialiser) -> None:
        self.config = config
        self._io = io
        self._telemetry = telemetry
        self._web_discovery = web_discovery
        self._env = static_environment

    async def files(self: Self) -> AsyncIterator[NswVgLvTaskDesc.Parse]:
        targets = await self._web_discovery.load_links(NSWVG_LV_DISCOVERY_CFG)
        targets = targets[-1:] if self.config.kind == 'latest' else targets
        async for target in self._env.with_targets(targets):
            root = f'{self.config.root_dir}/{target.zip_dst}'
            for f in sorted(await self._io.ls_dir(root)):
                if not f.endswith("csv"):
                    continue

                f_path = f'{self.config.root_dir}/{target.zip_dst}/{f}'
                f_size = await self._io.f_size(f_path)
                self._telemetry.record_file_queue(f_path, f_size)
                yield NswVgLvTaskDesc.Parse(f_path, f_size, target)

