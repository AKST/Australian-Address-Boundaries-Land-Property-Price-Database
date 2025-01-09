from typing import List, Self, Tuple

from lib.service.io import IoService
from .config import Config, WorkerConfig, WorkerTask

class Scheduler:
    def __init__(self: Self, io: IoService):
        self._io = io

    async def get_tasks(self: Self, cfg: Config) -> List[List[WorkerTask]]:
        return [
            [WorkerTask(fn, _get_table_name(fn)) for fn in file_group]
            for file_group in await self._get_grouped_files(cfg)
        ]

    async def _get_grouped_files(self: Self, cfg: Config) -> List[List[str]]:
        authority_files = await _get_authority_files(cfg, self._io)
        standard_files = _get_standard_files(cfg)
        return _group_by_size([
            (await self._io.f_size(file), file)
            for file in [*authority_files, *standard_files]
        ], cfg.workers)

async def _get_authority_files(cfg: Config, io: IoService) -> List[str]:
    return [f async for f in io.grep_dir(
        dir_name=f'{cfg.target.psv_dir}/Authority Code',
        pattern='*.psv',
    )]

def _get_standard_files(cfg: Config) -> List[str]:
    standard_prefix = f'{cfg.target.psv_dir}/Standard'
    return [
        f'{standard_prefix}/{s}_{t}_psv.psv'
        for t in [
            'STATE', 'ADDRESS_SITE', 'MB_2016', 'MB_2021', 'LOCALITY',
            'LOCALITY_ALIAS', 'LOCALITY_NEIGHBOUR', 'LOCALITY_POINT',
            'STREET_LOCALITY', 'STREET_LOCALITY_ALIAS', 'STREET_LOCALITY_POINT',
            'ADDRESS_DETAIL', 'ADDRESS_SITE_GEOCODE', 'ADDRESS_ALIAS',
            'ADDRESS_DEFAULT_GEOCODE', 'ADDRESS_FEATURE',
            'ADDRESS_MESH_BLOCK_2016', 'ADDRESS_MESH_BLOCK_2021',
            'PRIMARY_SECONDARY',
        ]
        for s in cfg.states
    ]

def _get_table_name(file: str) -> str:
    import os

    file = os.path.splitext(os.path.basename(file))[0]
    sidx = 15 if file.startswith('Authority_Code') else file.find('_')+1
    return file[sidx:file.rfind('_')]

def _group_by_size(items: List[Tuple[int, str]], n: int) -> List[List[str]]:
    sorted_items = sorted(items, key=lambda x: x[0], reverse=True)

    groups: List[List[str]] = [[] for _ in range(n)]
    group_sums = [0] * n  # total weight in each group

    for weight, name in sorted_items:
        min_index = min(range(n), key=lambda i: group_sums[i])
        groups[min_index].append(name)
        group_sums[min_index] += weight

    return groups
