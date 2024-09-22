from collections import defaultdict, deque
import glob
import os
from threading import Lock
from typing import List, Dict, Set

from .discovery import GnafPublicationTarget


DepMap = Dict[str, Set[str]]


def create_list_of_target_files(target: GnafPublicationTarget) -> Set[str]:
    authority_files = glob.glob(f'{target.psv_dir}/Authority Code/*.psv')
    standard_prefix = f'{target.psv_dir}/Standard'
    standard_files = [
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
        for s in ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'OT', 'ACT']
    ]
    return { *authority_files, *standard_files }


class GnafDataIngestionScheduler:
    """
    You could actually remove 90% of the complexity here
    if you just never added the foreign keys. Infact it
    may even be worthwhile doing that.

    My only reason for not doing that is laziliness tbh,
    when I first worked on this I never realised I could
    have just done that (I was abit rusty on databases),
    but I guess also because I wanted to make sure I was
    populating the database correctly and the foreigns
    helped me pick up when I stuffed up much earlier.
    """
    _lock: Lock
    _queue: deque[str]
    _all_files: Set[str]
    _dependencies: DepMap
    _dependencies_completed: Set[str]

    def __init__(self, lock, queue, all_files, dependencies):
        self._lock = lock
        self._queue = queue
        self._all_files = all_files
        self._dependencies = dependencies
        self._dependencies_completed = set()

    def iter(self):
        while True:
            with self._lock:
                if not self._queue:
                    break
                file = self._queue.popleft()

            yield file

            with self._lock:
                self._dependencies_completed.add(file)

                for d in self._dependencies:
                    if file in self._dependencies[d]:
                        self._dependencies[d].remove(file)

                ready_files = [f for f in self._all_files if not self._dependencies[f]]

                for f in ready_files:
                    if f not in self._queue:
                        self._queue.append(f)
                        self._all_files.remove(f)

    @staticmethod
    def create(target: GnafPublicationTarget):
        files = create_list_of_target_files(target)
        queue: deque[str] = deque()
        deps = { k: set(v) for k, v in create_dependency_graph(target).items() }
        lock = Lock()
        queue.extend((
            f
            for f in _get_ordered_files(deps, files)
            if not deps[f]
        ))

        return GnafDataIngestionScheduler(lock, queue, files, deps)

def create_dependency_graph(target: GnafPublicationTarget) -> Dict[str, Set[str]]:
    standard_prefix = f'{target.psv_dir}/Standard'
    authority_files = glob.glob(f'{target.psv_dir}/Authority Code/*.psv')

    return { f: set() for f in authority_files } | {
        f'{p}/{s}_{t}_psv.psv': { f'{p}/{s}_{d}_psv.psv' for d in ds } | set(authority_files)

        for t, ds in ({
            'STATE': [],
            'ADDRESS_SITE': ['STATE'],
            'MB_2016': [],
            'MB_2021': [],
            'LOCALITY': ['STATE'],
            'LOCALITY_ALIAS': ['LOCALITY'],
            'LOCALITY_NEIGHBOUR': ['LOCALITY'],
            'LOCALITY_POINT': ['LOCALITY'],
            'STREET_LOCALITY': ['LOCALITY'],
            'STREET_LOCALITY_ALIAS': ['STREET_LOCALITY'],
            'STREET_LOCALITY_POINT': ['STREET_LOCALITY'],
            'ADDRESS_DETAIL': ['ADDRESS_SITE', 'STATE', 'LOCALITY', 'STREET_LOCALITY'],
            'ADDRESS_SITE_GEOCODE': ['ADDRESS_SITE'],
            'ADDRESS_ALIAS': ['ADDRESS_DETAIL'],
            'ADDRESS_DEFAULT_GEOCODE': ['ADDRESS_DETAIL'],
            'ADDRESS_FEATURE': ['ADDRESS_DETAIL'],
            'ADDRESS_MESH_BLOCK_2016': ['ADDRESS_DETAIL', 'MB_2016'],
            'ADDRESS_MESH_BLOCK_2021': ['ADDRESS_DETAIL', 'MB_2021'],
            'PRIMARY_SECONDARY': ['ADDRESS_DETAIL'],
        }).items()
        for s in ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'OT', 'ACT']
        for p in [standard_prefix]
    }


def _get_ordered_files(dependencies: DepMap, all_files: Set[str]) -> List[str]:
    file_sizes = { file: os.path.getsize(file) for file in all_files }
    total_blocked_sizes = defaultdict(int)

    def dfs(file: str, visited: Set[str]):
        if file in visited:
            return 0
        visited.add(file)
        total_size = file_sizes[file]
        for dep in dependencies[file]:
            total_size += dfs(dep, visited)
        return total_size

    for file in dependencies:
        visited: Set[str] = set()
        total_blocked_sizes[file] = dfs(file, visited)

    # Sort files based on the total blocked sizes, with higher sizes first
    return sorted(all_files, key=lambda f: total_blocked_sizes[f], reverse=True)


