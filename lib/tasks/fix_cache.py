import json
from typing import Dict, List, Tuple
from lib.service.io import IoService

_CACHE_STATE = './_out_state/http-cache.json'
_CACHE_DIR = '_out_cache'

async def fix_cache(io: IoService) -> None:
    """
    For a number reasons it's possible for the cache to
    become kind of broken. Such reasons include:

    1. Running multiple tasks at a time that are writing
       and reading to the cache, and either of them
       recording a new asset (resulting it being orphaned).

    2. The logic for cache could be buggy in some cases.
    """
    state = json.loads(await io.f_read(_CACHE_STATE))

    remove_from_state: List[Tuple[str, str]] = []
    remove_from_disc: List[str] = []

    files_in_state: Dict[str, Tuple[str, str]] = dict()
    files_in_fs = { f async for f in io.grep_dir(_CACHE_DIR, '*') }

    print(type(state['files']))
    for url, formats in state['files'].items():
        for fmt_name, fmt in formats.items():
            path = f'{_CACHE_DIR}/{fmt['location']}'
            if path not in files_in_fs:
                remove_from_state.append((url, fmt_name))
            files_in_state[path] = (url, fmt_name)

    for file in files_in_fs:
        if file not in files_in_state:
            remove_from_disc.append(file)

    for f in remove_from_disc:
        print(f"deleteing '{f}'")
        await io.f_delete(f)

    for url, fmt in remove_from_state:
        print(f'removing {fmt} from {url}')
        del state['files'][url][fmt]
        if not state['files'][url]:
            del state['files'][url]

    if 'files' in state:
        await io.f_write(_CACHE_STATE, json.dumps(state, indent=1))




if __name__ == '__main__':
    import asyncio
    import resource

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    io = IoService.create(file_limit)
    asyncio.run(fix_cache(io))

