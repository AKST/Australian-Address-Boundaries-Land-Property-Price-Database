from lib.abs.config import ABS_MAIN_STRUCTURES, NON_ABS_MAIN_STRUCTURES
from lib.abs.ingest import ingest_sync
from lib.gnaf_db import GnafDb

async def ingest(gnaf_db: GnafDb) -> None:
    ingest_sync(gnaf_db, ABS_MAIN_STRUCTURES)
    ingest_sync(gnaf_db, NON_ABS_MAIN_STRUCTURES)

if __name__ == '__main__':
    import asyncio
    import logging
    import resource

    from lib.service.io import IoService

    from .fetch_static_files import get_session, initialise

    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    gnaf_db = GnafDb.create()

    async def main() -> None:
        io_service = IoService.create(file_limit)
        async with get_session(io_service) as session:
            await initialise(io_service, session)
        await ingest(gnaf_db)

    asyncio.run(main())

