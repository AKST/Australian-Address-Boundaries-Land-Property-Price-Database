import asyncio

from lib.pipeline.abs.config import ABS_MAIN_STRUCTURES, NON_ABS_MAIN_STRUCTURES
from lib.pipeline.abs.ingest import ingest
from lib.service.database import DatabaseService, DatabaseConfig

_OUTDIR = './_out_zip'

async def ingest_all(db: DatabaseService) -> None:
    """
    TODO make concurrent. Before I can do that I need to
    handle the schema initialisation more gracefully.
    """
    await ingest(db, ABS_MAIN_STRUCTURES, _OUTDIR)
    await ingest(db, NON_ABS_MAIN_STRUCTURES, _OUTDIR)

async def _main(db_conf: DatabaseConfig,
                file_limit: int) -> None:
        db = DatabaseService.create(db_conf, 32)
        io = IoService.create(file_limit)
        async with get_session(io) as session:
            await initialise(io, session)
        await ingest_all(db)

if __name__ == '__main__':
    import argparse
    import logging
    import resource

    from lib.service.io import IoService
    from lib.service.database.defaults import DB_INSTANCE_MAP

    from .fetch_static_files import get_session, initialise

    parser = argparse.ArgumentParser(description="Initialise nswvg db schema")
    parser.add_argument("--instance", type=int, required=True)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    if args.instance < 1 or args.instance > 2:
        raise ValueError('invalid instance')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)
    asyncio.run(_main(DB_INSTANCE_MAP[args.instance], file_limit))

