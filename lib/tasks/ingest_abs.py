import asyncio

from lib.pipeline.abs.config import ABS_MAIN_STRUCTURES, NON_ABS_MAIN_STRUCTURES
from lib.pipeline.abs.ingest import ingest
from lib.service.database import DatabaseService

_OUTDIR = './_out_zip'

async def ingest_all(db: DatabaseService) -> None:
    """
    TODO make concurrent. Before I can do that I need to
    handle the schema initialisation more gracefully.
    """
    await ingest(db, ABS_MAIN_STRUCTURES, _OUTDIR)
    await ingest(db, NON_ABS_MAIN_STRUCTURES, _OUTDIR)

if __name__ == '__main__':
    import argparse
    import logging
    import resource

    from lib.service.io import IoService
    from lib.service.database.defaults import instance_1_config, instance_2_config

    from .fetch_static_files import get_session, initialise

    parser = argparse.ArgumentParser(description="Initialise nswvg db schema")
    parser.add_argument("--instance", type=int, required=True)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    if args.instance == 1:
        db_conf = instance_1_config
    elif args.instance == 2:
        db_conf = instance_2_config
    else:
        raise ValueError('invalid instance')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    async def main() -> None:
        db = DatabaseService(db_conf)
        io = IoService.create(file_limit)
        async with get_session(io) as session:
            await initialise(io, session)
        await ingest_all(db)

    asyncio.run(main())

