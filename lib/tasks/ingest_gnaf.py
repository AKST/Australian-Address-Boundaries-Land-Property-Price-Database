from lib.pipeline.gnaf.ingestion import ingest
from lib.pipeline.gnaf.discovery import GnafPublicationTarget
from lib.service.database import DatabaseService

async def ingest_gnaf(target: GnafPublicationTarget, db: DatabaseService) -> None:
    """
    Note while the body of this isn't async (yet), I made the
    function async as a matter of establishing a stable
    interface in which I can add async functionality without
    having to chase up where it's being called (mostly the
    note books).
    """
    ingest(target, db)

if __name__ == '__main__':
    import asyncio
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
            env = await initialise(io, session)

        if env.gnaf.publication is None:
            raise ValueError('no gnaf publication')

        await ingest_gnaf(env.gnaf.publication, db)

    asyncio.run(main())
