from lib.pipeline.gnaf import defaults, ingest, GnafConfig
from lib.service.database import DatabaseService

async def ingest_gnaf(config: GnafConfig, db: DatabaseService) -> None:
    """
    Note while the body of this isn't async (yet), I made the
    function async as a matter of establishing a stable
    interface in which I can add async functionality without
    having to chase up where it's being called (mostly the
    note books).
    """
    ingest(config, db)

if __name__ == '__main__':
    import asyncio
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

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)
    db_conf = DB_INSTANCE_MAP[args.instance]
    gnaf_states = defaults.GNAF_STATE_INSTANCE_MAP[args.instance]

    async def main() -> None:
        db = DatabaseService.create(db_conf, 32)
        io = IoService.create(file_limit)
        async with get_session(io) as session:
            env = await initialise(io, session)

        if env.gnaf.publication is None:
            raise ValueError('no gnaf publication')

        config = GnafConfig(env.gnaf.publication, gnaf_states)
        await ingest_gnaf(config, db)

    asyncio.run(main())
