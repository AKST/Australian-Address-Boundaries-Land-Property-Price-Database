import asyncio


from lib.pipeline.abs.ingest import AbsIngestion
from lib.pipeline.abs.config import *
from lib.service.database import DatabaseService, DatabaseConfig
from lib.service.io import IoService
from lib.tooling.schema import SchemaController, SchemaDiscovery, Command

_OUTDIR = './_out_zip'

async def ingest_all(config: IngestionConfig,
                     db: DatabaseService,
                     io: IoService) -> None:
    """
    TODO make concurrent. Before I can do that I need to
    handle the schema initialisation more gracefully.
    """
    abs_ingestion = AbsIngestion(db, _OUTDIR)

    controller = SchemaController(io, db, SchemaDiscovery.create(io))
    await controller.command(Command.Drop(ns='abs'))
    await controller.command(Command.Create(ns='abs', omit_foreign_keys=True))
    await abs_ingestion.ingest(config)
    async with db.async_connect() as conn:
        # I don't really know what's going on here with dzn...
        await conn.execute("""
            UPDATE abs.dzn as dzn
               SET sa2_code = NULL,
                   state_code = NULL
             WHERE NOT EXISTS (SELECT 1 FROM abs.sa2 as sa2
                                WHERE dzn.sa2_code = sa2.sa2_code);
        """)
    await controller.command(Command.AddForeignKeys(ns='abs'))

async def _main(config: IngestionConfig,
                db_conf: DatabaseConfig,
                file_limit: int) -> None:
        db = DatabaseService.create(db_conf, 32)
        io = IoService.create(file_limit)

        async with get_session(io) as session:
            await initialise(io, session)
        try:
            await db.open()
            await ingest_all(config, db, io)
        finally:
            await db.close()

if __name__ == '__main__':
    import argparse
    import logging
    import resource

    from lib.pipeline.abs.defaults import ABS_MAIN_STRUCTURES, NON_ABS_MAIN_STRUCTURES
    from lib.service.database.defaults import DB_INSTANCE_MAP

    from .fetch_static_files import get_session, initialise

    parser = argparse.ArgumentParser(description="Initialise nswvg db schema")
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--worker-logs", action='store_true', default=False)
    parser.add_argument("--worker-db-connections", type=int, default=1)
    parser.add_argument("--debug", action='store_true', default=False)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    if args.instance < 1 or args.instance > 2:
        raise ValueError('invalid instance')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    worker_log_config = None
    if args.worker_logs:
        worker_log_config = WorkerLogConfig(
            level=logging.DEBUG if args.debug else logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S',
            format=f'[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        )

    db_config = DB_INSTANCE_MAP[args.instance]
    config = IngestionConfig(
        ingest_sources=[ABS_MAIN_STRUCTURES, NON_ABS_MAIN_STRUCTURES],
        worker_count=args.workers,
        worker_config=WorkerConfig(
            db_config=db_config,
            db_connections=args.worker_db_connections,
            log_config=worker_log_config,
        ),
    )

    asyncio.run(_main(config, db_config, file_limit))

