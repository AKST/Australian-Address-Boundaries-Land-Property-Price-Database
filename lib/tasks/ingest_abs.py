import asyncio


from lib.pipeline.abs import *
from lib.service.database import DatabaseService, DatabaseConfig
from lib.service.io import IoService
from lib.tooling.schema import SchemaController, SchemaDiscovery, Command

_OUTDIR = './_out_zip'

async def ingest_all(config: AbsIngestionConfig,
                     db: DatabaseService,
                     io: IoService) -> None:
    """
    TODO make concurrent. Before I can do that I need to
    handle the schema initialisation more gracefully.
    """
    abs_ingestion = AbsIngestionSupervisor(db, _OUTDIR)

    controller = SchemaController(io, db, SchemaDiscovery.create(io))
    await controller.command(Command.Drop(ns='abs'))
    await controller.command(Command.Create(ns='abs', omit_foreign_keys=True))
    await abs_ingestion.ingest(config)
    async with db.async_connect() as conn:
        clean_dzn_sql = await io.f_read('./sql/abs/tasks/clean_dzn_post_ingestion.sql')
        await conn.execute(clean_dzn_sql)
    await controller.command(Command.AddForeignKeys(ns='abs'))

async def _main(config: AbsIngestionConfig,
                db_conf: DatabaseConfig,
                file_limit: int) -> None:
        db = DatabaseService.create(db_conf, 32)
        io = IoService.create(file_limit)

        async with get_session(io, 'env-abs-cli') as session:
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

    from lib.defaults import INSTANCE_CFG

    from .fetch_static_files import get_session, initialise

    parser = argparse.ArgumentParser(description="Initialise nswvg db schema")
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--worker-logs", action='store_true', default=False)
    parser.add_argument("--worker-db-connections", type=int, default=8)
    parser.add_argument("--debug", action='store_true', default=False)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    db_config = INSTANCE_CFG[args.instance].database
    config = AbsIngestionConfig(
        ingest_sources=[
            ABS_MAIN_STRUCTURES,
            NON_ABS_MAIN_STRUCTURES,
            INDIGENOUS_STRUCTURES,
        ],
        worker_count=args.workers,
        worker_config=AbsWorkerConfig(
            db_config=db_config,
            db_connections=args.worker_db_connections,
            enable_logging=args.worker_logs,
            enable_logging_debug=args.debug,
        ),
    )

    asyncio.run(_main(config, db_config, file_limit))

