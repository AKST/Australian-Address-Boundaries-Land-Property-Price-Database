from dataclasses import dataclass, field
import logging

from lib.pipeline.nsw_vg.land_values_legacy.ingest import ingest_raw_files
from lib.pipeline.nsw_vg.land_values_legacy.ingest import create_vg_tables_from_raw
from lib.pipeline.nsw_vg.land_values_legacy.ingest import parse_property_description
from lib.pipeline.nsw_vg.land_values_legacy.ingest import empty_raw_entries
from lib.pipeline.nsw_vg.land_values.ingest import NswVgLandValuesRawIngestion
from lib.pipeline.nsw_vg.discovery import NswVgTarget
from lib.service.database import DatabaseService
from lib.service.io import IoService
from lib.tooling.schema import SchemaController, SchemaDiscovery, Command

_ZIPDIR = './_out_zip'

@dataclass
class NswVgLandValueIngestionConfig:
    keep_raw: bool = field(default=False)

async def ingest_land_values(
    config: NswVgLandValueIngestionConfig,
    db: DatabaseService,
    target: NswVgTarget,
) -> None:
    logger = logging.getLogger(f'{__name__}.ingest')

    logger.info('Step 1: Ingest raw files')
    await ingest_raw_files(db, target, read_dir=_ZIPDIR)

    logger.info('Step 2: Create Valuer General Tables')
    await create_vg_tables_from_raw(db)

    logger.info('Step 3: Parse Property Descriptions')
    await parse_property_description(db)

    logger.info('Step 4: Clean up')
    if config.keep_raw:
        logger.info("Keeping raw entries table")
    else:
        logger.info("Dropping raw entries table")
        await empty_raw_entries(db)

async def ingest_land_values_v2(
    db: DatabaseService,
    io: IoService,
    target: NswVgTarget,
) -> None:
    controller = SchemaController(io, db, SchemaDiscovery.create(io))
    await controller.command(Command.Drop(ns='nsw_vg', range=range(2, 3)))
    await controller.command(Command.Create(ns='nsw_vg', range=range(2, 3)))
    async with asyncio.TaskGroup() as tg:
        ingest = NswVgLandValuesRawIngestion(
            asyncio.Semaphore(db.pool_size),
            _ZIPDIR,
            1000,
            io,
            db,
            tg,
        )
        await ingest.ingest_raw_target(target)

if __name__ == '__main__':
    import argparse
    import asyncio
    import resource

    from lib.service.database.defaults import DB_INSTANCE_MAP

    from lib.tasks.fetch_static_files import get_session, initialise

    parser = argparse.ArgumentParser(description="Ingest NSW Land values")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--file-limit", type=int, default=None)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--db-pool-size", type=int, default=16)
    command = parser.add_subparsers(dest='command')

    v1_parser = command.add_parser('v1')
    v1_parser.add_argument("--keep-raw", type=bool, default=False)

    v1_parser = command.add_parser('v2')

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    logger = logging.getLogger(f'{__name__}.ingest::init')

    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if args.file_limit:
        soft_limit = min(args.file_limit, hard_limit)

    resource.setrlimit(resource.RLIMIT_NOFILE, (soft_limit, hard_limit))

    file_limit = int(soft_limit * 0.8)
    db_conf = DB_INSTANCE_MAP[args.instance]

    logger.debug(f'file limit {file_limit}')
    logger.debug(f'instance #{args.instance}')
    logger.debug(f'config {db_conf}')

    async def main() -> None:
        db = DatabaseService.create(db_conf, args.db_pool_size)
        io = IoService.create(file_limit - args.db_pool_size)

        async with get_session(io) as session:
            environment = await initialise(io, session)

        try:
            await db.open()
            match args.command:
                case 'v1':
                    await ingest_land_values(
                        NswVgLandValueIngestionConfig(
                            keep_raw=args.keep_raw,
                        ),
                        db,
                        environment.land_value.latest,
                    )
                case 'v2':
                    await ingest_land_values_v2(
                        db,
                        io,
                        environment.land_value.latest,
                    )
        finally:
            await db.close()

    asyncio.run(main())
