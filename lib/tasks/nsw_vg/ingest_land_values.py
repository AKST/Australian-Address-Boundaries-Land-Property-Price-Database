from dataclasses import dataclass, field
import logging

from lib.pipeline.nsw_vg.land_values.ingest import ingest_raw_files
from lib.pipeline.nsw_vg.land_values.ingest import create_vg_tables_from_raw
from lib.pipeline.nsw_vg.land_values.ingest import parse_property_description
from lib.pipeline.nsw_vg.land_values.ingest import empty_raw_entries
from lib.pipeline.nsw_vg.discovery import NswVgTarget
from lib.service.database import DatabaseService

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

if __name__ == '__main__':
    import argparse
    import asyncio
    import resource

    from lib.service.io import IoService
    from lib.service.database.defaults import instance_1_config, instance_2_config

    from lib.tasks.fetch_static_files import get_session, initialise

    parser = argparse.ArgumentParser(description="Ingest NSW Land values")
    parser.add_argument("--keep-raw", type=bool, default=False)
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    logger = logging.getLogger(f'{__name__}.ingest::init')

    config = NswVgLandValueIngestionConfig(
        keep_raw=args.keep_raw,
    )

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    if args.instance == 1:
        db_conf = instance_1_config
    elif args.instance == 2:
        db_conf = instance_2_config
    else:
        raise ValueError('invalid instance')

    logger.debug(f'file limit {file_limit}')
    logger.debug(f'instance #{args.instance}')
    logger.debug(f'config {db_conf}')

    async def main() -> None:
        db = DatabaseService(db_conf)
        io = IoService.create(file_limit)

        async with get_session(io) as session:
            environment = await initialise(io, session)

        await ingest_land_values(
            config,
            db,
            environment.land_value.latest,
        )

    asyncio.run(main())
