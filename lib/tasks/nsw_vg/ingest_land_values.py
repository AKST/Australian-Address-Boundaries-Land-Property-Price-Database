from dataclasses import dataclass, field
import logging

from lib.gnaf_db import GnafDb
from lib.nsw_vg.land_values.ingest import ingest_raw_files
from lib.nsw_vg.land_values.ingest import create_vg_tables_from_raw
from lib.nsw_vg.land_values.ingest import parse_property_description
from lib.nsw_vg.land_values.ingest import empty_raw_entries
from lib.nsw_vg.discovery import NswVgTarget


_ZIPDIR = './_out_zip'

@dataclass
class NswVgLandValueIngestionConfig:
    keep_raw: bool = field(default=False)

async def ingest_land_values(
    config: NswVgLandValueIngestionConfig,
    gnaf_db: GnafDb,
    target: NswVgTarget,
) -> None:
    """
    Note while the body of this isn't async (yet), I made the
    function async as a matter of establishing a stable
    interface in which I can add async functionality without
    having to chase up where it's being called (mostly the
    note books).
    """
    logger = logging.getLogger(f'{__name__}.ingest')

    logger.info('Step 1: Ingest raw files')
    ingest_raw_files(gnaf_db, target, read_dir=_ZIPDIR)

    logger.info('Step 2: Create Valuer General Tables')
    create_vg_tables_from_raw(gnaf_db)

    logger.info('Step 3: Parse Property Descriptions')
    parse_property_description(gnaf_db)

    logger.info('Step 4: Clean up')
    if config.keep_raw:
        logger.info("Keeping raw entries table")
    else:
        logger.info("Dropping raw entries table")
        empty_raw_entries(gnaf_db)

if __name__ == '__main__':
    import argparse
    import asyncio
    import resource

    from lib import notebook_constants as nc
    from lib.service.io import IoService

    from lib.tasks.fetch_static_files import get_session, initialise

    parser = argparse.ArgumentParser(description="Ingest NSW Land values")
    parser.add_argument("--keep-raw", type=bool, default=False)
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, default=1)

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
        db_conf, db_name = nc.gnaf_dbconf, nc.gnaf_dbname
    elif args.instance == 2:
        db_conf, db_name = nc.gnaf_dbconf_2, nc.gnaf_dbname_2
    else:
        raise ValueError('invalid instance')

    logger.debug(f'file limit {file_limit}')
    logger.debug(f'instance #{args.instance}')

    async def main() -> None:
        gnaf_db = GnafDb.create(db_conf, db_name)
        io_service = IoService.create(file_limit)

        async with get_session(io_service) as session:
            environment = await initialise(io_service, session)

        await ingest_land_values(
            config,
            gnaf_db,
            environment.land_value.latest,
        )

    asyncio.run(main())
