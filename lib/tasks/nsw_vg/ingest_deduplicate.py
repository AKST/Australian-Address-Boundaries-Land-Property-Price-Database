import logging
from typing import List

from lib.service.database import DatabaseService, DatabaseConfig
from lib.service.io import IoService
from lib.tooling.schema.controller import SchemaController
from lib.tooling.schema.discovery import SchemaDiscovery
from lib.tooling.schema.type import Command

from .config import NswVgTaskConfig

async def ingest_deduplicate(
    db: DatabaseService,
    io: IoService,
    config: NswVgTaskConfig.Dedup,
):
    logger = logging.getLogger(f'{__name__}.ingest_nswvg_deduplicate')
    scripts = [
        './sql/nsw_vg/tasks/from_raw_derive/001_identifiers.sql',
        './sql/nsw_vg/tasks/from_raw_derive/002_source.sql',
        './sql/nsw_vg/tasks/from_raw_derive/003_property.sql',
        './sql/nsw_vg/tasks/from_raw_derive/004_addresses/001_from_land_values.sql',
        './sql/nsw_vg/tasks/from_raw_derive/004_addresses/002_from_psi.sql',
        './sql/nsw_vg/tasks/from_raw_derive/004_addresses/003_from_psi_archive.sql',
        './sql/nsw_vg/tasks/from_raw_derive/004_addresses/004_rematerialize.sql',
        './sql/nsw_vg/tasks/from_raw_derive/005_populate_lrs/001_setup.sql',
        './sql/nsw_vg/tasks/from_raw_derive/005_populate_lrs/002_ingest_land_values.sql',
        './sql/nsw_vg/tasks/from_raw_derive/005_populate_lrs/003_ingest_psi_post_2001.sql',
        './sql/nsw_vg/tasks/from_raw_derive/005_populate_lrs/004_ingest_psi_pre_2001.sql',
        './sql/nsw_vg/tasks/from_raw_derive/005_populate_lrs/005_cleanup.sql',
    ]

    if 1 > config.run_from or len(scripts) < config.run_from:
        raise ValueError(f'dedup run from {config.run_from} is out of scope')
    else:
        scripts = scripts[config.run_from - 1:config.run_till]

    discovery = SchemaDiscovery.create(io)
    controller = SchemaController(io, db, discovery)

    async def run_commands(commands: List[Command.BaseCommand]):
        for c in commands:
            await controller.command(c)

    if config.truncate:
        await run_commands([
            Command.Truncate(ns='nsw_vg', cascade=True, range=range(4, 5)),
            Command.Truncate(ns='nsw_gnb', cascade=True),
            Command.Truncate(ns='nsw_lrs', cascade=True),
            Command.Truncate(ns='nsw_planning', cascade=True),
            Command.Truncate(ns='meta', cascade=True),
        ])

    if config.drop_dst_schema:
        await run_commands([
            Command.Drop(ns='nsw_vg', range=range(4, 5)),
            Command.Drop(ns='nsw_gnb'),
            Command.Drop(ns='nsw_lrs'),
            Command.Drop(ns='nsw_planning'),
            Command.Drop(ns='meta'),
            Command.Create(ns='meta'),
            Command.Create(ns='nsw_planning'),
            Command.Create(ns='nsw_lrs'),
            Command.Create(ns='nsw_gnb'),
            Command.Create(ns='nsw_vg', range=range(4, 5)),
        ])

    async with db.async_connect() as c, c.cursor() as cursor:
        for script_path in scripts:
            logger.info(f'running {script_path}')
            await cursor.execute(await io.f_read(script_path))

    logger.info('finished deduplicating')

    if config.drop_raw:
        raise NotImplementedError()

if __name__ == '__main__':
    import asyncio
    import argparse

    from lib.service.database.defaults import DB_INSTANCE_MAP

    parser = argparse.ArgumentParser(description="Ingest NSW VG Data")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--db-pool-size", type=int, default=8)
    parser.add_argument("--reinitialise-destination-schema", action='store_true', default=False)
    parser.add_argument("--initial-truncate", action='store_true', default=False)
    parser.add_argument("--drop-raw", action='store_true', default=False)
    parser.add_argument("--run-from", type=int, default=1)
    parser.add_argument("--run-till", type=int, default=12)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[main][%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.debug(args)
    db_config = DB_INSTANCE_MAP[args.instance]
    config = NswVgTaskConfig.Dedup(
        run_from=args.run_from,
        run_till=args.run_till,
        truncate=args.initial_truncate,
        drop_raw=args.drop_raw,
        drop_dst_schema=args.reinitialise_destination_schema,
    )

    async def _cli_main() -> None:
        io = IoService.create(None)
        db = DatabaseService.create(
            db_config,
            args.db_pool_size,
        )
        try:
            await db.open()
            await ingest_deduplicate(db, io, config)
        finally:
            await db.close()

    asyncio.run(_cli_main())

