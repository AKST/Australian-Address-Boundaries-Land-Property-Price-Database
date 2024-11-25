from dataclasses import dataclass, field
from typing import List, Literal, Optional
import logging

from lib.service.clock import ClockService
from lib.service.database import DatabaseService, DatabaseConfig
from lib.service.io import IoService
from lib.tooling.schema.controller import SchemaController
from lib.tooling.schema.discovery import SchemaDiscovery
from lib.tooling.schema.type import Command

from ..fetch_static_files import Environment, get_session, initialise
from ..schema.update import update_schema, UpdateSchemaConfig
from .ingest_property_descriptions import ingest_property_description, NswVgLegalDescriptionIngestionConfig
from .ingest_property_sales import PropertySaleIngestionConfig, ingest_property_sales_rows
from .ingest_land_values import NswVgLandValueIngestionConfig, ingest_land_values

ZIP_DIR = './_out_zip'

@dataclass
class NswVgIngestionDedupConfig:
    run_from: int
    run_till: int
    truncate: bool = field(default=False)
    drop_raw: bool = field(default=False)
    drop_dst_schema: bool = field(default=False)

@dataclass
class NswVgIngestionConfig:
    load_raw_land_values: Optional[NswVgLandValueIngestionConfig]
    load_raw_property_sales: Optional[PropertySaleIngestionConfig]
    deduplicate: Optional[NswVgIngestionDedupConfig]
    property_descriptions: Optional[NswVgLegalDescriptionIngestionConfig]

_logger = logging.getLogger(__name__)

async def ingest_nswvg(
    environment: Environment,
    clock: ClockService,
    db: DatabaseService,
    io: IoService,
    config: NswVgIngestionConfig,
):
    if config.load_raw_land_values:
        lv_conf = config.load_raw_land_values
        lv_target = environment.land_value.latest
        await ingest_land_values(db, io, lv_target, lv_conf)

    if config.load_raw_property_sales:
        ps_conf = config.load_raw_property_sales
        await ingest_property_sales_rows(environment, clock, io, ps_conf)

    if config.deduplicate:
        dedup_conf = config.deduplicate
        await ingest_nswvg_deduplicate(db, io, dedup_conf)

    if config.property_descriptions:
        try:
            await ingest_property_description(db, config.property_descriptions)
        except Exception as e:
            _logger.exception(e)
            _logger.error('failed to ingest all property descriptions')

async def ingest_nswvg_deduplicate(
    db: DatabaseService,
    io: IoService,
    config: NswVgIngestionDedupConfig,
):
    logger = logging.getLogger(f'{__name__}.ingest_nswvg_deduplicate')
    scripts = [
        './sql/nsw_vg/tasks/from_raw_derive/001_identifiers.sql',
        './sql/nsw_vg/tasks/from_raw_derive/002_source.sql',
        './sql/nsw_vg/tasks/from_raw_derive/003_property.sql',
        './sql/nsw_vg/tasks/from_raw_derive/004_addresses.sql',
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
    import argparse
    import asyncio
    import os
    import resource
    import sys
    from datetime import date
    from lib.service.database.defaults import DB_INSTANCE_MAP
    from lib.pipeline.nsw_vg.property_sales.ingestion import NSW_VG_PS_INGESTION_CONFIG, PropertySalesIngestion
    from lib.pipeline.nsw_vg.property_sales.orchestration import ParentConfig, ChildConfig, ChildLogConfig

    parser = argparse.ArgumentParser(description="Ingest NSW VG Data")
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--main-debug", action='store_true', default=False)
    parser.add_argument("--main-db-pool-size", type=int, default=8)

    parser.add_argument("--load-land-values", action='store_true', default=False)
    parser.add_argument("--lv-truncate-earlier", action='store_true', default=False)

    parser.add_argument("--load-property-sales", action='store_true', default=False)
    parser.add_argument("--ps-publish-min", type=int, default=None)
    parser.add_argument("--ps-publish-max", type=int, default=None)
    parser.add_argument("--ps-download-min", type=date.fromisoformat, default=None)
    parser.add_argument("--ps-download-max", type=date.fromisoformat, default=None)
    parser.add_argument("--ps-workers", type=int, default=None)
    parser.add_argument("--ps-worker-debug", type=int, default=False)
    parser.add_argument("--ps-worker-file-limit", type=int, default=None)
    parser.add_argument("--ps-worker-db-pool-size", type=int, default=None)
    parser.add_argument("--ps-worker-db-batch-size", type=int, default=50)
    parser.add_argument("--ps-worker-parser-chunk-size", type=int, default=8 * 2 ** 10)

    parser.add_argument("--nswlrs-propdesc", action='store_true', default=False)
    parser.add_argument("--nswlrs-propdesc-workers", type=int, default=1)
    parser.add_argument("--nswlrs-propdesc-subworkers", type=int, default=1)
    parser.add_argument("--nswlrs-propdesc-child-debug", action='store_true', default=False)

    parser.add_argument("--dedup", action='store_true', default=False)
    parser.add_argument("--dedup-reinitialise-destination-schema", action='store_true', default=False)
    parser.add_argument("--dedup-initial-truncate", action='store_true', default=False)
    parser.add_argument("--dedup-drop-raw", action='store_true', default=False)
    parser.add_argument("--dedup-run-from", type=int, default=1)
    parser.add_argument("--dedup-run-till", type=int, default=9)

    parser.add_argument("--load-parcels", action='store_true', default=False)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.main_debug else logging.INFO,
        format='[main][%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.debug(args)

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)
    db_config = DB_INSTANCE_MAP[args.instance]

    load_land_values = None
    if args.load_land_values:
        load_land_values = NswVgLandValueIngestionConfig(
            truncate_raw_earlier=args.lv_truncate_earlier
        )


    load_raw_property_sales = None
    if args.load_property_sales:
        required_args = [
            'ps_workers',
            'ps_worker_db_pool_size'
        ]

        missing_args = [arg for arg in required_args if getattr(args, arg) is None]
        if missing_args:
            print(f"Error: The following arguments are required when --a is provided: {', '.join(missing_args)}")
            sys.exit(1)

        load_raw_property_sales = PropertySaleIngestionConfig(
            worker_count=args.ps_workers,
            worker_config=ChildConfig(
                db_config=DB_INSTANCE_MAP[args.instance],
                db_pool_size=args.ps_worker_db_pool_size,
                db_batch_size=args.ps_worker_db_batch_size,
                file_limit=args.ps_worker_file_limit,
                ingestion_config=NSW_VG_PS_INGESTION_CONFIG,
                parser_chunk_size=args.ps_worker_parser_chunk_size,
                log_config=ChildLogConfig(
                    debug_logs=args.ps_worker_debug,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    format=f'[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s'
                ),
            ),
            parent_config=ParentConfig(
                target_root_dir=ZIP_DIR,
                publish_min=args.ps_publish_min,
                publish_max=args.ps_publish_max,
                download_min=args.ps_download_min,
                download_max=args.ps_download_max,
            ),
        )

    deduplicate = None
    if args.dedup:
        deduplicate = NswVgIngestionDedupConfig(
            run_from=args.dedup_run_from,
            run_till=args.dedup_run_till,
            truncate=args.dedup_initial_truncate,
            drop_raw=args.dedup_drop_raw,
            drop_dst_schema=args.dedup_reinitialise_destination_schema,
        )

    property_description_config = None
    if args.nswlrs_propdesc:
        property_description_config = NswVgLegalDescriptionIngestionConfig(
            db_config=DB_INSTANCE_MAP[args.instance],
            child_debug=args.nswlrs_propdesc_child_debug,
            workers=args.nswlrs_propdesc_workers,
            sub_workers=args.nswlrs_propdesc_subworkers,
        )

    config = NswVgIngestionConfig(
        load_raw_land_values=load_land_values,
        load_raw_property_sales=load_raw_property_sales,
        deduplicate=deduplicate,
        property_descriptions=property_description_config,
    )

    async def _cli_main(
        file_limit: int,
        parent_db_pool_limit: int,
        parent_db_config: DatabaseConfig,
        config: NswVgIngestionConfig,
    ) -> None:
        clock = ClockService()
        io = IoService.create(file_limit)
        db = DatabaseService.create(
            parent_db_config,
            parent_db_pool_limit,
        )

        async with get_session(io) as session:
            environment = await initialise(io, session)

        try:
            await db.open()
            await ingest_nswvg(
                environment,
                clock,
                db,
                io,
                config,
            )
        finally:
            await db.close()

    asyncio.run(_cli_main(
        file_limit,
        args.main_db_pool_size,
        DB_INSTANCE_MAP[args.instance],
        config,
    ))

