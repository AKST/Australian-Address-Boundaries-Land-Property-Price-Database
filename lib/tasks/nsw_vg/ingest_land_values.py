import asyncio
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


@dataclass
class NswVgLandValueIngestionConfig2:
    truncate_raw_earlier: bool = field(default=False)

async def ingest_land_values_v2(
    db: DatabaseService,
    io: IoService,
    target: NswVgTarget,
    config: NswVgLandValueIngestionConfig2,
) -> None:
    controller = SchemaController(io, db, SchemaDiscovery.create(io))
    if config.truncate_raw_earlier:
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
