import asyncio
from dataclasses import dataclass, field
import logging

from lib.pipeline.nsw_vg.land_values.ingest import NswVgLandValuesRawIngestion
from lib.pipeline.nsw_vg.discovery import NswVgTarget
from lib.service.database import DatabaseService
from lib.service.io import IoService
from lib.tooling.schema import SchemaController, SchemaDiscovery, Command

from .config import NswVgTaskConfig

_ZIPDIR = './_out_zip'

async def ingest_land_values(
    db: DatabaseService,
    io: IoService,
    target: NswVgTarget,
    config: NswVgTaskConfig.LvIngest,
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
