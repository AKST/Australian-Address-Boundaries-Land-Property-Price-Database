from lib.service.clock import ClockService
from lib.service.database import DatabaseService
from lib.service.io import IoService

from .config import GisTaskConfig
from .stage_api_data import stage_gis_api_data
from .ingest_deduplicate import ingest_deduplication

async def ingest(
    io: IoService,
    db: DatabaseService,
    clock: ClockService,
    config: GisTaskConfig.Ingestion,
):
    if config.staging:
        await stage_gis_api_data(
            io,
            db,
            clock,
            config.staging,
        )

    if config.deduplication:
        await ingest_deduplication(
            db,
            io,
            clock,
            config.deduplication,
        )
