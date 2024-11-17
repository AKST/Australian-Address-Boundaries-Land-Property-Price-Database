from dataclasses import dataclass, field
from lib.pipeline.nsw_vg.property_description.ingest import process_property_description
from lib.service.database import DatabaseService, DatabaseConfig
from lib.service.io import IoService

@dataclass
class NswVgLegalDescriptionIngestionConfig:
    workers: int

async def ingest_property_description(
        db: DatabaseService,
        io: IoService,
        config: NswVgLegalDescriptionIngestionConfig) -> None:
    await process_property_description(db, io, workers=config.workers)

async def cli_main(db_config: DatabaseConfig,
                   file_limit: int,
                   config: NswVgLegalDescriptionIngestionConfig) -> None:
    io = IoService.create(file_limit - config.workers)
    db_service = DatabaseService.create(db_config, config.workers)
    await ingest_property_description(db_service, io, config)


if __name__ == '__main__':
    import argparse
    import asyncio
    import logging
    import resource
    from lib.service.database.defaults import DB_INSTANCE_MAP

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (soft_limit, hard_limit))

    file_limit = int(soft_limit * 0.8)
    asyncio.run(cli_main(
        DB_INSTANCE_MAP[args.instance],
        file_limit,
        NswVgLegalDescriptionIngestionConfig(
            workers=args.workers,
        ),
    ))


