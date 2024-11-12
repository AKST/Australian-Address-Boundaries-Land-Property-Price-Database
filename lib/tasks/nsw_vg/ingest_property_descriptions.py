from lib.pipeline.nsw_vg.property_description.ingest import process_property_description
from lib.service.database import DatabaseService, DatabaseConfig

async def ingest_property_description(db: DatabaseService) -> None:
    await process_property_description(db)

async def cli_main(config: DatabaseConfig) -> None:
    db_service = DatabaseService.create(config, 4)
    await ingest_property_description(db_service)


if __name__ == '__main__':
    import argparse
    import asyncio
    import logging
    from lib.service.database.defaults import DB_INSTANCE_MAP

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    asyncio.run(cli_main(DB_INSTANCE_MAP[args.instance]))


