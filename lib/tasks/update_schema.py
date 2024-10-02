from dataclasses import dataclass, field
import logging
from typing import List

from lib.service.database import DatabaseService
from lib.service.io import IoService

_logger = logging.getLogger(f'{__name__}.initialise_db_schema')

packages_allowed = [
    'meta',
    'abs',
    'nsw_lrs',
    'nsw_environment',
    'nsw_property',
    'nsw_spatial',
    'nsw_vg',
]

@dataclass
class UpdateSchemaConfig:
    packages: List[str] = field(default_factory=lambda: packages_allowed)
    apply: bool = field(default=True)
    revert: bool = field(default=False)

async def update_schema(
    config: UpdateSchemaConfig,
    db: DatabaseService,
    io: IoService,
) -> None:
    _logger.info('initalising nsw_vg db schema')
    revert_files = [
        file
        for p in reversed(config.packages)
        for file in sorted([
            file
            async for file in io.grep_dir(f'sql/{p}/schema', '*_REVERT.sql')
        ], reverse=True)
    ] if config.revert else []

    apply_files = [
        file
        for p in config.packages
        for file in sorted([
            file
            async for file in io.grep_dir(f'sql/{p}/schema', '*_APPLY_*.sql')
        ])
    ] if config.apply else []

    async def run_sql(file, cursor):
        try:
            _logger.info(f'running {file}')
            file_body = await io.f_read(file)
            await cursor.execute(file_body)
        except Exception as e:
            _logger.error(f"failed to run {file}")
            raise e

    async with await db.async_connect() as c, c.cursor() as cursor:
        for file in revert_files:
            await run_sql(file, cursor)

        for file in apply_files:
            await run_sql(file, cursor)


if __name__ == '__main__':
    import asyncio
    import argparse
    import resource

    from lib.service.database.defaults import instance_1_config, instance_2_config

    parser = argparse.ArgumentParser(description="Initialise nswvg db schema")
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--packages", nargs='*', default=[])
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--enable-revert", action='store_true', default=False)
    parser.add_argument("--disable-apply", action='store_true', default=False)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    if args.instance == 1:
        db_conf = instance_1_config
    elif args.instance == 2:
        db_conf = instance_2_config
    else:
        raise ValueError('invalid instance')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    if args.packages:
        packages = [p for p in packages_allowed if p in args.packages]
    else:
        packages = packages_allowed

    config = UpdateSchemaConfig(
        packages=packages,
        apply=not args.disable_apply,
        revert=args.enable_revert,
    )

    main_logger = logging.getLogger(f'{__name__}::init')
    main_logger.debug(f'operating on instance #{args.instance}')
    main_logger.debug(f'db config {db_conf}')
    main_logger.debug(f'file limit {file_limit}')
    main_logger.debug(f'config {config}')

    async def main() -> None:
        db = DatabaseService(db_conf)
        io = IoService.create(file_limit)
        await update_schema(config, db, io)

    asyncio.run(main())

