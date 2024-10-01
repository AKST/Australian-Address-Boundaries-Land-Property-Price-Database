from dataclasses import dataclass, field
import logging

from lib.gnaf_db import GnafDb
from lib.service.io import IoService

_logger = logging.getLogger(f'{__name__}.initialise_nsw_vg_schema')

@dataclass
class InitialiseNswVgSchemaConfig:
    apply: bool = field(default=True)
    revert: bool = field(default=False)

async def initialise_nsw_vg_schema(
    config: InitialiseNswVgSchemaConfig,
    gnaf_db: GnafDb,
    io: IoService,
) -> None:
    _logger.info('initalising nsw_vg db schema')
    revert_files = [f async for f in io.grep_dir('sql/nsw_vg/schema', '*_REVERT.sql')]
    apply_files = [f async for f in io.grep_dir('sql/nsw_vg/schema', '*_APPLY_*.sql')]

    async with (
        await gnaf_db.async_connect() as conn,
        conn.cursor() as cursor
    ):
        if config.revert:
            for file in sorted(revert_files, reverse=True):
                _logger.info(f'running revert {file}')
        if config.apply:
            for file in sorted(apply_files):
                _logger.info(f'running apply {file}')
                try:
                    file_body = await io.f_read(file)
                    await cursor.execute(file_body)
                except Exception as e:
                    _logger.error(f"failed to run {file}")
                    raise e


if __name__ == '__main__':
    import asyncio
    import argparse
    import resource

    from lib import notebook_constants as nc

    parser = argparse.ArgumentParser(description="Initialise nswvg db schema")
    parser.add_argument("--instance", type=int, default=1)
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--enable-revert", action='store_true', default=False)
    parser.add_argument("--disable-apply", action='store_true', default=False)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    if args.instance == 1:
        db_conf, db_name = nc.gnaf_dbconf, nc.gnaf_dbname
    elif args.instance == 2:
        db_conf, db_name = nc.gnaf_dbconf_2, nc.gnaf_dbname_2
    else:
        raise ValueError('invalid instance')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    config = InitialiseNswVgSchemaConfig(
        apply=not args.disable_apply,
        revert=args.enable_revert,
    )

    main_logger = logging.getLogger(f'{__name__}::init')
    main_logger.debug(f'operating on instance #{args.instance}')
    main_logger.debug(f'db config {db_conf}')
    main_logger.debug(f'db name {db_name}')
    main_logger.debug(f'file limit {file_limit}')
    main_logger.debug(f'config {config}')

    async def main() -> None:
        gnaf_db = GnafDb.create(db_conf, db_name)
        io_service = IoService.create(file_limit)
        await initialise_nsw_vg_schema(config, gnaf_db, io_service)

    asyncio.run(main())

