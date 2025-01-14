import logging
from typing import List

from lib.service.clock import ClockService
from lib.service.database import DatabaseService, DatabaseConfig
from lib.service.io import IoService
from lib.tooling.schema import Command, SchemaController, SchemaDiscovery

from .config import GisTaskConfig

all_scripts = [
    f'./sql/nsw_spatial/tasks/{script}.sql'
    for script in [
        'dedup_lot_layer',
        'dedup_prop_layer',
    ]
]


async def ingest_deduplication(
    db: DatabaseService,
    io: IoService,
    clock: ClockService,
    cfg: GisTaskConfig.Deduplication
):
    logger = logging.getLogger(__name__)

    run_from = cfg.run_from or 1
    run_till = cfg.run_till or len(all_scripts)
    if 1 > run_from or len(all_scripts) < run_from:
        raise ValueError(f'dedup run from {cfg.run_from} is out of scope')
    else:
        scripts = all_scripts[run_from - 1:run_till]

    discovery = SchemaDiscovery.create(io)
    controller = SchemaController(io, db, discovery)

    async def run_commands(commands: List[Command.BaseCommand]):
        for c in commands:
            await controller.command(c)

    if cfg.truncate:
        await run_commands([
            Command.Truncate(ns='nsw_lrs', cascade=True, range=range(4, 5)),
        ])

if __name__ == '__main__':
    import asyncio
    import argparse

    from lib.defaults import INSTANCE_CFG
    from lib.utility.logging import config_vendor_logging, config_logging

    parser = argparse.ArgumentParser(description="Deduplicate GIS")
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--run-from", type=int, default=1)
    parser.add_argument("--run-till", type=int, default=len(all_scripts))

    args = parser.parse_args()

    config_vendor_logging({'sqlglot', 'psycopg.pool'})
    config_logging(worker=None, debug=args.debug)
    logging.debug(args)

    cli_conf = GisTaskConfig.Deduplication(
        run_from=args.run_from,
        run_till=args.run_till,
    )
