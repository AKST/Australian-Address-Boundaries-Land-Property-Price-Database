import asyncio
import logging
from dataclasses import dataclass, field
from multiprocessing import Process, Semaphore
from multiprocessing.synchronize import Semaphore as SemaphoreT
from typing import Callable

from lib.pipeline.nsw_vg.property_description import *
from lib.service.database import DatabaseService, DatabaseConfig
from lib.tasks.nsw_vg.config import NswVgTaskConfig

async def cli_main(config: NswVgTaskConfig.PropDescIngest) -> None:
    db_service = DatabaseService.create(config.db_config, config.workers)
    await ingest_property_description(db_service, config)

async def ingest_property_description(
        db: DatabaseService,
        config: NswVgTaskConfig.PropDescIngest) -> None:
    semaphore = Semaphore(1)
    spawn_worker_with_worker_config = lambda w_config: \
        Process(target=spawn_worker,
                args=(w_config, semaphore, config.worker_debug, config.db_config))
    pool = PropDescIngestionWorkerPool(semaphore, spawn_worker_with_worker_config)
    parent = PropDescIngestionSupervisor(db, pool)
    await parent.ingest(config.workers, config.sub_workers)

def spawn_worker(config: WorkerProcessConfig, semaphore: SemaphoreT, worker_debug: bool, db_config: DatabaseConfig):
    async def worker_runtime(config: WorkerProcessConfig, semaphore: SemaphoreT, db_config: DatabaseConfig):
        logging.basicConfig(
            level=logging.DEBUG if worker_debug else logging.INFO,
            format=f'[{config.worker_no}][%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')

        db = DatabaseService.create(db_config, len(config.quantiles))
        worker = PropDescIngestionWorker(semaphore, db)
        await worker.ingest(config.quantiles)
    asyncio.run(worker_runtime(config, semaphore, db_config))

if __name__ == '__main__':
    import argparse
    from lib.service.database.defaults import DB_INSTANCE_MAP

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--debug-worker", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--sub_workers", type=int, required=True)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    asyncio.run(cli_main(
        NswVgTaskConfig.PropDescIngest(
            worker_debug=args.debug_worker,
            workers=args.workers,
            sub_workers=args.sub_workers,
            db_config=DB_INSTANCE_MAP[args.instance],
        ),
    ))


