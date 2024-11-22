import asyncio
import logging
from dataclasses import dataclass, field
from multiprocessing import Process, Semaphore
from multiprocessing.synchronize import Semaphore as SemaphoreT
from typing import Callable

from lib.pipeline.nsw_vg.property_description.ingest import PropertyDescriptionIngestionParent
from lib.pipeline.nsw_vg.property_description.ingest import PropertyDescriptionIngestionChild
from lib.pipeline.nsw_vg.property_description.ingest import QuantileWorkerPool
from lib.pipeline.nsw_vg.property_description.ingest import WorkerProcessConfig
from lib.service.database import DatabaseService, DatabaseConfig

@dataclass
class NswVgLegalDescriptionIngestionConfig:
    workers: int
    sub_workers: int
    child_debug: bool
    db_config: DatabaseConfig

async def cli_main(config: NswVgLegalDescriptionIngestionConfig) -> None:
    db_service = DatabaseService.create(config.db_config, config.workers)
    await ingest_property_description(db_service, config)

async def ingest_property_description(
        db: DatabaseService,
        config: NswVgLegalDescriptionIngestionConfig) -> None:
    semaphore = Semaphore(1)
    spawn_child_with_worker_config = lambda w_config: \
        Process(target=spawn_child,
                args=(w_config, semaphore, config.child_debug, config.db_config))
    pool = QuantileWorkerPool(semaphore, spawn_child_with_worker_config)
    parent = PropertyDescriptionIngestionParent(db, pool)
    await parent.ingest(config.workers, config.sub_workers)

def spawn_child(config: WorkerProcessConfig, semaphore: SemaphoreT, child_debug: bool, db_config: DatabaseConfig):
    async def child_runtime(config: WorkerProcessConfig, semaphore: SemaphoreT, db_config: DatabaseConfig):
        logging.basicConfig(
            level=logging.DEBUG if child_debug else logging.INFO,
            format=f'[{config.child_no}][%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')

        db = DatabaseService.create(db_config, len(config.quantiles))
        child = PropertyDescriptionIngestionChild(semaphore, db)
        await child.ingest(config.quantiles)
    asyncio.run(child_runtime(config, semaphore, db_config))

if __name__ == '__main__':
    import argparse
    from lib.service.database.defaults import DB_INSTANCE_MAP

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--debug-child", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--sub_workers", type=int, required=True)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    asyncio.run(cli_main(
        NswVgLegalDescriptionIngestionConfig(
            db_config=DB_INSTANCE_MAP[args.instance],
            workers=args.workers,
            sub_workers=args.sub_workers,
            child_debug=args.debug_child,
        ),
    ))


