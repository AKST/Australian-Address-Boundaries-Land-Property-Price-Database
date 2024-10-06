import asyncio
from datetime import datetime, date
from dataclasses import dataclass
from functools import reduce
import logging
from multiprocessing import Process, Queue as IpcQueue
from time import time
from typing import List, Optional, Self, Tuple
from pathlib import Path
from pprint import pprint
import resource
import os

from lib.pipeline.nsw_vg.property_sales.data import *
from lib.pipeline.nsw_vg.property_sales.file_format import PropertySalesRowParserFactory, BufferedFileReaderTextSource
from lib.pipeline.nsw_vg.property_sales.ingestion import NSW_VG_PS_INGESTION_CONFIG, PropertySalesIngestion
from lib.pipeline.nsw_vg.property_sales.orchestration import *
from lib.service.clock import ClockService
from lib.service.io import IoService
from lib.service.database import DatabaseService, DatabaseConfig
from lib.utility.sampling import Sampler, SamplingConfig

from ..fetch_static_files import Environment, get_session, initialise
from ..update_schema import update_schema, UpdateSchemaConfig

ZIP_DIR = './_out_zip'

@dataclass
class PropertySaleIngestionConfig:
    db_config: DatabaseConfig
    file_limit: int
    worker_count: int
    worker_config: ChildConfig
    parent_config: ParentConfig
    update: UpdateSchemaConfig

async def _child_main(
    config: ChildConfig,
    recv_msgs: IpcQueue,
    send_msgs: IpcQueue,
) -> None:
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)

    if config.file_limit:
        soft_limit = min(config.file_limit, hard_limit)
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft_limit, hard_limit))

    file_limit = int(soft_limit * 0.8)
    io = IoService.create(file_limit - config.db_pool_size)
    clock = ClockService()
    db = DatabaseService.create(config.db_config, config.db_pool_size)

    try:
        await db.open()
        async with asyncio.TaskGroup() as tg:
            factory = PropertySalesRowParserFactory(
                io,
                BufferedFileReaderTextSource,
                config.parser_chunk_size,
            )
            ingestion = PropertySalesIngestion.create(
                db,
                tg,
                config.ingestion_config,
                config.db_batch_size,
            )
            server = NswVgPsChildServer(
                tg,
                ingestion,
                factory,
                ParentClient(
                    os.getpid(),
                    send_msgs,
                    threshold=1000,
                ),
            )

            server.start_ingestion()

            while not server.closing:
                message = await asyncio.to_thread(recv_msgs.get)
                await tg.create_task(server.on_message(message))
            await tg.create_task(server.flush())
        logging.info('this child has finished')
    except Exception as e:
        logging.error("this child has died")
        logging.exception(e)
        raise e
    finally:
        await db.close()

async def ingest_property_sales_rows(
    environment: Environment,
    clock: ClockService,
    io: IoService,
    worker_count: int,
    worker_config: ChildConfig,
    parent_config: ParentConfig,
):
    logger = logging.getLogger(f'{__name__}.ingest_property_sales_rows')
    sampler = Sampler[IngestionSample].create(
        clock,
        SamplingConfig(min_sample_delta=0.1),
        logging.getLogger(f'{__name__}.progress'),
        IngestionSample(),
    )

    q_recv: IpcQueue = IpcQueue()
    p_children: List[NswVgPsChildClient] = []
    try:
        for idx in range(0, worker_count):
            q_send: IpcQueue = IpcQueue()
            worker_args = (idx, worker_config, q_send, q_recv)
            p_child = Process(target=_child_proc_entry, args=worker_args)
            p_children.append(NswVgPsChildClient(q_send, p_child))
            p_child.start()
    except Exception as e:
        for p in p_children:
            p.terminate()
        raise e

    async with asyncio.TaskGroup() as tg:
        orchestrator = NswVgPsIngestionCoordinator(
            parent_config,
            sampler,
            q_recv,
            p_children,
            tg,
            io,
        )
        await orchestrator.process([
            *environment.sale_price_annual.links,
            *environment.sale_price_weekly.links,
        ])
    for p in p_children:
        p.kill()
    logger.debug('ingestion ended')

async def _cli_main(config: PropertySaleIngestionConfig):
    logger = logging.getLogger(f'{__name__}._cli_main')
    clock = ClockService()
    io = IoService.create(config.file_limit - (config.worker_count + 1))
    async with get_session(io) as session:
        environment = await initialise(io, session)

    db = DatabaseService.create(config.db_config, 1)
    try:
        await db.open()
        logger.debug('intialising schema starting')
        await update_schema(config.update, db, io)
        logger.debug('intialising schema done')
    finally:
        await db.close()

    logger.debug('starting ingestion task')
    await ingest_property_sales_rows(
        environment,
        clock,
        io,
        config.worker_count,
        config.worker_config,
        config.parent_config,
    )

def _child_proc_entry(
    idx: int,
    worker_config: ChildConfig,
    recv_msgs: IpcQueue,
    send_msgs: IpcQueue,
) -> None:
    logging.basicConfig(
        level=logging.DEBUG if worker_config.debug_logs else logging.INFO,
        format=f'[{idx}][%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.debug(f'initalising child process #{idx}')

    asyncio.run(_child_main(worker_config, recv_msgs, send_msgs))

if __name__ == '__main__':
    import argparse
    import os

    from lib.service.database.defaults import DB_INSTANCE_MAP

    parser = argparse.ArgumentParser(description="Ingest NSW Land values")
    parser.add_argument("--debug-parent", action='store_true', default=False)
    parser.add_argument("--debug-child", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--workers", type=int, default=os.cpu_count())
    parser.add_argument("--worker-file-limit", type=int)
    parser.add_argument("--db-batch-size", type=int, default=50)
    parser.add_argument("--db-pool-size", type=int, default=8)
    parser.add_argument("--parser-chunk-size", type=int, default=8 * 2 ** 10)
    parser.add_argument("--publish-min", type=int)
    parser.add_argument("--publish-max", type=int)
    parser.add_argument("--download-min", type=date.fromisoformat)
    parser.add_argument("--download-max", type=date.fromisoformat)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug_parent else logging.INFO,
        format='[main][%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.debug(args)

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)
    db_config = DB_INSTANCE_MAP[args.instance]

    task_config = PropertySaleIngestionConfig(
        db_config=db_config,
        file_limit=file_limit,
        worker_count=args.workers,
        parent_config=ParentConfig(
            target_root_dir=ZIP_DIR,
            publish_min=args.publish_min,
            publish_max=args.publish_max,
            download_min=args.download_min,
            download_max=args.download_max,
        ),
        worker_config=ChildConfig(
            parser_chunk_size=args.parser_chunk_size,
            file_limit=args.worker_file_limit,
            db_pool_size=args.db_pool_size,
            db_batch_size=args.db_batch_size,
            db_config=db_config,
            ingestion_config=NSW_VG_PS_INGESTION_CONFIG,
            debug_logs=args.debug_child,
        ),
        update=UpdateSchemaConfig(
            packages=['nsw_vg'],
            apply=True,
            revert=True,
        ),
    )

    logging.debug(task_config)
    asyncio.run(_cli_main(task_config))

