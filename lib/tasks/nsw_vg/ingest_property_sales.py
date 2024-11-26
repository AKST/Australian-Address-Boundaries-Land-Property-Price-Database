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
from lib.service.database import DatabaseService
from lib.utility.sampling import Sampler, SamplingConfig

from .config import NswVgTaskConfig
from ..fetch_static_files import Environment

async def ingest_property_sales_rows(
    environment: Environment,
    clock: ClockService,
    io: IoService,
    config: NswVgTaskConfig.PsiIngest,
):
    worker_count = config.worker_count
    worker_config = config.worker_config
    parent_config = config.parent_config
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

def _child_proc_entry(
    idx: int,
    worker_config: NswVgPsiWorkerConfig,
    recv_msgs: IpcQueue,
    send_msgs: IpcQueue,
) -> None:
    if worker_config.log_config:
        logging.basicConfig(
            level=logging.DEBUG if worker_config.log_config.debug_logs else logging.INFO,
            format=f'[{idx}]{worker_config.log_config.format}',
            datefmt=worker_config.log_config.datefmt)

    logging.debug(f'initalising child process #{idx}')

    asyncio.run(_child_main(worker_config, recv_msgs, send_msgs))

async def _child_main(
    config: NswVgPsiWorkerConfig,
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
