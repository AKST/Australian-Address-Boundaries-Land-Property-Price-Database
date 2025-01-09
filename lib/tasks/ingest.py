from dataclasses import dataclass, field
from datetime import datetime
import logging
import shutil
from typing import Optional, Set

import lib.pipeline.abs.config as abs_config
from lib.pipeline.abs import *
from lib.pipeline.gnaf import GnafWorkerConfig, GnafConfig, GnafState
from lib.pipeline.nsw_vg.config import *
from lib.pipeline.nsw_vg.land_values import NswVgLvCsvDiscoveryMode
from lib.pipeline.nsw_vg.property_sales.ingestion import NSW_VG_PS_INGESTION_CONFIG
from lib.service.clock import ClockService
from lib.service.docker import DockerService, ImageConfig, ContainerConfig
from lib.service.database import DatabaseService, DatabaseConfig
from lib.service.io import IoService
from lib.tasks.fetch_static_files import initialise, get_session
from lib.tasks.ingest_gnaf import ingest_gnaf
from lib.tasks.ingest_abs import ingest_all as ingest_abs
from lib.tasks.nsw_vg.config import NswVgTaskConfig
from lib.tasks.nsw_vg.ingest import ingest_nswvg
from lib.tasks.schema.count import run_count_for_schemas
from lib.tasks.schema.update import update_schema, UpdateSchemaConfig
from lib.tooling.schema.config import ns_dependency_order

@dataclass
class IngestConfig:
    io_file_limit: Optional[int]
    db_config: DatabaseConfig
    gnaf_states: Set[GnafState]
    nswvg_psi_publish_min: Optional[int]
    nswvg_lv_depth: NswVgLvCsvDiscoveryMode.T
    docker_volume: str
    docker_image_config: ImageConfig
    docker_container_config: ContainerConfig
    enable_gnaf: bool

async def ingest_all(config: IngestConfig):
    io_service = IoService.create(None)

    async with get_session(io_service, 'env') as session:
        environment = await initialise(io_service, session)

    docker_service = DockerService.create()
    docker_service.resert_volume(config.docker_volume)
    image = docker_service.create_image(config.docker_image_config)
    image.prepare()

    container = docker_service.create_container(image, config.docker_container_config)
    container.clean()
    container.prepare(config.db_config)
    container.start()
    db_service_config = config.db_config
    db_service = DatabaseService.create(db_service_config, 32)

    await db_service.wait_till_running()
    await db_service.open()

    if environment.gnaf.publication is None:
        raise TypeError('missing gnaf publication')

    await update_schema(
        UpdateSchemaConfig(packages=ns_dependency_order, range=None, apply=True),
        db_service,
        io_service,
    )

    await ingest_abs(
        AbsIngestionConfig(
            ingest_sources=[ABS_MAIN_STRUCTURES, NON_ABS_MAIN_STRUCTURES],
            worker_count=4,
            worker_config=AbsWorkerConfig(
                db_config=db_service_config,
                db_connections=2,
                log_config=AbsWorkerLogConfig(
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt=None,
                ),
            ),
        ),
        db_service,
        io_service,
    )


    await ingest_nswvg(
        environment,
        ClockService(),
        db_service,
        io_service,
        NswVgTaskConfig.Ingestion(
            load_raw_land_values=NswVgTaskConfig.LandValue.Main(
                discovery_mode=config.nswvg_lv_depth,
                truncate_raw_earlier=False,
                child_n=8,
                child_cfg=NswVgTaskConfig.LandValue.Child(
                    debug=False,
                    db_conn=16,
                    chunk_size=1000,
                    db_config=db_service_config,
                ),
            ),
            load_raw_property_sales=NswVgTaskConfig.PsiIngest(
                worker_count=8,
                worker_config=NswVgPsiWorkerConfig(
                    db_config=db_service_config,
                    db_pool_size=16,
                    db_batch_size=1000,
                    file_limit=None,
                    ingestion_config=NSW_VG_PS_INGESTION_CONFIG,
                    parser_chunk_size=8 * 2 ** 10,
                    log_config=None,
                ),
                parent_config=NswVgPsiSupervisorConfig(
                    target_root_dir='./_out_zip',
                    publish_min=config.nswvg_psi_publish_min,
                    publish_max=None,
                    download_min=None,
                    download_max=None,
                ),
            ),
            deduplicate=NswVgTaskConfig.Dedup(
                run_from=None,
                run_till=None,
            ),
            property_descriptions=NswVgTaskConfig.PropDescIngest(
                worker_debug=False,
                workers=8,
                sub_workers=8,
                db_config=db_service_config,
            ),
        ),
    )

    if config.enable_gnaf:
        await ingest_gnaf(
            GnafConfig(
                target=environment.gnaf.publication,
                states=config.gnaf_states,
                workers=8,
                worker_config=GnafWorkerConfig(
                    db_config=instance_cfg.database,
                    db_poolsize=8,
                    batch_size=1000,
                ),
            ),
            db_service,
            io_service,
        )

    await run_count_for_schemas(db_service_config, ns_dependency_order)


if __name__ == '__main__':
    import asyncio
    import argparse
    import resource

    from lib.defaults import INSTANCE_CFG

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)

    args = parser.parse_args()

    logging.getLogger('sqlglot').setLevel(logging.ERROR)
    logging.getLogger('psycopg.pool').setLevel(logging.ERROR)
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    instance_cfg = INSTANCE_CFG[args.instance]
    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    config = IngestConfig(
        io_file_limit=file_limit,
        db_config=instance_cfg.database,
        nswvg_psi_publish_min=instance_cfg.nswvg_psi_min_pub_year,
        nswvg_lv_depth=instance_cfg.nswvg_lv_discovery_mode,
        docker_volume=instance_cfg.docker_volume,
        docker_image_config=instance_cfg.docker_image,
        docker_container_config=instance_cfg.docker_container,
        gnaf_states=instance_cfg.gnaf_states,
        enable_gnaf=instance_cfg.enable_gnaf,
    )

    asyncio.run(ingest_all(config))
