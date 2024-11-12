from dataclasses import dataclass, field
import logging
from typing import Optional

import lib.pipeline.abs.config as abs_config
import lib.pipeline.nsw_vg.property_sales.orchestration.config as nswvg_psi_conf
from lib.pipeline.abs.defaults import ABS_MAIN_STRUCTURES, NON_ABS_MAIN_STRUCTURES
from lib.pipeline.gnaf.init_schema import init_target_schema
from lib.pipeline.nsw_vg.property_sales.ingestion import NSW_VG_PS_INGESTION_CONFIG
from lib.service.clock import ClockService
from lib.service.docker import DockerService, ImageConfig, ContainerConfig
from lib.service.database import DatabaseService, DatabaseConfig
from lib.service.io import IoService
from lib.tasks.fetch_static_files import initialise, get_session
from lib.tasks.ingest_gnaf import ingest_gnaf
from lib.tasks.ingest_abs import ingest_all as ingest_abs
from lib.tasks.nsw_vg.ingest import ingest_nswvg, NswVgIngestionConfig, NswVgIngestionDedupConfig, NswVgLandValueIngestionConfig
from lib.tasks.nsw_vg.ingest_property_sales import PropertySaleIngestionConfig
from lib.tasks.schema.count import run_count_for_schemas
from lib.tasks.schema.update import update_schema, UpdateSchemaConfig
from lib.tooling.schema.config import ns_dependency_order

@dataclass
class IngestConfig:
    io_file_limit: Optional[int]
    db_config: DatabaseConfig
    docker_image_config: ImageConfig
    docker_container_config: ContainerConfig


async def ingest_all(config: IngestConfig):
    io_service = IoService.create(None)

    async with get_session(io_service) as session:
        environment = await initialise(io_service, session)

    docker_service = DockerService.create()
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

    await init_target_schema(
        environment.gnaf.publication,
        io_service,
        db_service,
    )

    await update_schema(
        UpdateSchemaConfig(packages=ns_dependency_order, range=None, apply=True),
        db_service,
        io_service,
    )

    await ingest_abs(
        abs_config.IngestionConfig(
            ingest_sources=[ABS_MAIN_STRUCTURES, NON_ABS_MAIN_STRUCTURES],
            worker_count=4,
            worker_config=abs_config.WorkerConfig(
                db_config=db_service_config,
                db_connections=2,
                log_config=abs_config.WorkerLogConfig(
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
        NswVgIngestionConfig(
            load_raw_land_values=NswVgLandValueIngestionConfig(
                truncate_raw_earlier=False,
            ),
            load_raw_property_sales=PropertySaleIngestionConfig(
                worker_count=6,
                worker_config=nswvg_psi_conf.ChildConfig(
                    db_config=db_service_config,
                    db_pool_size=4,
                    db_batch_size=1000,
                    file_limit=None,
                    ingestion_config=NSW_VG_PS_INGESTION_CONFIG,
                    parser_chunk_size=8 * 2 ** 10,
                    log_config=None,
                ),
                parent_config=nswvg_psi_conf.ParentConfig(
                    target_root_dir='./_out_zip',
                    publish_min=None,
                    publish_max=None,
                    download_min=None,
                    download_max=None,
                ),
            ),
            deduplicate=NswVgIngestionDedupConfig(
                run_from=1,
                run_till=6,
            ),
            load_parcels=False,
        ),
    )

    await ingest_gnaf(environment.gnaf.publication, db_service)

    await run_count_for_schemas(db_service_config, ns_dependency_order)


if __name__ == '__main__':
    import asyncio
    import argparse
    import resource

    from lib.service.database.defaults import DB_INSTANCE_MAP
    from lib.service.docker.defaults import INSTANCE_IMAGE_CONF_MAP, INSTANCE_CONTAINER_CONF_MAP

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    docker_image_conf = INSTANCE_IMAGE_CONF_MAP[args.instance]
    docker_container_conf = INSTANCE_CONTAINER_CONF_MAP[args.instance]

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    config = IngestConfig(
        io_file_limit=file_limit,
        db_config=DB_INSTANCE_MAP[args.instance],
        docker_image_config=INSTANCE_IMAGE_CONF_MAP[args.instance],
        docker_container_config=INSTANCE_CONTAINER_CONF_MAP[args.instance],
    )

    asyncio.run(ingest_all(config))
