import os
from typing import Dict

from lib.service.database.config import *
from lib.service.docker.config import *
from .config import InstanceCfg

_docker_image_name = 'gnaf_pg_gis_db'
_docker_image_tag_1 = "20241004_10_28"
_docker_image_tag_2 = "20241004_10_28"
_docker_image_tag_3 = "20250102_15_33"
_docker_container_name_1 = 'gnaf_db_prod'
_docker_container_name_2 = 'gnaf_db_test'
_docker_container_name_3 = 'gnaf_db_test_2'
_docker_project_label = 'gnaf_pg_gis_db_proj'

_VOLUME_MOUNT = {
    os.path.abspath('./config/postgresql/20241015_config.conf'): {
        'bind': '/etc/postgresql/postgresql.conf',
        'mode': 'rw',
    }
}

_COMMAND_ON_RUN = ['-c', 'config_file=/etc/postgresql/postgresql.conf']

# NOTE THIS DATABASE IS NOT PRIVATE OF INTENDED
# TO BE HOSTED ON ANY MACHINE AND SHOULDN'T BE
# TREATED AS ANYTHING OTHER THAN A FANCY FILE
# THAT JUST HAPPENS TO BE FAIRLY ORGANISED.
#
# If for any reason you choose to extend this
# application or encorporate it in a larger
# system, absolutely define credentials in a
# more confindential manner.

INSTANCE_CFG: Dict[int, InstanceCfg] = {
    1: InstanceCfg(
        enable_gnaf=True,
        database=DatabaseConfig(
            dbname='gnaf_db',
            user='postgres',
            host='localhost',
            port=5434,
            password='throwAwayPassword',
        ),
        docker_container=ContainerConfig(
            container_name=_docker_container_name_1,
            project_name=_docker_project_label,
            volumes=_VOLUME_MOUNT,
            command=_COMMAND_ON_RUN,
            shared_memory='256mb',
            cpu_count=None,
            memory_limit=None,
        ),
        docker_image=ImageConfig(
            image_name=_docker_image_name,
            image_tag=_docker_image_tag_1,
            dockerfile_path="./config/dockerfiles/postgres_2"
        ),
    ),
    2: InstanceCfg(
        enable_gnaf=False,
        database=DatabaseConfig(
            dbname='gnaf_db_2',
            user='postgres',
            host='localhost',
            port=5433,
            password='throwAwayPassword2',
        ),
        docker_container=ContainerConfig(
            container_name=_docker_container_name_2,
            project_name=_docker_project_label,
            volumes=_VOLUME_MOUNT,
            command=_COMMAND_ON_RUN,
            shared_memory='256mb',
            cpu_count=None,
            memory_limit=None,
        ),
        docker_image=ImageConfig(
            image_name=_docker_image_name,
            image_tag=_docker_image_tag_2,
            dockerfile_path="./config/dockerfiles/postgres_2"
        ),
    ),
    3: InstanceCfg(
        enable_gnaf=False,
        database=DatabaseConfig(
            dbname='gnaf_db_3',
            user='postgres',
            host='localhost',
            port=5432,
            password='throwAwayPassword3',
        ),
        docker_container=ContainerConfig(
            container_name=_docker_container_name_3,
            project_name=_docker_project_label,
            volumes=_VOLUME_MOUNT,
            command=_COMMAND_ON_RUN,
            shared_memory='256mb',
            cpu_count=8,
            memory_limit="4g",
        ),
        docker_image=ImageConfig(
            image_name=_docker_image_name,
            image_tag=_docker_image_tag_2,
            dockerfile_path="./config/dockerfiles/postgres_2"
        ),
    ),
}
