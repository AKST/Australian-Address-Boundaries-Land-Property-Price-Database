import os
from typing import Dict

from .config import *

_docker_image_name = 'gnaf_pg_gis_db'
_docker_image_tag_1 = "20241004_10_28"
_docker_image_tag_2 = "20241004_10_28"
_docker_container_name_1 = 'gnaf_db_prod'
_docker_container_name_2 = 'gnaf_db_test'
_docker_project_label = 'gnaf_pg_gis_db_proj'

VOLUME_MOUNT = {
    os.path.abspath('./config/postgresql/20241015_config.conf'): {
        'bind': '/etc/postgresql/postgresql.conf',
        'mode': 'rw',
    }
}

COMMAND_ON_RUN = ['-c', 'config_file=/etc/postgresql/postgresql.conf']

INSTANCE_1_IMAGE_CONF = ImageConfig(
    image_name=_docker_image_name,
    image_tag=_docker_image_tag_1,
    dockerfile_path="./config/dockerfiles/postgres_2"
)

INSTANCE_2_IMAGE_CONF = ImageConfig(
    image_name=_docker_image_name,
    image_tag=_docker_image_tag_2,
    dockerfile_path="./config/dockerfiles/postgres_2"
)

INSTANCE_1_CONTAINER_CONF = ContainerConfig(
    container_name=_docker_container_name_1,
    project_name=_docker_project_label,
    volumes=VOLUME_MOUNT,
    command=COMMAND_ON_RUN,
)

INSTANCE_2_CONTAINER_CONF = ContainerConfig(
    container_name=_docker_container_name_2,
    project_name=_docker_project_label,
    volumes=VOLUME_MOUNT,
    command=COMMAND_ON_RUN,
)

INSTANCE_CONTAINER_CONF_MAP: Dict[int, ContainerConfig] = {
    1: INSTANCE_1_CONTAINER_CONF,
    2: INSTANCE_2_CONTAINER_CONF,
}

INSTANCE_IMAGE_CONF_MAP: Dict[int, ImageConfig] = {
    1: INSTANCE_1_IMAGE_CONF,
    2: INSTANCE_2_IMAGE_CONF,
}
