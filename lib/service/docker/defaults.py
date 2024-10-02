from .config import *


_docker_image_name = 'gnaf_pg_gis_db'
_docker_image_tag_1 = "20240908_19_53"
_docker_image_tag_2 = "20240908_19_53"
_docker_container_name_1 = 'gnaf_db_prod'
_docker_container_name_2 = 'gnaf_db_test'
_docker_project_label = 'gnaf_pg_gis_db_proj'

INSTANCE_1_IMAGE_CONF = ImageConfig(
    image_name=_docker_image_name,
    image_tag=_docker_image_tag_1,
    dockerfile_path="./dockerfiles/postgres_2"
)

INSTANCE_2_IMAGE_CONF = ImageConfig(
    image_name=_docker_image_name,
    image_tag=_docker_image_tag_2,
    dockerfile_path="./dockerfiles/postgres_2"
)

INSTANCE_1_CONTAINER_CONF = ContainerConfig(
    container_name=_docker_container_name_1,
    project_name=_docker_project_label,
)

INSTANCE_2_CONTAINER_CONF = ContainerConfig(
    container_name=_docker_container_name_2,
    project_name=_docker_project_label,
)
