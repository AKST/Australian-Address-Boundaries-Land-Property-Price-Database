import os
from typing import Dict, Set

from lib.pipeline.gnaf.config import GnafState
from lib.pipeline.nsw_vg.land_values import NswVgLvCsvDiscoveryMode
from lib.service.database.config import *
from lib.service.docker.config import *
from .config import InstanceCfg

ALL_STATES: Set[GnafState] = { 'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'OT', 'ACT' }

_docker_image_name = 'au_land_pggis_db'
_docker_image_tag_1 = "20250106_11_39"
_docker_image_tag_2 = "20250106_11_39"
_docker_image_tag_3 = "20250106_11_39"
_docker_volumn_1 = 'vol_au_land_db'
_docker_volumn_2 = 'vol_au_land_db_test_1'
_docker_volumn_3 = 'vol_au_land_db_test_2'
_docker_container_name_1 = 'au_land_db_prod'
_docker_container_name_2 = 'au_land_db_test'
_docker_container_name_3 = 'au_land_db_test_2'
_docker_project_label = 'au_land_pggis_db_proj'
_db_name_1 = 'au_land_db'
_db_name_2 = 'au_land_db_2'
_db_name_3 = 'au_land_db_3'

Mb_256 = 256_000_000

_COMMAND_ON_RUN = ['-c', 'config_file=/etc/postgresql/postgresql.conf']

def _create_mounted_dirs(
    data_volume_name: str,
    postgres_config: str = '20241015_config',
) -> VolumeConfig:
    return {
        data_volume_name: {
            'bind': '/var/lib/postgresql/data',
            'mode': 'rw',
        },
        os.path.abspath(f'./config/postgresql/{postgres_config}.conf'): {
            'bind': '/etc/postgresql/postgresql.conf',
            'mode': 'rw',
        },
    }

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
        nswvg_lv_discovery_mode=NswVgLvCsvDiscoveryMode.EachNthYear(4, include_first=True),
        nswvg_psi_min_pub_year=None,
        gnaf_states=ALL_STATES,
        enable_gnaf=True,
        database=DatabaseConfig(
            dbname=_db_name_1,
            user='postgres',
            host='localhost',
            port=5434,
            password='throwAwayPassword',
        ),
        docker_volume=_docker_volumn_1,
        docker_container=ContainerConfig(
            container_name=_docker_container_name_1,
            project_name=_docker_project_label,
            volumes=_create_mounted_dirs(
                data_volume_name=_docker_volumn_1,
                postgres_config='20241015_config',
            ),
            command=_COMMAND_ON_RUN,
            shared_memory=Mb_256,
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
        nswvg_lv_discovery_mode=NswVgLvCsvDiscoveryMode.Latest(),
        nswvg_psi_min_pub_year=2024,
        gnaf_states={'NSW'},
        enable_gnaf=True,
        database=DatabaseConfig(
            dbname=_db_name_2,
            user='postgres',
            host='localhost',
            port=5433,
            password='throwAwayPassword2',
        ),
        docker_volume=_docker_volumn_2,
        docker_container=ContainerConfig(
            container_name=_docker_container_name_2,
            project_name=_docker_project_label,
            volumes=_create_mounted_dirs(
                data_volume_name=_docker_volumn_2,
                postgres_config='20241015_config',
                # postgres_config='concurrent_config_20250104',
            ),
            command=_COMMAND_ON_RUN,
            shared_memory=Mb_256,
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
        nswvg_lv_discovery_mode=NswVgLvCsvDiscoveryMode.EachNthYear(4, include_first=True),
        nswvg_psi_min_pub_year=None,
        gnaf_states={'NSW'},
        enable_gnaf=True,
        database=DatabaseConfig(
            dbname=_db_name_3,
            user='postgres',
            host='localhost',
            port=5431,
            password='throwAwayPassword3',
        ),
        docker_volume=_docker_volumn_3,
        docker_container=ContainerConfig(
            container_name=_docker_container_name_3,
            project_name=_docker_project_label,
            volumes=_create_mounted_dirs(
                data_volume_name=_docker_volumn_3,
                postgres_config='20241015_config',
                # postgres_config='concurrent_config_20250104',
            ),
            command=_COMMAND_ON_RUN,
            shared_memory=Mb_256,
            cpu_count=8,
            memory_limit="4g",
        ),
        docker_image=ImageConfig(
            image_name=_docker_image_name,
            image_tag=_docker_image_tag_3,
            dockerfile_path="./config/dockerfiles/postgres_16"
        ),
    ),
}
