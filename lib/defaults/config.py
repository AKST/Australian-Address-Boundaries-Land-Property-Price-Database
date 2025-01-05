from dataclasses import dataclass
from typing import Set

from lib.pipeline.gnaf.config import GnafState
from lib.pipeline.nsw_vg.land_values import NswVgLvCsvDiscoveryMode
from lib.service.database.config import *
from lib.service.docker.config import *

@dataclass
class InstanceCfg:
    database: DatabaseConfig
    docker_volume: str
    docker_container: ContainerConfig
    docker_image: ImageConfig
    enable_gnaf: bool
    gnaf_states: Set[GnafState]
    nswvg_lv_discovery_mode: NswVgLvCsvDiscoveryMode.T
