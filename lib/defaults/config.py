from dataclasses import dataclass

from lib.service.database.config import *
from lib.service.docker.config import *

@dataclass
class InstanceCfg:
    database: DatabaseConfig
    docker_container: ContainerConfig
    docker_image: ImageConfig
    enable_gnaf: bool
