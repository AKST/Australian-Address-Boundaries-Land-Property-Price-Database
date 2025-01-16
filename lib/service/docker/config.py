from dataclasses import dataclass
from typing import Any, List, Literal, Optional, Self, TypedDict

VolumeConfig = dict[str, dict[Literal['mode', 'bind'], str]]

@dataclass
class ImageConfig:
    image_name: str
    image_tag: str
    dockerfile_path: str

class _DockerKwargs(TypedDict, total=False):
    name: str
    labels: dict[str, str]
    volumes: VolumeConfig
    command: list[str]
    shm_size: Optional[int]

@dataclass
class ContainerConfig:
    container_name: str
    project_name: str
    volumes: VolumeConfig
    command: list[str]
    cpu_count: Optional[int]
    memory_limit: Optional[str]
    shared_memory: Optional[int]

    def docker_kwargs(self: Self) -> _DockerKwargs:
        kwargs: _DockerKwargs = {
            'name': self.container_name,
            'labels': {"project": self.project_name},
            'volumes': self.volumes,
            'command': self.command,
        }

        if self.shared_memory is not None:
            kwargs['shm_size'] = self.shared_memory
        # if self.cpu_count:
        #     kwargs['cpus'] = self.cpu_count
        # if self.memory_limit:
        #     kwargs['mem_limit'] = self.memory_limit

        return kwargs


