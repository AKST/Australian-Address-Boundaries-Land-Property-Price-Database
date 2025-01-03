from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Self

@dataclass
class ImageConfig:
    image_name: str
    image_tag: str
    dockerfile_path: str

@dataclass
class ContainerConfig:
    container_name: str
    project_name: str
    volumes: Dict[str, Dict[str, str]]
    command: List[str]
    cpu_count: Optional[int]
    memory_limit: Optional[str]
    shared_memory: Optional[str]

    def docker_kwargs(self: Self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            'name': self.container_name,
            'labels': {"project": self.project_name},
            'volumes': self.volumes,
            'command': self.command,
        }

        if self.shared_memory:
            kwargs['shm_size'] = self.shared_memory
        if self.cpu_count:
            kwargs['cpus'] = self.cpu_count
        if self.memory_limit:
            kwargs['mem_limit'] = self.memory_limit

        return kwargs


