from dataclasses import dataclass
from typing import Dict, List

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


