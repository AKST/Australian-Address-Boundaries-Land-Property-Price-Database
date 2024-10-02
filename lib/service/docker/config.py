from dataclasses import dataclass

@dataclass
class ImageConfig:
    image_name: str
    image_tag: str
    dockerfile_path: str

@dataclass
class ContainerConfig:
    container_name: str
    project_name: str


