import docker
from docker.errors import NotFound, ImageNotFound
from typing import Self, Any

from lib.service.database import DatabaseConfig

from .config import ImageConfig, ContainerConfig

class DockerService:
    """
    To be honest... This is more of a factory.
    """
    docker: Any

    def __init__(self: Self, docker: Any) -> None:
        self.docker = docker

    @staticmethod
    def create():
        client = docker.from_env()
        return DockerService(client)

    def create_image(self: Self, config: ImageConfig) -> 'DockerImage':
        return DockerImage(self.docker, config)

    def create_container(self: Self,
                         image: 'DockerImage',
                         config: ContainerConfig) -> 'DockerContainer':
        return DockerContainer(self.docker, image, config)

class DockerImage:
    config: ImageConfig
    docker: Any

    def __init__(self: Self, docker: Any, config: ImageConfig):
        self.docker = docker
        self.config = config

    def _build_container(self: Self, **kwargs):
        name, tag = self.config.image_name, self.config.image_tag
        self.docker.containers.run(image=f"{name}:{tag}", **kwargs)

    def prepare(self: Self):
        name, tag = self.config.image_name, self.config.image_tag
        file = self.config.dockerfile_path
        if not self._image_built():
            self.docker.images.build(path=file, tag=f"{name}:{tag}")

    def nuke(self: Self):
        f = {"ancestor": self.config.image_name}
        for container in self.docker.containers.list(all=True, filters=f):
            container.stop()
            container.remove()

        name, tag = self.config.image_name, self.config.image_tag
        self.docker.images.remove(image=f"{name}:{tag}", force=True)

    def _image_built(self: Self):
        name, tag = self.config.image_name, self.config.image_tag
        try:
            self.docker.images.get(f"{name}:{tag}")
            return True
        except ImageNotFound:
            return False

class DockerContainer:
    config: ContainerConfig
    docker: Any
    image: DockerImage

    def __init__(self: Self,
                 docker: Any,
                 image: DockerImage,
                 config: ContainerConfig):
        self.docker = docker
        self.image = image
        self.config = config

    def prepare(self: Self, config: DatabaseConfig):
        try:
            self._get_container()
        except NotFound:
            self.image._build_container(
                name=self.config.container_name,
                labels={"project": self.config.project_name},
                environment={
                    'POSTGRES_DB': config.dbname,
                    'POSTGRES_USER': config.user,
                    'POSTGRES_PASSWORD': config.password,
                },
                ports={'5432/tcp': config.port},
                detach=True,
            )

    def start(self: Self):
        self._get_container().start()

    def stop(self: Self):
        self._get_container().stop()

    def clean(self: Self):
        try:
            container = self._get_container()
            container.stop()
            container.remove()
        except NotFound:
            pass

        f = {"status": "exited", "label": self.config.project_name}
        for container in self.docker.containers.list(all=True, filters=f):
            container.stop()
            container.remove()

    def _get_container(self: Self):
        return self.docker.containers.get(self.config.container_name)

