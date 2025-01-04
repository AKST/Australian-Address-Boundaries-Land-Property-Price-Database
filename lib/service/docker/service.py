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
    def create() -> 'DockerService':
        client = docker.from_env()
        return DockerService(client)

    def resert_volume(self: Self, volume_name: str):
        for container in self.docker.containers.list(all=True):
            if any((
                mount["Name"] == volume_name
                for mount in container.attrs["Mounts"]
                if 'Name' in mount
            )):
                container.stop()
                container.remove(force=True)
        try:
            existing_volume = self.docker.volumes.get(volume_name)
            existing_volume.remove()
        except docker.errors.NotFound:
            pass

        self.docker.volumes.create(name=volume_name)

    def create_image(self: Self, config: ImageConfig) -> 'DockerImage':
        return DockerImage(self.docker, config)

    def create_container(self: Self,
                         image: 'DockerImage',
                         config: ContainerConfig) -> 'DockerContainer':
        return DockerContainer(self.docker, image, config)

# TODO issue #31 make this async
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

# TODO issue #31 make this async
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

    def prepare(self: Self, db_config: DatabaseConfig):
        try:
            self._get_container()
        except NotFound:
            environment = {
                'POSTGRES_DB': db_config.dbname,
                'POSTGRES_USER': db_config.user,
                'POSTGRES_PASSWORD': db_config.password,
            }
            self.image._build_container(
                environment=environment,
                ports={'5432/tcp': db_config.port},
                detach=True,
                **self.config.docker_kwargs(),
            )

    def start(self: Self):
        self._get_container().start()

    def stop(self: Self, throw_if_not_found=True):
        try:
            container = self._get_container()
        except NotFound as e:
            if throw_if_not_found:
                raise e
        container.stop()

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

