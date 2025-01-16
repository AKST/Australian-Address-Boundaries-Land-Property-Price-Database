import asyncio
from aiodocker import Docker as DockerClient
from aiodocker.types import JSONObject
from aiodocker.exceptions import DockerError
import logging
from typing import Any, Dict, List, Self, Optional

from lib.service.database import DatabaseConfig
from lib.service.io import IoService
from .config import ImageConfig, ContainerConfig, VolumeConfig

class DockerService:
    """
    To be honest... This is more of a factory.
    """
    logger = logging.getLogger(__name__)

    def __init__(
            self: Self,
            docker: DockerClient) -> None:
        self.docker = docker

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.close()

    async def close(self: Self):
        await self.docker.close()


    @staticmethod
    def create() -> 'DockerService':
        client = DockerClient()
        return DockerService(client)

    async def reset_volume(self: Self, volume_name: str):
        containers = await self.docker.containers.list()
        for container in containers:
            container_deets = await container.show()
            mounts = container_deets.get('Mounts', [])
            if any(mount.get('Name') == volume_name for mount in mounts):
                await container.delete(force=True)

        try:
            volume = await self.docker.volumes.get(volume_name)
            volume.delete()
        except DockerError:
            pass

        await self.docker.volumes.create({'Name': volume_name})

    def create_image(self: Self, config: ImageConfig) -> 'DockerImage':
        return DockerImage(self.docker, config)

    def create_container(self: Self,
                                image: 'DockerImage',
                                config: ContainerConfig) -> 'DockerContainer':
        return DockerContainer(self.docker, image, config)

class DockerImage:
    logger = logging.getLogger(f'{__name__}.image')

    def __init__(self: Self, docker: DockerClient, config: ImageConfig):
        self.docker = docker
        self.config = config

    async def _build_container(
        self: Self,
        environment: dict[str, str],
        name: str,
        command: List[str],
        shm_size: Optional[int] = None,
        ports: Optional[dict[str, int]] = None,
        labels: Optional[dict[str, str]] = None,
        volumes: Optional[VolumeConfig] = None,
    ):
        """
        See here for dockers API
        https://docs.docker.com/reference/api/engine/version/v1.47/#tag/Container/operation/ContainerCreate

        """
        volumes, labels, ports = volumes or {}, labels or {}, ports or {}

        host_config: JSONObject = {
            'Binds': [
                f"{host_path}:{volume_info['bind']}:{volume_info['mode']}"
                for host_path, volume_info in volumes.items()
            ],
            'PortBindings': {
                port: [{'HostPort': str(host_port)}]
                for port, host_port in ports.items()
            },
        }

        if shm_size is not None:
            host_config = { **host_config, 'ShmSize': shm_size }

        container = await self.docker.containers.create(config={
            'Image': f"{self.config.image_name}:{self.config.image_tag}",
            'Env': [f"{key}={value}" for key, value in environment.items()],
            'ExposedPorts': {port: {} for port in ports.keys()},
            'Labels': labels,
            'Cmd': command,
            'HostConfig': host_config
        }, name=name)

        await container.start()
        return container

    async def prepare(self: Self):
        import io
        import tarfile

        name, tag = self.config.image_name, self.config.image_tag
        file = self.config.dockerfile_path
        if await self._image_built():
            return

        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            tar.add(self.config.dockerfile_path, arcname="Dockerfile")
        tar_buffer.seek(0)

        await self.docker.images.build(fileobj=tar_buffer, tag=f"{name}:{tag}", encoding="gzip")

    async def nuke(self: Self):
        name, tag = self.config.image_name, self.config.image_tag
        containers = await self.docker.containers.list(filters={"ancestor": name})
        for container in containers:
            await container.delete(force=True)

        await self.docker.images.delete(f"{name}:{tag}", force=True)

    async def _image_built(self: Self):
        name, tag = self.config.image_name, self.config.image_tag
        try:
            await self.docker.images.inspect(f"{name}:{tag}")
            return True
        except DockerError:
            return False

class DockerContainer:
    logger = logging.getLogger(f'{__name__}.container')

    def __init__(self: Self,
                 docker: DockerClient,
                 image: DockerImage,
                 config: ContainerConfig):
        self.docker = docker
        self.image = image
        self.config = config

    async def prepare(self: Self, db_config: DatabaseConfig):
        try:
            await self._get_container()
        except DockerError:
            await self.image._build_container(
                environment={
                    'POSTGRES_DB': db_config.dbname,
                    'POSTGRES_USER': db_config.user,
                    'POSTGRES_PASSWORD': db_config.password,
                },
                ports={'5432/tcp': db_config.port},
                **self.config.docker_kwargs(),
            )

    async def start(self: Self):
        container = await self._get_container()
        await container.start()

    async def stop(self: Self, throw_if_not_found=True):
        try:
            container = await self._get_container()
        except DockerError as e:
            if throw_if_not_found:
                raise e
        await container.stop()

    async def clean(self: Self):
        try:
            container = await self._get_container()
            await container.stop()
            await container.delete()
        except DockerError:
            pass

        await asyncio.gather(*[
            container.delete(force=True)
            for container in await self.docker.containers.list(filters={
                "status": ["exited"],
                "label": [self.config.project_name],
            })
        ])

    async def _get_container(self: Self):
        return await self.docker.containers.get(self.config.container_name)

