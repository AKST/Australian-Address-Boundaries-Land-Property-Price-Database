import time
import psycopg
import subprocess
from typing import Self

from docker.errors import NotFound, ImageNotFound
from lib import notebook_constants as nc
from sqlalchemy import create_engine

from lib.gnaf.discovery import GnafPublicationTarget


def pg_url(conf, dbname):
    return f"postgresql+psycopg2://{conf['user']}:{conf['password']}@{conf['host']}:{conf['port']}/{dbname}"

class GnafImage:
    def __init__(self: Self, docker, image_name, image_tag, dockerfile_path):
        self.docker = docker
        self.image_name = image_name
        self.image_tag = image_tag
        self.dockerfile_path = dockerfile_path

    @staticmethod
    def create(client=None, name=None, tag=None, dockerfile=None):
        import docker

        return GnafImage(
            client or docker.from_env(),
            name or nc.gnaf_image_name,
            tag or nc.gnaf_image_tag,
            dockerfile or "./dockerfiles/postgres_2")

    def build_container(self: Self, **kwargs):
        self.docker.containers.run(
            image=f"{self.image_name}:{self.image_tag}",
            **kwargs,
        )

    def prepare(self: Self):
        if not self._image_built():
            self.docker.images.build(
                path=self.dockerfile_path,
                tag=f"{self.image_name}:{self.image_tag}"
            )

    def nuke(self: Self):
        f = {"ancestor": self.image_name}
        for container in self.docker.containers.list(all=True, filters=f):
            container.stop()
            container.remove()
        self.docker.images.remove(image=f"{self.image_name}:{self.image_tag}", force=True)

    def _image_built(self: Self):
        try:
            self.docker.images.get(f"{self.image_name}:{self.image_tag}")
            return True
        except ImageNotFound:
            return False


class GnafContainer:
    def __init__(self: Self, docker, container_name, image, project_name):
        self.docker = docker
        self.container_name = container_name
        self.image = image
        self.project_name = project_name

    @staticmethod
    def create(client=None, container_name=None, image=None, project_name=None):
        import docker

        return GnafContainer(
            client or docker.from_env(),
            container_name or nc.gnaf_container,
            image or GnafImage.create(),
            project_name or nc.gnaf_docker_project_label)

    def prepare(self: Self, conf, dbname):
        try:
            self._get_container()
        except NotFound:
            self.image.build_container(
                name=self.container_name,
                labels={"project": self.project_name},
                environment={
                    'POSTGRES_DB': dbname,
                    'POSTGRES_USER': conf['user'],
                    'POSTGRES_PASSWORD': conf['password'],
                },
                ports={'5432/tcp': conf['port']},
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

        f = {"status": "exited", "label": self.project_name}
        for container in self.docker.containers.list(all=True, filters=f):
            container.stop()
            container.remove()

    def _get_container(self: Self):
        return self.docker.containers.get(self.container_name)

class GnafDb:
    def __init__(self: Self, conf, dbname):
        self.conf = conf
        self.dbname = dbname

    @staticmethod
    def create(conf=None, dbname=None):
        from lib import notebook_constants as nc
        return GnafDb(conf or nc.gnaf_dbconf, dbname or nc.gnaf_dbname)

    def engine(self: Self):
        return create_engine(pg_url(self.conf, self.dbname))

    def connect(self: Self) -> psycopg.Connection:
        return psycopg.connect(dbname=self.dbname, **self.conf)

    async def async_connect(self: Self) -> psycopg.AsyncConnection:
        return await psycopg.AsyncConnection.connect(dbname=self.dbname, **self.conf)

    def wait_till_running(self: Self, interval=5, timeout=60):
        start_time = time.time()
        while True:
            try:
                conn = psycopg.connect(dbname='postgres', **self.conf)
                conn.close()
                break
            except psycopg.OperationalError as e:
                if time.time() - start_time > timeout:
                    raise e
                time.sleep(interval)

    def init_schema(self: Self, gnaf_target):
        for script in [
            gnaf_target.create_tables_sql,
            gnaf_target.fk_constraints_sql,
            *nc.gnaf_all_scripts,
        ]:
            with self.connect() as conn:
                cursor = conn.cursor()
                with open(script, 'r') as sql_file:
                    print(f"running {script}")
                    cursor.execute(sql_file.read())
                cursor.close()
