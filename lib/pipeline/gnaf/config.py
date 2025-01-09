from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Set, Literal, List

from lib.service.database import DatabaseConfig
from lib.service.static_environment import Target

GnafState = Literal['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'OT', 'ACT']

class PublicationTarget(Target, ABC):
    @property
    @abstractmethod
    def fk_constraints_sql(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def create_tables_sql(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def psv_dir(self) -> str:
        raise NotImplementedError()

@dataclass(frozen=True)
class Config:
    target: PublicationTarget
    states: Set[GnafState]
    workers: int
    worker_config: 'WorkerConfig'

@dataclass(frozen=True)
class WorkerTask:
    file_source: str
    table_name: str

@dataclass(frozen=True)
class WorkerConfig:
    db_config: DatabaseConfig
    db_poolsize: int
    batch_size: int
