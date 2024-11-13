from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Set, Literal

from lib.service.static_environment import Target

GnafState = Literal['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'OT', 'ACT']

class GnafPublicationTarget(Target, ABC):
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

@dataclass
class GnafConfig:
    target: GnafPublicationTarget
    states: Set[GnafState]

