from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Set, Literal

from lib.service.static_environment import Target
from lib.pipeline.gnaf_2.config import PublicationTarget, GnafState
from lib.pipeline.gnaf_2 import GnafPublicationTarget

@dataclass
class GnafConfig:
    target: PublicationTarget
    states: Set[GnafState]

