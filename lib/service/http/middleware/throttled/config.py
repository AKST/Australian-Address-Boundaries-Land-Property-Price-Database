from dataclasses import dataclass
from collections import namedtuple

@dataclass
class HostSemaphoreConfig:
    host: str
    limit: int
