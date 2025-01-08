from abc import ABC, abstractmethod
from dataclasses import dataclass
import psutil
from typing import Self

@dataclass
class SystemCapacity:
    @dataclass
    class DiscSpace:
        total_gb: int
        used_gb: int
        free_gb: int

    cpus: int
    memory_gb: int
    disc_space: 'SystemCapacity.DiscSpace'

class AbstactSystemCapabilities(ABC):
    @abstractmethod
    def capacity(self: Self) -> SystemCapacity:
        raise NotImplementedError()

class SystemCapablities(AbstactSystemCapabilities):
    def capacity(self: Self) -> SystemCapacity:
        cpus = psutil.cpu_count(logical=True)
        memory = psutil.virtual_memory().total // (1024 ** 3)
        disk_usage = psutil.disk_usage('/')
        return SystemCapacity(
            cpus=psutil.cpu_count(logical=True),
            memory_gb=psutil.virtual_memory().total // (1024 ** 3),
            disc_space=SystemCapacity.DiscSpace(
                total_gb=disk_usage.total,
                used_gb=disk_usage.used,
                free_gb=disk_usage.free,
            ),
        )

