from lib.service.http import HostSemaphoreConfig
from .constants import VG_LV_HOST, VG_PS_HOST

THROTTLE_CONFIG = [
    HostSemaphoreConfig(VG_LV_HOST, 8),
    HostSemaphoreConfig(VG_PS_HOST, 8),
]
