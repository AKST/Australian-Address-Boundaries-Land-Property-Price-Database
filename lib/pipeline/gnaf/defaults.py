from typing import Set, Dict
from .config import GnafState

ALL_STATES: Set[GnafState] = { 'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'OT', 'ACT' }

GNAF_STATE_INSTANCE_MAP: Dict[int, Set[GnafState]] = {
    1: ALL_STATES,
    2: { 'NSW' },
}
