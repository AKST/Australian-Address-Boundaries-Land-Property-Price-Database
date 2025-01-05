from typing import List

from ..discovery import NswVgTarget
from .config import DiscoveryMode

def select_targets(mode: DiscoveryMode.T, all_targets: List[NswVgTarget]) -> List[NswVgTarget]:
    if not all_targets:
        return all_targets

    def sorted_targets():
        return sorted(all_targets, key=lambda k: k.datetime, reverse=True)

    def latest():
        return max(all_targets, key=lambda t: t.datetime)

    match mode:
        case DiscoveryMode.All():
            return all_targets
        case DiscoveryMode.Latest():
            return [latest()]
        case DiscoveryMode.EachYear():
            month = latest().datetime.month
            return [t for t in sorted_targets() if t.datetime.month == month]
        case DiscoveryMode.EachNthYear(n):
            month = latest().datetime.month
            targets = [t for t in sorted_targets() if t.datetime.month == month]
            return [t for i, t in enumerate(targets) if i % n == 0]
        case DiscoveryMode.TheseYears(year_set):
            month = latest().datetime.month
            return [
                t
                for t in sorted_targets()
                if t.datetime.month == month and t.datetime.year in year_set
            ]
        case other:
            raise TypeError(f'unknown mode {mode}')
