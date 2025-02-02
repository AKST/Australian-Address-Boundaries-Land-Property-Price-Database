from typing import List, Union, TypeVar

from ..discovery import NswVgTarget
from .config import DiscoveryMode, ByoLandValue

_T = TypeVar('_T', bound=Union[NswVgTarget, ByoLandValue])

def select_targets(mode: DiscoveryMode.T, all_targets: List[_T]) -> List[_T]:
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
        case DiscoveryMode.EachNthYear(n, include_first):
            month = latest().datetime.month
            targets_sorted = sorted_targets()
            targets = [t for t in targets_sorted if t.datetime.month == month]
            targets = [t for i, t in enumerate(targets) if i % n == 0]
            if include_first and targets[-1] != targets_sorted[-1]:
                targets.append(targets_sorted[-1])
            return targets
        case DiscoveryMode.TheseYears(year_set):
            month = latest().datetime.month
            return [
                t
                for t in sorted_targets()
                if t.datetime.month == month and t.datetime.year in year_set
            ]
        case other:
            raise TypeError(f'unknown mode {mode}')
