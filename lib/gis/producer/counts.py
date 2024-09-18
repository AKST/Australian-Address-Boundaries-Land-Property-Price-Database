from typing import Self, Optional, List, Tuple, Dict

_CountMap = Dict[str, Tuple[int, int]]

class ClauseCounts:
    _counts: _CountMap

    """
    This mostly exist for the purpose of tracking
    progress in the producer so people can actually
    see progress take place.
    """
    def __init__(self, counts: Optional[_CountMap] =None):
        self._counts = counts or {}

    def init_clause(self, clause, count):
        self._counts[clause] = (0, count)

    @staticmethod
    def from_list(ls: List[Tuple[int, str]]) -> Self:
        counts = { clause: (0, count) for count, clause in ls }
        return ClauseCounts(counts)

    def inc(self, clause: str, amount: int):
        progress, total = self._counts[clause]
        self._counts[clause] = (progress + amount, total)

    def progress(self, clause: str) -> int:
        return self._counts[clause]
