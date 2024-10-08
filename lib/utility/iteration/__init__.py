from typing import Any, List, Tuple, TypeVar, Sequence

A = TypeVar('A')
B = TypeVar('B')

def partition(pairs: Sequence[Tuple[A, B]]) -> Tuple[List[A], List[B]]:
    return list(map(list, zip(*pairs))) # type: ignore
