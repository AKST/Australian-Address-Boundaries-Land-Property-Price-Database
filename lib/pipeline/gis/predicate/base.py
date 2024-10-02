from dataclasses import dataclass, field
from typing import Iterator, Self

@dataclass
class PredicateFunction:
    field: str
    kind: str

    def default_param(self, scope):
        raise ValueError(f"no default avalilable for {self.kind}")

class PredicateParam:
    kind: str
    scope: str

    def __init__(self, kind, scope):
        self.kind = kind
        self.scope = scope

    def apply(self, field) -> str:
        raise NotImplementedError()

    def can_shard(self) -> bool:
        raise NotImplementedError()

    def shard(self) -> Iterator[Self]:
        raise NotImplementedError()

    def can_cache(self) -> bool:
        raise NotImplementedError()

