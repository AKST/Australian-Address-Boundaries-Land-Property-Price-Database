from dataclasses import dataclass, field
from typing import Self

from lib.utility.sampling import AbstractSample

@dataclass
class IngestionSample(AbstractSample):
    parsed: float = field(default=0.0)
    ingested: float = field(default=0.0)

    @classmethod
    def empty(cls):
        return IngestionSample()

    def __str__(self: Self) -> str:
        if int(self.parsed):
            p = round(self.ingested / self.parsed, 6) * 100
            return f'(parsed: {self.parsed}, ingested: {self.ingested} [{p:.2f}%])'
        else:
            return f'(parsed: {self.parsed}, ingested: {self.ingested})'

    def __add__(self: Self, other: 'IngestionSample') -> 'IngestionSample':
        parsed = self.parsed + other.parsed
        ingested = self.ingested + other.ingested
        return IngestionSample(parsed=parsed, ingested=ingested)

    def __sub__(self: Self, other: 'IngestionSample') -> 'IngestionSample':
        parsed = self.parsed - other.parsed
        ingested = self.ingested - other.ingested
        return IngestionSample(parsed=parsed, ingested=ingested)

    def __truediv__(self: Self, other) -> 'IngestionSample':
        if isinstance(other, IngestionSample):
            parsed = self.parsed / other.parsed
            ingested = self.ingested / other.ingested
            return IngestionSample(parsed=parsed, ingested=ingested)
        if isinstance(other, float):
            parsed = self.parsed / other
            ingested = self.ingested / other
            return IngestionSample(parsed=parsed, ingested=ingested)
        raise TypeError(f'cannot divide IngestionSample by {type(other)}')

    def round(self: Self, n: int):
        return IngestionSample(parsed=round(self.parsed, n),
                               ingested=round(self.ingested, n))
