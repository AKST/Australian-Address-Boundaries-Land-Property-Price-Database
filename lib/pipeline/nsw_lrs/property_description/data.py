from dataclasses import dataclass
from typing import Optional, List, Self

@dataclass
class PropertyDescription:
    free_text: str
    folios: 'PropertyFolios'
    permits: 'Permits'

@dataclass
class Permits:
    enclosure_permits: List[str]

@dataclass
class PropertyFolios:
    complete: List['Folio']
    partial: List['Folio']

    @property
    def all(self: Self) -> List['Folio']:
        return list(set(self.complete + self.partial))

@dataclass(frozen=True)
class Folio:
    id: str
    lot: str
    section: Optional[str]
    plan: str
