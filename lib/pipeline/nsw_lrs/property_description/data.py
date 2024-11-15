from dataclasses import dataclass
from typing import Optional, List, Self

@dataclass
class PropertyDescription:
    free_text: str
    parcels: 'PropertyLandParcels'
    permits: 'Permits'

@dataclass
class Permits:
    enclosure_permits: List[str]

@dataclass
class PropertyLandParcels:
    complete: List['LandParcel']
    partial: List['LandParcel']

    @property
    def all(self: Self) -> List['LandParcel']:
        return list(set(self.complete + self.partial))

@dataclass(frozen=True)
class LandParcel:
    id: str
    lot: str
    section: Optional[str]
    plan: str
