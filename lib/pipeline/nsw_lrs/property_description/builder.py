from typing import Self, List, Literal

from .data import LandParcel, PropertyLandParcels, PropertyDescription, Permits

PermitKind = Literal['enclosure']

class PropertyDescriptionBuilder:
    def __init__(self: Self) -> None:
        self.permits = Permits(enclosure_permits=[])
        self.complete_parcels: List[LandParcel] = []
        self.partial_parcels: List[LandParcel] = []

    def add_parcel(self: Self, parcel: LandParcel, partial: bool) -> None:
        dest = self.partial_parcels if partial else self.complete_parcels
        dest.append(parcel)

    def add_permit(self: Self, kind: PermitKind, id: str) -> None:
        match kind:
            case 'enclosure':
                self.permits.enclosure_permits.append(id)
            case True:
                raise ValueError(f'unknown permit {kind}')

    def create(self: Self, full_text: str) -> PropertyDescription:
        return PropertyDescription(
            free_text=full_text,
            permits=self.permits,
            parcels=PropertyLandParcels(
                complete=self.complete_parcels,
                partial=self.partial_parcels,
            ),
        )

