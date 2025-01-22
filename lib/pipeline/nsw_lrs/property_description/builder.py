from typing import Self, List, Literal

from .data import Folio, PropertyFolios, PropertyDescription, Permits

PermitKind = Literal['enclosure']

class PropertyDescriptionBuilder:
    def __init__(self: Self) -> None:
        self.permits = Permits(enclosure_permits=[])
        self.complete_folios: List[Folio] = []
        self.partial_folios: List[Folio] = []

    def add_folio(self: Self, folio: Folio, partial: bool) -> None:
        dest = self.partial_folios if partial else self.complete_folios
        dest.append(folio)

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
            folios=PropertyFolios(
                complete=self.complete_folios,
                partial=self.partial_folios,
            ),
        )

