from dataclasses import dataclass
from shapely.geometry import shape
from typing import Set, Any, Dict

@dataclass
class ComponentPair:
    geometry: Any
    attributes: Dict[str, Any]

class ComponentFactory:
    _seen: Set[Any] = set()
    _id_Field: str

    def __init__(self, id_field: str):
        self._id_field = id_field

    def from_feature(self, feature: Dict[str, Any]) -> ComponentPair | None:
        obj_id = feature['attributes'][self._id_field]

        if obj_id in self._seen:
            return None

        geometry = feature['geometry']
        if 'rings' in geometry:
            geom = shape({"type": "Polygon", "coordinates": geometry['rings']})
        elif 'paths' in geometry:
            geom = shape({"type": "LineString", "coordinates": geometry['paths']})
        else:
            geom = shape(geometry)

        self._seen.add(obj_id)
        return ComponentPair(geom, feature['attributes'])
