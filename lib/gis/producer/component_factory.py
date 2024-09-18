from collections import namedtuple
from shapely.geometry import shape
from typing import Set, Any, Optional

# These are the components required to assemble a dataframe
_Components = namedtuple('_Components', ['geometry', 'attributes'])

class ComponentFactory:
    _seen: Set[Any] = set()
    _id_Field: str

    def __init__(self, id_field: str):
        self._id_field = id_field

    def from_feature(self, f) -> Optional[_Components]:
        obj_id = f['attributes'][self._id_field]
        if obj_id in self._seen:
            return

        geometry = f['geometry']
        if 'rings' in geometry:
            geom = shape({"type": "Polygon", "coordinates": geometry['rings']})
        elif 'paths' in geometry:
            geom = shape({"type": "LineString", "coordinates": geometry['paths']})
        else:
            geom = shape(geometry)

        self._seen.add(obj_id)
        return _Components(geom, f['attributes'])
