from dataclasses import dataclass, field
from shapely.geometry import shape
from typing import Set, List, Any, Dict, Self, Optional

import geopandas as gpd

from lib.pipeline.gis.request import GisProjection

@dataclass
class ComponentPair:
    geometry: Any
    attributes: Dict[str, Any]

@dataclass
class FactoryState:
    """
    This largely exist to capture duplicate data.
    """
    id_field: str
    projection: GisProjection
    seen: Set[Any] = field(default_factory=lambda: set())

@dataclass
class DataframeBuilder:
    state: FactoryState
    components: List[ComponentPair] = field(default_factory=lambda: [])

    def consume_feature(self, feature: Dict[str, Any]):
        obj_id = feature['attributes'][self.state.id_field]

        if obj_id in self.state.seen:
            return

        geometry = feature['geometry']
        if 'rings' in geometry:
            geom = shape({"type": "Polygon", "coordinates": geometry['rings']})
        elif 'paths' in geometry:
            geom = shape({"type": "LineString", "coordinates": geometry['paths']})
        else:
            geom = shape(geometry)

        self.state.seen.add(obj_id)
        self.components.append(ComponentPair(geom, feature['attributes']))

    def get_dataframe(self: Self,
                      epsg_crs: int) -> gpd.GeoDataFrame:
        geometries, attributes = [], []
        for c in self.components:
            geometries.append(c.geometry)
            attributes.append(c.attributes)

        if not attributes:
            return gpd.GeoDataFrame()

        return gpd.GeoDataFrame(
            attributes,
            geometry=geometries,
            crs=f"EPSG:{epsg_crs}",
        ).rename(columns={
            f.name: f.rename
            for f in self.state.projection.get_fields()
            if f.rename
        })


@dataclass
class DataframeFactory:
    _state: FactoryState

    @staticmethod
    def create(id_field: str, projection: GisProjection) -> 'DataframeFactory':
        return DataframeFactory(FactoryState(id_field, projection))

    def build(self: Self,
              epsg_crs: int,
              features: List[Dict[str, Any]]) -> gpd.GeoDataFrame:
        builder = DataframeBuilder(self._state)
        for feature in features:
            builder.consume_feature(feature)
        return builder.get_dataframe(epsg_crs)

