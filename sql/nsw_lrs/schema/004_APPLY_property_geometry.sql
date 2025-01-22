CREATE TABLE IF NOT EXISTS nsw_lrs.property_geometry (
  property_id int NOT NULL UNIQUE,
  geometry GEOMETRY(MultiPolygon, 7844),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
);

CREATE INDEX idx_property_id_property_geometry
  ON nsw_lrs.property_geometry(property_id);
CREATE INDEX idx_property_geometry_property_geometry
  ON nsw_lrs.property_geometry
  USING GIST (geometry);

CREATE TABLE IF NOT EXISTS nsw_lrs.base_parcel_geometry (
  base_parcel_id varchar(20) NOT NULL UNIQUE,
  geometry GEOMETRY(MultiPolygon, 7844),
  FOREIGN KEY (base_parcel_id) REFERENCES nsw_lrs.base_parcel(base_parcel_id)
);

CREATE INDEX idx_parcel_geometry_base_parcel_id
  ON nsw_lrs.base_parcel_geometry(base_parcel_id);

CREATE INDEX idx_parcel_geometry_geometry
  ON nsw_lrs.base_parcel_geometry
  USING GIST (geometry);

