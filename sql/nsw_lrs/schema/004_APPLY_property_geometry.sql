CREATE TABLE IF NOT EXISTS nsw_lrs.property_bounds (
  property_id int NOT NULL,
  geometry GEOMETRY(MultiPolygon, 7844),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
);
