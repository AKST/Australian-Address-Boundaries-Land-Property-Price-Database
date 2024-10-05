CREATE SCHEMA IF NOT EXISTS nsw_property;

CREATE TABLE IF NOT EXISTS nsw_property.property (
  id INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS nsw_property.zone (
  property_id INT NOT NULL,
  zone_id INT NOT NULL,
  UNIQUE (property_id, zone_id, effective_date),
  FOREIGN KEY (property_id) REFERENCES nsw_property.property(id),
  FOREIGN KEY (zone_id) REFERENCES nsw_planning.epa_2006_zone(zone_id)
) inherits (meta.event);

CREATE TABLE IF NOT EXISTS nsw_property.property_parcel (
  property_id INT NOT NULL,
  parcel_id BIGINT NOT NULL,
  part BOOLEAN NOT NULL,
  UNIQUE (property_id, parcel_id, effective_date),

  FOREIGN KEY (property_id) REFERENCES nsw_property.property(id),
  FOREIGN KEY (parcel_id) REFERENCES nsw_lrs.parcel(id)
) inherits (meta.event);

CREATE TABLE IF NOT EXISTS nsw_property.dimensions (
  property_id INT NOT NULL,
  dimensions TEXT NOT NULL,
  UNIQUE (dimensions, property_id, effective_date),

  FOREIGN KEY (property_id) REFERENCES nsw_property.property(id)
) inherits (meta.event);

CREATE TABLE IF NOT EXISTS nsw_property.description (
  property_id INT NOT NULL,
  description TEXT NOT NULL,

  UNIQUE (description, property_id),
  FOREIGN KEY (property_id) REFERENCES nsw_property.property(id)
) inherits (meta.event);
