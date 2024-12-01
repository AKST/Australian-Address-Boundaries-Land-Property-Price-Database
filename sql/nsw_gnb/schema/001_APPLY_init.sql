CREATE SCHEMA IF NOT EXISTS nsw_gnb;

CREATE TABLE IF NOT EXISTS nsw_gnb.locality (
  locality_id SERIAL PRIMARY KEY,
  locality_name TEXT NOT NULL,

  UNIQUE (locality_name)
);

CREATE TABLE IF NOT EXISTS nsw_gnb.street (
  street_id SERIAL PRIMARY KEY,
  street_name TEXT NOT NULL,
  locality_id INT,

  FOREIGN KEY (locality_id) REFERENCES nsw_gnb.locality(locality_id),

  UNIQUE (street_name, locality_id)
);

CREATE TABLE IF NOT EXISTS nsw_gnb.address (
  address_id BIGSERIAL PRIMARY KEY,
  property_id int NOT NULL,
  strata_lot_number int,
  property_name TEXT,
  unit_number TEXT,
  street_number TEXT,
  street_id INT,
  locality_id INT,
  postcode varchar(4),

  normalised_property_id nsw_lrs.normalised_property_id
    GENERATED ALWAYS AS
    (ROW(property_id, COALESCE(strata_lot_number, -1))::nsw_lrs.normalised_property_id)
    STORED,

  FOREIGN KEY (street_id) REFERENCES nsw_gnb.street(street_id),
  FOREIGN KEY (locality_id) REFERENCES nsw_gnb.locality(locality_id),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id),
  UNIQUE (property_id, strata_lot_number, effective_date)
) INHERITS (meta.event);

CREATE INDEX idx_property_id_address
    ON nsw_gnb.address(normalised_property_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS nsw_gnb.full_property_address AS
WITH
  reduced AS (
    SELECT DISTINCT ON (normalised_property_id)
           address_id, property_id, strata_lot_number,
           normalised_property_id, effective_date
      FROM nsw_gnb.address
      ORDER BY normalised_property_id, effective_date DESC)
SELECT DISTINCT ON (property_id, strata_lot_number)
       a.property_id,
       a.strata_lot_number,
       a.normalised_property_id,
       property_name,
       unit_number,
       street_number,
       street_name,
       locality_name,
       postcode
  FROM reduced
  LEFT JOIN nsw_gnb.address a USING (address_id)
  LEFT JOIN nsw_gnb.street USING (street_id, locality_id)
  LEFT JOIN nsw_gnb.locality USING (locality_id);

