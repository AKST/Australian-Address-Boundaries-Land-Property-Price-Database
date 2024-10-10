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
  property_name TEXT,
  unit_number TEXT,
  street_number TEXT,
  street_id INT,

  FOREIGN KEY (street_id) REFERENCES nsw_gnb.street(street_id),

  UNIQUE (property_name, unit_number, street_number, street_id)
);

