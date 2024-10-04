--
-- These districts are a valuer general specific
-- boundary, some of the them are associated with
-- local government areas but not all are.
--
CREATE TABLE IF NOT EXISTS nsw_vg.district (
  district_code INT PRIMARY KEY,
  district_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg.suburb (
  suburb_id SERIAL PRIMARY KEY,
  suburb_name TEXT NOT NULL,
  district_code INT NOT NULL,
  UNIQUE (suburb_name, district_code)
);

CREATE TABLE IF NOT EXISTS nsw_vg.street (
  street_id SERIAL PRIMARY KEY,
  street_name TEXT NOT NULL,
  district_code INT,
  suburb_id INT,
  postcode varchar(4),
  UNIQUE (street_name, suburb_id, postcode)
);

CREATE TABLE IF NOT EXISTS nsw_vg.address (
  address_id SERIAL PRIMARY KEY,
  district_code INT NOT NULL,
  unit_number TEXT,
  house_number TEXT,
  street_id INT,
  suburb_id INT NOT NULL,
  postcode varchar(4),
  UNIQUE (district_code, unit_number, house_number, street_id, suburb_id, postcode)
);
