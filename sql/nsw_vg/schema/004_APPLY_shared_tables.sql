CREATE TABLE IF NOT EXISTS nsw_vg.district (
    district_code INT PRIMARY KEY,
    district_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg.zone (
    zone_id SERIAL PRIMARY KEY,
    zone_code varchar(3) NOT NULL,
    zone_std nsw_vg.zoning_standard NOT NULL,
    district_code INT NOT NULL,
    UNIQUE (zone_code, zone_std, district_code)
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

CREATE TABLE IF NOT EXISTS nsw_vg.nsw_property_description (
    nsw_property_description_id SERIAL PRIMARY KEY,
    nsw_property_description TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS nsw_vg.property (
    property_id SERIAL PRIMARY KEY,
    nsw_property_id INT NOT NULL,
    nsw_property_description_id INT,
    address_id INT NOT NULL
);
