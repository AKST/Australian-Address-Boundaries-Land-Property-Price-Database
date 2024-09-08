CREATE SCHEMA IF NOT EXISTS nsw_valuer_general;

CREATE TABLE IF NOT EXISTS nsw_valuer_general.source_file (
    source_file_id SERIAL PRIMARY KEY,
    source_file_name TEXT NOT NULL,
    source_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_valuer_general.source (
    source_id SERIAL PRIMARY KEY,
    source_file_id INT NOT NULL,
    source_file_position BIGINT NOT NULL,
    FOREIGN KEY (source_file_id) REFERENCES nsw_valuer_general.source_file (source_file_id)
);

CREATE TABLE IF NOT EXISTS nsw_valuer_general.district (
    district_code INT PRIMARY KEY,
    district_name TEXT UNIQUE NOT NULL 
);

CREATE TABLE IF NOT EXISTS nsw_valuer_general.suburb (
    suburb_id SERIAL PRIMARY KEY,
    suburb_name TEXT NOT NULL,
    district_code INT,
    UNIQUE (suburb_name, district_code),
    FOREIGN KEY (district_code) REFERENCES nsw_valuer_general.district (district_code)
);

CREATE TABLE IF NOT EXISTS nsw_valuer_general.street (
    street_id SERIAL PRIMARY KEY,
    street_name TEXT NOT NULL,
    district_code INT,
    suburb_id INT,
    postcode varchar(4),
    UNIQUE (street_name, suburb_id, postcode),
    FOREIGN KEY (suburb_id) REFERENCES nsw_valuer_general.suburb (suburb_id),
    FOREIGN KEY (district_code) REFERENCES nsw_valuer_general.district (district_code)
);

CREATE TABLE IF NOT EXISTS nsw_valuer_general.property (
    property_id INT PRIMARY KEY,
    district_code INT NOT NULL,
    property_type TEXT NOT NULL,
    property_name TEXT,
    unit_number TEXT,
    house_number TEXT,
    street_id INT,
    suburb_id INT NOT NULL,
    postcode varchar(4),
    zone_code varchar(3) NOT NULL,
    area FLOAT,
    source_id BIGINT NOT NULL,
    
    FOREIGN KEY (street_id) REFERENCES nsw_valuer_general.street (street_id),
    FOREIGN KEY (suburb_id) REFERENCES nsw_valuer_general.suburb (suburb_id),
    FOREIGN KEY (district_code) REFERENCES nsw_valuer_general.district (district_code),
    FOREIGN KEY (source_id) REFERENCES nsw_valuer_general.source (source_id)
);

CREATE TABLE IF NOT EXISTS nsw_valuer_general.property_description (
    property_id INT PRIMARY KEY,
    property_description TEXT,
    FOREIGN KEY (property_id) REFERENCES nsw_valuer_general.property (property_id)
);

CREATE TABLE IF NOT EXISTS nsw_valuer_general.valuations (
    property_id INT NOT NULL,
    base_date DATE NOT NULL,
    land_value BIGINT,
    authority TEXT,
    basis TEXT,
    
    PRIMARY KEY (property_id, base_date),
    FOREIGN KEY (property_id) REFERENCES nsw_valuer_general.property (property_id)
);