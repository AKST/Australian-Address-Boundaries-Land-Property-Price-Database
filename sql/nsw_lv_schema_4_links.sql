CREATE SCHEMA IF NOT EXISTS nsw_valuer_general;

CREATE TABLE IF NOT EXISTS nsw_valuer_general.street_assoc (
    street_id BIGINT,
    gnaf_street_locality_pid varchar(15),
    
    PRIMARY KEY (street_id, gnaf_street_locality_pid),
    FOREIGN KEY (street_id) REFERENCES nsw_valuer_general.street (street_id),
    FOREIGN KEY (gnaf_street_locality_pid) REFERENCES gnaf.STREET_LOCALITY (street_locality_pid)
);

CREATE TABLE IF NOT EXISTS nsw_valuer_general.suburb_assoc (
    suburb_id INT,
    gnaf_locality_pid varchar(15),
    
    PRIMARY KEY (suburb_id, gnaf_locality_pid),
    FOREIGN KEY (suburb_id) REFERENCES nsw_valuer_general.suburb (suburb_id),
    FOREIGN KEY (gnaf_locality_pid) REFERENCES gnaf.LOCALITY (locality_pid)
);

CREATE TABLE IF NOT EXISTS nsw_valuer_general.property_assoc (
    property_id INT,
    gnaf_address_detail_pid varchar(15),

    PRIMARY KEY (property_id, gnaf_address_detail_pid),
    FOREIGN KEY (property_id) REFERENCES nsw_valuer_general.property (property_id),
    FOREIGN KEY (gnaf_address_detail_pid) REFERENCES gnaf.ADDRESS_DETAIL (address_detail_pid)
);