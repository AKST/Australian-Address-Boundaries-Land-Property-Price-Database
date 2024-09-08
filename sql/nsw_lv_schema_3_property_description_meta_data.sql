CREATE SCHEMA IF NOT EXISTS nsw_valuer_general;

CREATE TABLE IF NOT EXISTS nsw_valuer_general.land_parcel_link (
    land_parcel_link_id SERIAL PRIMARY KEY,
    property_id INT NOT NULL,
    land_parcel_id TEXT NOT NULL,
    part BOOLEAN NOT NULL,
    FOREIGN KEY (property_id) REFERENCES nsw_valuer_general.property (property_id)
);