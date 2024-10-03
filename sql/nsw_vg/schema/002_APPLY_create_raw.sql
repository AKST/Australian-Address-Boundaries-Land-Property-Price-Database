CREATE SCHEMA IF NOT EXISTS nsw_vg_raw;

CREATE TABLE IF NOT EXISTS nsw_vg_raw.land_value_row (
    district_code INT,
    district_name TEXT,
    property_id INT PRIMARY KEY,
    property_type TEXT,
    property_name TEXT,
    unit_number TEXT,
    house_number TEXT,
    street_name TEXT,
    suburb_name TEXT,
    postcode varchar(4),
    property_description TEXT,
    zone_code varchar(3) NOT NULL,
    area FLOAT,
    area_type TEXT,
    base_date_1 DATE,
    land_value_1 BIGINT,
    authority_1 TEXT,
    basis_1 TEXT,
    base_date_2 DATE,
    land_value_2 BIGINT,
    authority_2 TEXT,
    basis_2 TEXT,
    base_date_3 DATE,
    land_value_3 BIGINT,
    authority_3 TEXT,
    basis_3 TEXT,
    base_date_4 DATE,
    land_value_4 BIGINT,
    authority_4 TEXT,
    basis_4 TEXT,
    base_date_5 DATE,
    land_value_5 BIGINT,
    authority_5 TEXT,
    basis_5 TEXT,
    source_file_position INT NOT NULL,
    source_file_name TEXT NOT NULL,
    source_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.property_sale_data_row_a_legacy (
    file_path TEXT PRIMARY KEY,
    year_of_sale INT NOT NULL,
    submitting_user_id TEXT,
    date_provided DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.property_sale_data_row_b_legacy (
    district_code INT NOT NULL,
    source TEXT,
    valuation_number TEXT,
    property_id INT,
    unit_number TEXT,
    house_number TEXT,
    street_name TEXT,
    locality_name TEXT,
    postcode varchar(4),
    contract_date DATE,
    purchase_price FLOAT,
    land_description TEXT,
    area FLOAT,
    area_type varchar(1),
    dimensions TEXT,
    comp_code TEXT,
    zone_code varchar(3),
    zone_standard nsw_environment.zoning_standard NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.property_sale_data_row_a_modern (
    year_of_sale INT NOT NULL,
    file_path TEXT PRIMARY KEY,
    file_type TEXT,
    district_code INT NOT NULL,
    date_provided DATE NOT NULL,
    submitting_user_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.property_sale_data_row_b_modern (
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    property_name TEXT,
    unit_number TEXT,
    house_number TEXT,
    street_name TEXT,
    locality_name TEXT,
    postcode varchar(4),
    area FLOAT,
    area_type varchar(1),
    contract_date DATE,
    settlement_date DATE,
    purchase_price FLOAT,
    zone_code varchar(3),
    zone_standard nsw_environment.zoning_standard NOT NULL,
    nature_of_property TEXT NOT NULL,
    primary_purpose TEXT,
    strata_lot_number INT,
    comp_code TEXT,
    sale_code TEXT,
    interest_of_sale INT,
    dealing_number TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.property_sale_data_row_c (
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    property_description TEXT
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.property_sale_data_row_d (
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    participant nsw_vg.sale_participant NOT NULL
);
