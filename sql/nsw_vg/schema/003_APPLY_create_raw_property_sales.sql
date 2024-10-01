CREATE TABLE IF NOT EXISTS nsw_vg_stage.property_sale_data_row_a_legacy (
    file_path TEXT PRIMARY KEY,
    file_type TEXT,
    year_of_sale INT NOT NULL,
    date_provided DATE NOT NULL,
    submitting_user_id TEXT
);

CREATE TABLE IF NOT EXISTS nsw_vg_stage.property_sale_data_row_b_legacy (
    district_code INT NOT NULL,
    property_id INT,
    property_name TEXT,
    unit_number TEXT,
    house_number TEXT,
    street_name TEXT,
    suburb_name TEXT,
    postcode varchar(4),
    area FLOAT,
    area_type varchar(1),
    zone_code varchar(3),
    zone_standard nsw_vg.zoning_standard,
    comp_code TEXT,
    source TEXT,
    valuation_number TEXT,
    land_description TEXT,
    dimensions TEXT
);

CREATE TABLE IF NOT EXISTS nsw_vg_stage.property_sale_data_row_a_modern (
    file_path TEXT PRIMARY KEY,
    file_type TEXT,
    year_of_sale INT NOT NULL,
    date_provided DATE NOT NULL,
    submitting_user_id TEXT,
    district_code INT NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg_stage.property_sale_data_row_b_modern (
    district_code INT NOT NULL,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    property_id INT,
    property_name TEXT,
    unit_number TEXT,
    house_number TEXT,
    street_name TEXT,
    suburb_name TEXT,
    postcode varchar(4),
    area FLOAT,
    area_type varchar(1),
    zone_code varchar(3),
    zone_standard nsw_vg.zoning_standard,
    comp_code TEXT,
    settlement_date DATE,
    nature_of_property TEXT NOT NULL,
    primary_purpose TEXT,
    strata_lot_number INT,
    sale_code TEXT,
    interest_of_sale INT,
    dealing_number TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg_stage.property_sale_data_row_c (
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    property_description TEXT
);

CREATE TABLE IF NOT EXISTS nsw_vg_stage.property_sale_data_row_d (
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    participant nsw_vg.sale_participant NOT NULL
);
