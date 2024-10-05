CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a_legacy (
    position bigint NOT NULL,
    file_path TEXT PRIMARY KEY,
    year_of_sale INT NOT NULL,
    submitting_user_id TEXT,
    date_provided DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_legacy (
    position bigint NOT NULL,
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
    zone_code varchar(4),
    zone_standard nsw_vg.zoning_standard
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a (
    position bigint NOT NULL,
    year_of_sale INT NOT NULL,
    file_path TEXT PRIMARY KEY,
    file_type TEXT,
    district_code INT NOT NULL,
    date_provided DATE NOT NULL,
    submitting_user_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b (
    position bigint NOT NULL,
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

    -- EP&A act zoning will not be more than 3 characters
    -- long however, some of the legacy zones (which can
    -- appear in this data due to the fact in 2011 there
    -- is abit of overlap in data between the old and new
    -- format) can appear here in this column.
    zone_code varchar(4),
    zone_standard nsw_vg.zoning_standard,

    nature_of_property TEXT NOT NULL,
    primary_purpose TEXT,
    strata_lot_number INT,
    comp_code TEXT,
    sale_code TEXT,
    interest_of_sale INT,
    dealing_number TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_c (
    position bigint NOT NULL,
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    property_description TEXT
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_d (
    position bigint NOT NULL,
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    participant nsw_vg.sale_participant NOT NULL
);
