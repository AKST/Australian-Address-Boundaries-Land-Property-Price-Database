-- When ingesting this in events and source:
--
-- - legacy data
--   - `source_date` is `source_file.date_published`
--   - `CURRENT_DATE()` is `source_file.date_recorded`
--   - `basis_date_N` is `event.effective_date`
--
CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a_legacy (
    ps_row_a_legacy_id BIGSERIAL PRIMARY KEY,
    position bigint NOT NULL,
    file_path TEXT NOT NULL UNIQUE,
    year_of_sale INT NOT NULL,
    submitting_user_id TEXT,
    date_provided DATE NOT NULL
);

CREATE INDEX idx_file_path_ps_row_a_legacy
    ON nsw_vg_raw.ps_row_a_legacy(file_path);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_legacy (
    ps_row_b_legacy_id BIGSERIAL PRIMARY KEY,
    position bigint NOT NULL,
    file_path TEXT NOT NULL,
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
    zone_standard nsw_vg.zoning_standard,
    UNIQUE (file_path, position)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a_legacy_source(
    ps_row_a_legacy_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_a_legacy_id) REFERENCES nsw_vg_raw.ps_row_a_legacy(ps_row_a_legacy_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_legacy_source(
    ps_row_b_legacy_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_b_legacy_id) REFERENCES nsw_vg_raw.ps_row_b_legacy(ps_row_b_legacy_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE INDEX idx_ps_row_a_legacy_source_a ON nsw_vg_raw.ps_row_a_legacy_source(ps_row_a_legacy_id);
CREATE INDEX idx_ps_row_a_legacy_source_b ON nsw_vg_raw.ps_row_a_legacy_source(source_id);
CREATE INDEX idx_ps_row_b_legacy_source_a ON nsw_vg_raw.ps_row_b_legacy_source(ps_row_b_legacy_id);
CREATE INDEX idx_ps_row_b_legacy_source_b ON nsw_vg_raw.ps_row_b_legacy_source(source_id);

--
-- # Non Legacy
--

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a (
    ps_row_a_id BIGSERIAL PRIMARY KEY,
    position bigint NOT NULL,
    year_of_sale INT NOT NULL,
    file_path TEXT NOT NULL UNIQUE,
    file_type TEXT,
    district_code INT NOT NULL,
    date_provided DATE NOT NULL,
    submitting_user_id TEXT NOT NULL
);

CREATE INDEX idx_file_path_ps_row_a
    ON nsw_vg_raw.ps_row_a(file_path);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b (
    ps_row_b_id BIGSERIAL PRIMARY KEY,
    position bigint NOT NULL,
    file_path TEXT NOT NULL,
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
    dealing_number TEXT NOT NULL,
    UNIQUE (file_path, position)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_c (
    ps_row_c_id BIGSERIAL PRIMARY KEY,
    position bigint NOT NULL,
    file_path TEXT,
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    property_description TEXT,
    UNIQUE (file_path, position)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_d (
    ps_row_d_id BIGSERIAL PRIMARY KEY,
    position bigint NOT NULL,
    file_path TEXT,
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    participant nsw_vg.sale_participant NOT NULL,
    UNIQUE (file_path, position)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a_source(
    ps_row_a_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_a_id) REFERENCES nsw_vg_raw.ps_row_a(ps_row_a_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_source(
    ps_row_b_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_b_id) REFERENCES nsw_vg_raw.ps_row_b(ps_row_b_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_c_source(
    ps_row_c_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_c_id) REFERENCES nsw_vg_raw.ps_row_c(ps_row_c_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_d_source(
    ps_row_d_id bigint UNIQUE NOT NULL,
    source_id bigint UNIQUE NOT NULL,
    FOREIGN KEY (ps_row_d_id) REFERENCES nsw_vg_raw.ps_row_d(ps_row_d_id),
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE INDEX idx_ps_row_a_source_a ON nsw_vg_raw.ps_row_a_source(ps_row_a_id);
CREATE INDEX idx_ps_row_a_source_b ON nsw_vg_raw.ps_row_a_source(source_id);
CREATE INDEX idx_ps_row_b_source_a ON nsw_vg_raw.ps_row_b_source(ps_row_b_id);
CREATE INDEX idx_ps_row_b_source_b ON nsw_vg_raw.ps_row_b_source(source_id);
CREATE INDEX idx_ps_row_c_source_a ON nsw_vg_raw.ps_row_c_source(ps_row_c_id);
CREATE INDEX idx_ps_row_c_source_b ON nsw_vg_raw.ps_row_c_source(source_id);
CREATE INDEX idx_ps_row_d_source_a ON nsw_vg_raw.ps_row_d_source(ps_row_d_id);
CREATE INDEX idx_ps_row_d_source_b ON nsw_vg_raw.ps_row_d_source(source_id);

