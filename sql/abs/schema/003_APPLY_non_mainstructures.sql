CREATE TABLE IF NOT EXISTS abs.localities (
    locality_id VARCHAR(5) PRIMARY KEY,
    locality_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.state_electoral_division_2021 (
    electorate_id VARCHAR(5) PRIMARY KEY,
    electorate_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.state_electoral_division_2022 (
    electorate_id VARCHAR(5) PRIMARY KEY,
    electorate_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.state_electoral_division_2024 (
    electorate_id VARCHAR(5) PRIMARY KEY,
    electorate_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.federal_electoral_division_2021 (
    electorate_id VARCHAR(5) PRIMARY KEY,
    electorate_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.lga_2021 (
    lga_id VARCHAR(5) PRIMARY KEY,
    lga_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.lga_2022 (
    lga_id VARCHAR(5) PRIMARY KEY,
    lga_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.lga_2023 (
    lga_id VARCHAR(5) PRIMARY KEY,
    lga_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.lga_2024 (
    lga_id VARCHAR(5) PRIMARY KEY,
    lga_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.post_code (
    post_code VARCHAR(4) PRIMARY KEY,
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844)
);

-- https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/non-abs-structures/destination-zones
CREATE TABLE IF NOT EXISTS abs.dzn (
    dzn_code VARCHAR(11) PRIMARY KEY,
    sa2_code VARCHAR(9),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (sa2_code) REFERENCES abs.sa2(sa2_code),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE INDEX idx_geometry_abs_lga_2021 ON abs.lga_2021 USING GIST (geometry);
CREATE INDEX idx_geometry_abs_lga_2022 ON abs.lga_2022 USING GIST (geometry);
CREATE INDEX idx_geometry_abs_lga_2023 ON abs.lga_2023 USING GIST (geometry);
CREATE INDEX idx_geometry_abs_lga_2024 ON abs.lga_2024 USING GIST (geometry);

