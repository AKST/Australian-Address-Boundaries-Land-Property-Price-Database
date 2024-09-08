CREATE SCHEMA IF NOT EXISTS non_abs_main_structures;

CREATE TABLE IF NOT EXISTS non_abs_main_structures.localities (
    locality_id VARCHAR(5) PRIMARY KEY,
    locality_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS non_abs_main_structures.state_electoral_division_2021 (
    electorate_id VARCHAR(5) PRIMARY KEY,
    electorate_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS non_abs_main_structures.state_electoral_division_2022 (
    electorate_id VARCHAR(5) PRIMARY KEY,
    electorate_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS non_abs_main_structures.state_electoral_division_2024 (
    electorate_id VARCHAR(5) PRIMARY KEY,
    electorate_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS non_abs_main_structures.federal_electoral_division_2021 (
    electorate_id VARCHAR(5) PRIMARY KEY,
    electorate_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS non_abs_main_structures.lga_2021 (
    lga_id VARCHAR(5) PRIMARY KEY,
    lga_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS non_abs_main_structures.lga_2022 (
    lga_id VARCHAR(5) PRIMARY KEY,
    lga_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS non_abs_main_structures.lga_2023 (
    lga_id VARCHAR(5) PRIMARY KEY,
    lga_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS non_abs_main_structures.lga_2024 (
    lga_id VARCHAR(5) PRIMARY KEY,
    lga_name VARCHAR(50),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS non_abs_main_structures.post_code (
    post_code VARCHAR(4) PRIMARY KEY,
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844)
);


-- https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/non-abs-structures/destination-zones
CREATE TABLE IF NOT EXISTS abs_main_structures.dzn (
    dzn_code VARCHAR(11) PRIMARY KEY,
    sa2_code VARCHAR(9),
    state_code VARCHAR(1),
    in_australia BOOLEAN,
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (sa2_code) REFERENCES abs_main_structures.sa2(sa2_code),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

