CREATE SCHEMA IF NOT EXISTS abs_main_structures;

CREATE TABLE IF NOT EXISTS abs_main_structures.state (
    state_code VARCHAR(1) PRIMARY KEY,
    state_name VARCHAR(100),
    geometry GEOMETRY(MultiPolygon, 7844)
);

CREATE TABLE IF NOT EXISTS abs_main_structures.gccsa (
    gcc_code VARCHAR(5) PRIMARY KEY,
    gcc_name VARCHAR(100),
    state_code VARCHAR(1),
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs_main_structures.sa4 (
    sa4_code VARCHAR(3) PRIMARY KEY,
    sa4_name VARCHAR(100),
    gcc_code VARCHAR(5),
    state_code VARCHAR(1),
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (gcc_code) REFERENCES abs_main_structures.gccsa(gcc_code),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs_main_structures.sa3 (
    sa3_code VARCHAR(5) PRIMARY KEY,
    sa3_name VARCHAR(100),
    sa4_code VARCHAR(3),
    gcc_code VARCHAR(5),
    state_code VARCHAR(1),
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (sa4_code) REFERENCES abs_main_structures.sa4(sa4_code),
    FOREIGN KEY (gcc_code) REFERENCES abs_main_structures.gccsa(gcc_code),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs_main_structures.sa2 (
    sa2_code VARCHAR(9) PRIMARY KEY,
    sa2_name VARCHAR(100),
    sa3_code VARCHAR(5),
    sa4_code VARCHAR(3),
    gcc_code VARCHAR(5),
    state_code VARCHAR(1),
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (sa3_code) REFERENCES abs_main_structures.sa3(sa3_code),
    FOREIGN KEY (sa4_code) REFERENCES abs_main_structures.sa4(sa4_code),
    FOREIGN KEY (gcc_code) REFERENCES abs_main_structures.gccsa(gcc_code),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs_main_structures.sa1 (
    sa1_code VARCHAR(11) PRIMARY KEY,
    sa2_code VARCHAR(9),
    sa3_code VARCHAR(5),
    sa4_code VARCHAR(3),
    gcc_code VARCHAR(5),
    state_code VARCHAR(1),
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (sa2_code) REFERENCES abs_main_structures.sa2(sa2_code),
    FOREIGN KEY (sa3_code) REFERENCES abs_main_structures.sa3(sa3_code),
    FOREIGN KEY (sa4_code) REFERENCES abs_main_structures.sa4(sa4_code),
    FOREIGN KEY (gcc_code) REFERENCES abs_main_structures.gccsa(gcc_code),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs_main_structures.meshblock (
    mb_code VARCHAR(15) PRIMARY KEY,
    mb_cat VARCHAR(50),
    sa1_code VARCHAR(11),
    sa2_code VARCHAR(9),
    sa3_code VARCHAR(5),
    sa4_code VARCHAR(3),
    gcc_code VARCHAR(5),
    state_code VARCHAR(1),
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(Polygon, 7844),
    FOREIGN KEY (sa1_code) REFERENCES abs_main_structures.sa1(sa1_code),
    FOREIGN KEY (sa2_code) REFERENCES abs_main_structures.sa2(sa2_code),
    FOREIGN KEY (sa3_code) REFERENCES abs_main_structures.sa3(sa3_code),
    FOREIGN KEY (sa4_code) REFERENCES abs_main_structures.sa4(sa4_code),
    FOREIGN KEY (gcc_code) REFERENCES abs_main_structures.gccsa(gcc_code),
    FOREIGN KEY (state_code) REFERENCES abs_main_structures.state(state_code)
);

CREATE INDEX IF NOT EXISTS idx_sa1_code ON abs_main_structures.meshblock (sa1_code);
CREATE INDEX IF NOT EXISTS idx_sa2_code ON abs_main_structures.meshblock (sa2_code);
CREATE INDEX IF NOT EXISTS idx_sa3_code ON abs_main_structures.meshblock (sa3_code);
CREATE INDEX IF NOT EXISTS idx_sa4_code ON abs_main_structures.meshblock (sa4_code);
CREATE INDEX IF NOT EXISTS idx_gcc_code ON abs_main_structures.meshblock (gcc_code);
CREATE INDEX IF NOT EXISTS idx_state_code ON abs_main_structures.meshblock (state_code);
CREATE INDEX IF NOT EXISTS idx_geometry ON abs_main_structures.meshblock USING GIST (geometry);
