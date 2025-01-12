CREATE TABLE IF NOT EXISTS abs.indigenous_region (
  indigenous_reg_code varchar(3) PRIMARY KEY,
  indigenous_reg_name varchar(50),
  state_code VARCHAR(1),
  area_sqkm DOUBLE PRECISION,
  geometry GEOMETRY(MultiPolygon, 7844),
  FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.indigenous_area (
  indigenous_area_code varchar(6) PRIMARY KEY,
  indigenous_area_name varchar(50),
  indigenous_reg_code varchar(3),
  state_code VARCHAR(1),
  area_sqkm DOUBLE PRECISION,
  geometry GEOMETRY(MultiPolygon, 7844),
  FOREIGN KEY (state_code) REFERENCES abs.state(state_code),
  FOREIGN KEY (indigenous_reg_code) REFERENCES abs.indigenous_region(indigenous_reg_code)
);

CREATE TABLE IF NOT EXISTS abs.indigenous_location (
  indigenous_loc_code varchar(8) PRIMARY KEY,
  indigenous_loc_name varchar(50),
  indigenous_area_code varchar(6),
  indigenous_reg_code varchar(3),
  state_code VARCHAR(1),
  area_sqkm DOUBLE PRECISION,
  geometry GEOMETRY(MultiPolygon, 7844),
  FOREIGN KEY (state_code) REFERENCES abs.state(state_code),
  FOREIGN KEY (indigenous_area_code) REFERENCES abs.indigenous_area(indigenous_area_code),
  FOREIGN KEY (indigenous_reg_code) REFERENCES abs.indigenous_region(indigenous_reg_code),
);
