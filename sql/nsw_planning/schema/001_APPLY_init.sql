CREATE SCHEMA IF NOT EXISTS nsw_planning;

CREATE TABLE IF NOT EXISTS nsw_planning.epa_2006_zone (
  zone_id SERIAL PRIMARY KEY,
  zone_code varchar(3) NOT NULL UNIQUE
  -- TODO Add local government id
);
