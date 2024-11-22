CREATE SCHEMA IF NOT EXISTS nsw_planning;

CREATE TABLE IF NOT EXISTS nsw_planning.epa_2006_zone (
  zone_code varchar(4) PRIMARY KEY
);

ALTER TABLE nsw_lrs.zone_observation
  ADD CONSTRAINT fk_nsw_lrs_zone_observation_zone_code
  FOREIGN KEY (zone_code) REFERENCES nsw_planning.epa_2006_zone(zone_code);
