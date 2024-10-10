--
-- These districts are a valuer general specific
-- boundary, some of the them are associated with
-- local government areas but not all are.
--
CREATE TABLE IF NOT EXISTS nsw_vg.valuation_district (
  district_code INT PRIMARY KEY,
  district_name TEXT UNIQUE
);

CREATE UNIQUE INDEX nsw_vg_valuation_district_unique_name
    ON nsw_vg.valuation_district(district_name) WHERE district_name IS NOT NULL;

--
-- # Observed Zoning
--
-- The department of planning observes zoning
--
CREATE TABLE IF NOT EXISTS nsw_vg.observed_zoning (
  property_id INT NOT NULL,
  zone_code varchar(4) NOT NULL,

  UNIQUE (property_id, zone_code, effective_date),

  FOREIGN KEY (property_id) REFERENCES nsw_property.property(id),
  FOREIGN KEY (zone_code) REFERENCES nsw_planning.epa_2006_zone(zone_code)
) inherits (meta.event);

