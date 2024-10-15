--
-- These districts are a valuer general specific
-- boundary, some of the them are associated with
-- local government areas but not all are.
--
CREATE TABLE IF NOT EXISTS nsw_vg.valuation_district (
  valuation_district_code INT PRIMARY KEY,
  valuation_district_name TEXT
);

CREATE UNIQUE INDEX nsw_vg_valuation_district_unique_name
    ON nsw_vg.valuation_district(valuation_district_name)
    WHERE valuation_district_name IS NOT NULL;

--
-- # Observed Zoning
--
-- The department of planning observes zoning
--
CREATE TABLE IF NOT EXISTS nsw_vg.observed_zoning (
  property_id INT NOT NULL,
  zone_code varchar(4) NOT NULL,

  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id),
  FOREIGN KEY (zone_code) REFERENCES nsw_planning.epa_2006_zone(zone_code)
) inherits (meta.event);

