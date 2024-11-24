--
-- # Record Zones
--
-- We can do this before setting up meta data, this
-- is mostly for establishing these identifiers.
--

INSERT INTO nsw_planning.epa_2006_zone(zone_code)
SELECT * FROM
   (SELECT zone_code FROM nsw_vg_raw.land_value_row
     WHERE zone_standard = 'ep&a_2006'
     UNION
    SELECT zone_code FROM nsw_vg_raw.ps_row_b
     WHERE zone_standard = 'ep&a_2006') as t
   ON CONFLICT (zone_code) DO NOTHING;

--
-- # Establishing NSW LRS Primary Purposes
--

INSERT INTO nsw_lrs.primary_purpose(primary_purpose)
SELECT DISTINCT primary_purpose
  FROM nsw_vg_raw.ps_row_b
  WHERE primary_purpose IS NOT NULL
  ON CONFLICT (primary_purpose) DO NOTHING;

--
-- # Establish Distriction
--

INSERT INTO nsw_vg.valuation_district(valuation_district_code, valuation_district_name)
SELECT c.district_code, n.district_name
  FROM
   (SELECT district_code FROM nsw_vg_raw.land_value_row UNION
    SELECT district_code FROM nsw_vg_raw.ps_row_b_legacy UNION
    SELECT district_code FROM nsw_vg_raw.ps_row_a UNION
    SELECT district_code FROM nsw_vg_raw.ps_row_b) as c
  LEFT JOIN (SELECT DISTINCT ON (district_code) district_code, district_name
               FROM nsw_vg_raw.land_value_row
              WHERE district_name IS NOT NULL) as n USING (district_code)
    ON CONFLICT (valuation_district_code) DO NOTHING;
