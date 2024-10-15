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
