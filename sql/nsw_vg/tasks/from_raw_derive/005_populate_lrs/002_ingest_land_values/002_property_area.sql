INSERT INTO nsw_lrs.property_area(source_id, effective_date, property_id, sqm_area)
SELECT source_id, effective_date, property_id, pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_land_values
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL;

