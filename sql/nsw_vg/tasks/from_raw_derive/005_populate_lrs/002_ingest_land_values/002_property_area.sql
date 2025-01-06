INSERT INTO nsw_lrs.property_area(source_id, effective_date, property_id, sqm_area)
SELECT source_id, effective_date, property_id, pg_temp.sqm_area(area, area_type)
  FROM nsw_vg_raw.land_value_row
  LEFT JOIN nsw_vg_raw.land_value_row_complement USING (property_id, source_date)
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL;

