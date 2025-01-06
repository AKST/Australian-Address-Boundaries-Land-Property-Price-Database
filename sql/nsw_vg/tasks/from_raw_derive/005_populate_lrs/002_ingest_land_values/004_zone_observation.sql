INSERT INTO nsw_lrs.zone_observation(
    source_id,
    effective_date,
    property_id,
    zone_code)
SELECT source_id,
       effective_date,
       property_id,
       zone_code
  FROM nsw_vg_raw.land_value_row
  LEFT JOIN nsw_vg_raw.land_value_row_complement USING (property_id, source_date)
  WHERE zone_standard = 'ep&a_2006';

