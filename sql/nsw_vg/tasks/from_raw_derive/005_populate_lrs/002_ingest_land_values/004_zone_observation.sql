INSERT INTO nsw_lrs.zone_observation(
    source_id,
    effective_date,
    property_id,
    zone_code)
SELECT source_id,
       effective_date,
       property_id,
       zone_code
  FROM pg_temp.sourced_raw_land_values
  WHERE zone_standard = 'ep&a_2006';

