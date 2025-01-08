--
-- ## Zones
--

SET session_replication_role = 'replica';

INSERT INTO nsw_lrs.zone_observation(
    source_id,
    effective_date,
    property_id,
    zone_code)
SELECT DISTINCT ON (effective_date, property_id)
       source_id,
       effective_date,
       property_id,
       zone_code
  FROM pg_temp.sourced_raw_property_sales_b
  WHERE zone_standard = 'ep&a_2006'
    AND NOT seen_in_land_values
    AND strata_lot_number IS NULL
  ORDER BY effective_date, property_id, date_provided DESC;

SET session_replication_role = 'origin';
SELECT meta.check_constraints('nsw_lrs', 'zone_observation');

