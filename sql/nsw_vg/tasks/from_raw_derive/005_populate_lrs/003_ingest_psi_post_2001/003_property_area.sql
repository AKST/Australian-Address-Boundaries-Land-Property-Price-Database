--
-- ## Ingest Property Area
--

SET session_replication_role = 'replica';

INSERT INTO nsw_lrs.property_area(
    source_id,
    effective_date,
    property_id,
    strata_lot_number,
    sqm_area)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    source_id,
    effective_date,
    property_id,
    strata_lot_number,
    pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_property_sales_b
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
    AND NOT seen_in_land_values
    AND property_id IS NOT NULL
  ORDER BY effective_date,
           property_id,
           strata_lot_number,
           date_provided DESC;

SET session_replication_role = 'origin';
SELECT meta.check_constraints('nsw_lrs', 'property_area');

