INSERT INTO nsw_lrs.property_under_strata_plan(
    source_id,
    effective_date,
    property_id,
    under_strata_plan)
SELECT DISTINCT ON (effective_date, property_id)
    source_id,
    effective_date,
    property_id,
    (property_type = 'UNDERSP') as under_strata_plan
  FROM pg_temp.sourced_raw_land_values
  WHERE property_type IS NOT NULL;

