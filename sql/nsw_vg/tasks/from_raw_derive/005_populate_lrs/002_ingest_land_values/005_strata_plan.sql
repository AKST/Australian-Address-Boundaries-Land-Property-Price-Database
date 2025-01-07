SET session_replication_role = 'replica';

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
  FROM nsw_vg_raw.land_value_row
  LEFT JOIN nsw_vg_raw.land_value_row_complement USING (property_id, source_date)
  WHERE property_type IS NOT NULL;

SET session_replication_role = 'origin';

SELECT meta.check_constraints('nsw_lrs', 'property_under_strata_plan');
