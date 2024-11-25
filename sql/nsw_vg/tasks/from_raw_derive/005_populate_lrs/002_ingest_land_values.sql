--
-- # Ingest Land Values
--

INSERT INTO nsw_lrs.legal_description(
  source_id,
  effective_date,
  property_id,
  legal_description,
  legal_description_kind)
SELECT source_id, effective_date, property_id, property_description, '> 2004-08-17'
  FROM pg_temp.sourced_raw_land_values
  WHERE property_description IS NOT NULL;

INSERT INTO nsw_lrs.property_area(source_id, effective_date, property_id, sqm_area)
SELECT source_id, effective_date, property_id, pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_land_values
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL;

INSERT INTO nsw_vg.land_valuation(
    source_id,
    effective_date,
    valuation_district_code,
    property_id,
    valuation_base_date,
    valuation_basis,
    valuation_authority,
    land_value)
SELECT lv_ids.source_id,
       lv_ids.source_date,
       lv_ids.district_code,
       lv_ids.property_id,
       lv_entries.base_date_1,
       lv_entries.basis_1,
       lv_entries.authority_1,
       lv_entries.land_value_1
  FROM (
      SELECT source_id, source_date, property_id, zone_code, zone_standard, district_code
        FROM pg_temp.sourced_raw_land_values
  ) as lv_ids
  LEFT JOIN (
    SELECT property_id, source_id, base_date_1, basis_1, authority_1, land_value_1
      FROM pg_temp.sourced_raw_land_values
      WHERE land_value_1 IS NOT NULL
    UNION ALL
    SELECT property_id, source_id, base_date_2, basis_2, authority_2, land_value_2
      FROM pg_temp.sourced_raw_land_values
      WHERE land_value_2 IS NOT NULL
    UNION ALL
    SELECT property_id, source_id, base_date_3, basis_3, authority_3, land_value_3
      FROM pg_temp.sourced_raw_land_values
      WHERE land_value_3 IS NOT NULL
    UNION ALL
    SELECT property_id, source_id, base_date_4, basis_4, authority_4, land_value_4
      FROM pg_temp.sourced_raw_land_values
      WHERE land_value_4 IS NOT NULL
    UNION ALL
    SELECT property_id, source_id, base_date_5, basis_5, authority_5, land_value_5
      FROM pg_temp.sourced_raw_land_values
      WHERE land_value_5 IS NOT NULL
  ) AS lv_entries USING (property_id, source_id);

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

