INSERT INTO nsw_vg.land_valuation(
    source_id,
    effective_date,
    valuation_district_code,
    property_id,
    valuation_base_date,
    valuation_basis,
    valuation_authority,
    land_value)
SELECT DISTINCT ON (property_id, base_date)
       source_id,
       lv_entries.base_date,
       district_code,
       property_id,
       lv_entries.base_date,
       lv_entries.basis,
       lv_entries.authority,
       lv_entries.land_value
  FROM nsw_vg_raw.land_value_row
  LEFT JOIN nsw_vg_raw.land_value_row_complement USING (property_id, source_date)
  LEFT JOIN LATERAL (
    SELECT
      property_id,
      source_id,
      UNNEST(ARRAY[base_date_1, base_date_2, base_date_3, base_date_4, base_date_5]) as base_date,
      UNNEST(ARRAY[basis_1, basis_2, basis_3, basis_4, basis_5]) as basis,
      UNNEST(ARRAY[authority_1, authority_2, authority_3, authority_4, authority_5]) as authority,
      UNNEST(ARRAY[land_value_1, land_value_2, land_value_3, land_value_4, land_value_5]) as land_value)
    AS lv_entries USING (property_id, source_id)
  WHERE land_value IS NOT NULL
  ORDER BY property_id, base_date, source_date DESC;

