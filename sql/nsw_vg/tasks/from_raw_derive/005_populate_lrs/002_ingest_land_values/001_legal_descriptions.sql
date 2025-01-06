--
-- # Ingest Land Values
--

INSERT INTO nsw_lrs.legal_description(
  source_id,
  effective_date,
  legal_description_id,
  property_id,
  legal_description,
  legal_description_kind)
SELECT source_id,
       effective_date,
       uuid_generate_v4(),
       property_id,
       property_description,
       '> 2004-08-17'
  FROM nsw_vg_raw.land_value_row
  LEFT JOIN nsw_vg_raw.land_value_row_complement USING (property_id, source_date)
  WHERE property_description IS NOT NULL;

