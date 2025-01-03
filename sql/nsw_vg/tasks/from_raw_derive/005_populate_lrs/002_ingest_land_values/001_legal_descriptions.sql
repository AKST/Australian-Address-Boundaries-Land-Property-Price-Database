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

