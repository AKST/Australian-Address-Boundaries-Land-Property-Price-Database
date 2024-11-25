--
-- ## Ingest Legacy PSI from sourced tables
--

INSERT INTO nsw_lrs.archived_legal_description(source_id, effective_date, property_id, legal_description)
SELECT source_id, effective_date, property_id, b.land_description
  FROM pg_temp.sourced_raw_property_sales_b_legacy as b
  WHERE b.land_description IS NOT NULL
    AND property_id IS NOT NULL;

INSERT INTO nsw_lrs.property_area(source_id, effective_date, property_id, sqm_area)
SELECT source_id, effective_date, property_id, pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_property_sales_b_legacy
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
    AND property_id IS NOT NULL
    AND NOT seen_in_modern_psi;

INSERT INTO nsw_lrs.described_dimensions(source_id, effective_date, property_id, dimension_description)
SELECT source_id, effective_date, property_id, dimensions
  FROM pg_temp.sourced_raw_property_sales_b_legacy
  WHERE property_id IS NOT NULL
    AND dimensions IS NOT NULL;

