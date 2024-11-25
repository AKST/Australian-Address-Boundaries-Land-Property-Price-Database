--
-- # Ingest PSI
--
-- ## Ingest Property Description
--

INSERT INTO nsw_lrs.legal_description(
  source_id,
  effective_date,
  property_id,
  legal_description,
  legal_description_kind)
SELECT DISTINCT ON (effective_date, property_id, full_property_description)
    source_id,
    effective_date,
    property_id,
    full_property_description,
    (case
      when date_provided > '2004-08-17' then '> 2004-08-17'
      else 'initial'
    end)::nsw_lrs.legal_description_kind
  FROM pg_temp.consolidated_property_description_c as c
  WHERE full_property_description IS NOT NULL
    AND strata_lot_number IS NULL
    AND NOT seen_in_land_values
  ORDER BY effective_date, property_id, full_property_description, source_id;

INSERT INTO nsw_lrs.legal_description_by_strata_lot(
  source_id,
  effective_date,
  property_id,
  property_strata_lot,
  legal_description,
  legal_description_kind)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number, full_property_description)
    source_id,
    effective_date,
    property_id,
    strata_lot_number,
    full_property_description,
    (case
      when date_provided > '2004-08-17' then '> 2004-08-17'
      else 'initial'
    end)::nsw_lrs.legal_description_kind
  FROM pg_temp.consolidated_property_description_c as c
  WHERE full_property_description IS NOT NULL
    AND strata_lot_number IS NOT NULL
  ORDER BY effective_date, property_id, strata_lot_number, full_property_description, c.source_id;

--
-- ## Ingest Property Area
--

INSERT INTO nsw_lrs.property_area(
    source_id,
    effective_date,
    property_id,
    sqm_area)
SELECT DISTINCT ON (effective_date, property_id)
    source_id,
    effective_date,
    property_id,
    pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_property_sales_b
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
    AND NOT seen_in_land_values
    AND strata_lot_number IS NULL
    AND property_id IS NOT NULL;

INSERT INTO nsw_lrs.property_area_by_strata_lot(
    source_id,
    effective_date,
    property_id,
    property_strata_lot,
    sqm_area)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    source_id,
    effective_date,
    property_id,
    strata_lot_number,
    pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_property_sales_b
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
       AND strata_lot_number IS NOT NULL
       AND property_id IS NOT NULL;

--
-- ## Ingest Primary Purpose
--

INSERT INTO nsw_lrs.property_primary_purpose(
    source_id,
    effective_date,
    primary_purpose_id,
    property_id,
    strata_lot_no)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    source_id,
    effective_date,
    primary_purpose_id,
    property_id,
    strata_lot_number
  FROM pg_temp.sourced_raw_property_sales_b b
  LEFT JOIN nsw_lrs.primary_purpose USING (primary_purpose)
  WHERE b.primary_purpose IS NOT NULL
    AND property_id IS NOT NULL;

--
-- ## Zones
--

INSERT INTO nsw_lrs.zone_observation(
    source_id,
    effective_date,
    property_id,
    zone_code)
SELECT DISTINCT ON (effective_date, property_id, zone_code)
       source_id,
       effective_date,
       property_id,
       zone_code
  FROM pg_temp.sourced_raw_property_sales_b
  WHERE zone_standard = 'ep&a_2006'
    AND NOT seen_in_land_values
    AND strata_lot_number IS NULL;

