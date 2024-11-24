CREATE OR REPLACE FUNCTION pg_temp.sqm_area(
    area FLOAT,
    area_unit VARCHAR(1)
) RETURNS FLOAT AS $$
    SELECT CASE area_unit
        WHEN 'H' THEN area * 10000
        WHEN 'M' THEN area
        ELSE NULL
    END;
$$ LANGUAGE sql;

--
-- # Ingest Land Values
-- ## Init Temp Tables
--

CREATE TEMP TABLE pg_temp.lv_effective_dates AS
  SELECT land_value_row_id, property_id, COALESCE(base_date_1, source_date) AS effective_date
    FROM nsw_vg_raw.land_value_row;

CREATE TEMP TABLE pg_temp.sourced_raw_land_values AS
SELECT source_id,
       r.*,
       effective_date
  FROM nsw_vg_raw.land_value_row as r
  LEFT JOIN pg_temp.lv_effective_dates USING (land_value_row_id)
  LEFT JOIN nsw_vg_raw.land_value_row_source USING (land_value_row_id);

CREATE INDEX idx_sourced_raw_land_values_source_date_property_id
    ON pg_temp.sourced_raw_land_values(source_date, property_id);

--
-- ## Ingest
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

DROP TABLE pg_temp.sourced_raw_land_values;

--
-- # Ingest PSI
--
-- ## Init Temp tables
--

WITH
  with_effective_dates AS (
    SELECT ps_row_b_id,
           property_id,
           strata_lot_number,
           COALESCE(contract_date, settlement_date, date_provided) AS effective_date,
           date_provided
      FROM nsw_vg_raw.ps_row_b as r
      WHERE sale_counter IS NOT NULL),

  distrinct_sales AS (
    SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
           effective_date, property_id, strata_lot_number, date_provided, ps_row_b_id
      FROM with_effective_dates
      ORDER BY effective_date, property_id, strata_lot_number, date_provided DESC),

  seen_in_land_values AS (
    SELECT r.*, (land_value_row_id IS NOT NULL) as seen_in_land_values
      FROM distrinct_sales r
      LEFT JOIN pg_temp.lv_effective_dates USING (property_id, effective_date))

SELECT source_id,
       file_source_id,
       r.effective_date,
       r.seen_in_land_values,
       b.*
  INTO TEMP TABLE pg_temp.sourced_raw_property_sales_b
  FROM seen_in_land_values r
  LEFT JOIN nsw_vg_raw.ps_row_b b USING (ps_row_b_id)
  LEFT JOIN nsw_vg_raw.ps_row_b_source USING (ps_row_b_id)
  LEFT JOIN meta.source_file USING (source_id);

CREATE INDEX idx_sourced_raw_property_sales_b_sale_counter
    ON pg_temp.sourced_raw_property_sales_b(file_source_id, sale_counter, property_id);

--
-- ## Concatenciate long property descriptions
--

WITH
  sourced_raw_property_sales_c AS (
    SELECT source_id, file_source_id, r.*
      FROM nsw_vg_raw.ps_row_c as r
      LEFT JOIN nsw_vg_raw.ps_row_c_source USING (ps_row_c_id)
      LEFT JOIN meta.source_file USING (source_id)),

  aggregated AS (
    SELECT MIN(source_id) as source_id,
           file_source_id,
           sale_counter,
           property_id,
           STRING_AGG(property_description, '' ORDER BY position) AS full_property_description
      FROM sourced_raw_property_sales_c
      WHERE property_description IS NOT NULL
        AND sale_counter IS NOT NULL
        AND property_id IS NOT NULL
      GROUP BY (file_source_id, sale_counter, property_id))

SELECT b.strata_lot_number, b.effective_date, b.seen_in_land_values, b.date_provided, c.*
  INTO TEMP TABLE pg_temp.consolidated_property_description_c
  FROM pg_temp.sourced_raw_property_sales_b as b
  LEFT JOIN aggregated as c USING (file_source_id, sale_counter, property_id)
  WHERE full_property_description IS NOT NULL;

--
-- ## Ingest PSI from sourced tables
--
-- ### Ingest Property Description

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
-- ### Ingest Property Area
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
--
-- ## Clean up
--

--
-- # Ingest Legacy Property Sales Information
--
-- ## Init Temp tables
--

WITH
  unique_pairs AS (
    SELECT DISTINCT ON (property_id, effective_date)
           source_id,
           file_source_id,
           date_published,
           r.*,
           r.contract_date as effective_date
      FROM nsw_vg_raw.ps_row_b_legacy as r
      LEFT JOIN nsw_vg_raw.ps_row_b_legacy_source USING (ps_row_b_legacy_id)
      LEFT JOIN meta.source_file USING (source_id)
      LEFT JOIN meta.file_source USING (file_source_id)
      --
      -- Combined with the `SELECT DISTINCT ON` and this ordering
      -- we limit results to unique pairs of (property_id, effective_date)
      -- while prioritising the most up to date ones.
      --
      ORDER BY property_id, effective_date, date_published DESC),

  relevant_modern_psi AS (
    SELECT property_id, effective_date, source_id as ps_row_b_id
    FROM pg_temp.sourced_raw_property_sales_b
    WHERE strata_lot_number IS NULL)

SELECT r.*,
       (m.ps_row_b_id IS NOT NULL) AS seen_in_modern_psi
  INTO TEMP pg_temp.sourced_raw_property_sales_b_legacy
  FROM unique_pairs r
  LEFT JOIN relevant_modern_psi m USING (property_id, effective_date);

CREATE INDEX idx_sourced_raw_property_sales_b_legacy_property_id
    ON pg_temp.sourced_raw_property_sales_b_legacy(property_id);

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

DROP TABLE IF EXISTS pg_temp.lv_effective_dates;
DROP TABLE IF EXISTS pg_temp.consolidated_property_description_c;
DROP TABLE IF EXISTS pg_temp.sourced_raw_property_sales_b;
DROP TABLE IF EXISTS pg_temp.sourced_raw_property_sales_b_legacy;
DROP FUNCTION pg_temp.sqm_area;


