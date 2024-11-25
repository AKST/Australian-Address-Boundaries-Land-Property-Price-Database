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
-- # Init Temp Tables
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
-- # Init Temp tables
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
-- # Concatenciate long property descriptions
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
-- # Init Temp tables
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

