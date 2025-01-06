CREATE OR REPLACE FUNCTION pg_temp.sqm_area(
    area FLOAT,
    area_unit VARCHAR(1)
) RETURNS FLOAT AS $$
    SELECT CASE area_unit
        WHEN 'H' THEN area * 10000
        WHEN 'M' THEN area
        ELSE NULL
    END;
$$ LANGUAGE sql PARALLEL SAFE;

--
-- # Init Temp tables
--

WITH
  with_baseline_information AS (
    SELECT *,
           COALESCE(contract_date, settlement_date, date_provided) as effective_date
      FROM nsw_vg_raw.ps_row_b
      WHERE property_id IS NOT NULL
        AND sale_counter IS NOT NULL
        -- TODO document what's going on here.
        AND length(dealing_number) > 1),

  --
  -- Rank the contents of `pg_temp.sourced_raw_property_sales_b`
  -- to prioritise the most complete and up to date rows.
  --
  with_rank AS (
    SELECT ps_row_b_id,
           ROW_NUMBER() OVER (
             PARTITION BY dealing_number, property_id, strata_lot_number
             ORDER BY (
               (CASE WHEN contract_date IS NOT NULL THEN 1 ELSE 0 END) +
               (CASE WHEN settlement_date IS NOT NULL THEN 1 ELSE 0 END) +
               (CASE WHEN purchase_price IS NOT NULL THEN 1 ELSE 0 END) +
               (CASE WHEN area_type IS NOT NULL THEN 1 ELSE 0 END) +
               (CASE WHEN area IS NOT NULL THEN 1 ELSE 0 END)) DESC,
             date_provided DESC
           ) AS score
      FROM with_baseline_information)

SELECT c.source_id,
       d.file_source_id,
       b.*,
       (e.source_id IS NOT NULL) as seen_in_land_values
  INTO TEMP TABLE pg_temp.sourced_raw_property_sales_b
  FROM with_rank r
  LEFT JOIN with_baseline_information AS b USING (ps_row_b_id)
  LEFT JOIN nsw_vg_raw.ps_row_b_source AS c USING (ps_row_b_id)
  LEFT JOIN meta.source_file AS d USING (source_id)
  LEFT JOIN nsw_vg_raw.land_value_row_complement AS e USING (property_id, effective_date)
  WHERE r.score = 1;


CREATE INDEX idx_sourced_raw_property_sales_b_sale_counter
    ON pg_temp.sourced_raw_property_sales_b(file_source_id, sale_counter, property_id);

--
-- # Concatenciate long property descriptions
--

WITH
  sourced_raw_property_sales_c AS (
    SELECT source_id,
           file_source_id,
           r.*
      FROM nsw_vg_raw.ps_row_c as r
      LEFT JOIN nsw_vg_raw.ps_row_c_source USING (ps_row_c_id)
      LEFT JOIN meta.source_file USING (source_id)),

  aggregated AS (
    SELECT (ARRAY_AGG(source_id ORDER BY position))[1] as source_id,
           file_source_id,
           sale_counter,
           STRING_AGG(property_description, '' ORDER BY position) AS full_property_description
      FROM sourced_raw_property_sales_c
      WHERE property_description IS NOT NULL AND sale_counter IS NOT NULL
      GROUP BY (file_source_id, sale_counter)),

  joined AS (
    SELECT c.source_id,
           c.file_source_id,
           c.full_property_description,
           b.property_id,
           b.strata_lot_number,
           b.effective_date,
           b.seen_in_land_values,
           b.date_provided,
           b.dealing_number
      FROM pg_temp.sourced_raw_property_sales_b as b
      INNER JOIN aggregated as c USING (file_source_id, sale_counter)
      WHERE full_property_description IS NOT NULL
        AND property_id IS NOT NULL)

SELECT DISTINCT ON (property_id, strata_lot_number, dealing_number)
       *
  INTO TEMP TABLE pg_temp.consolidated_property_description_c
  FROM joined
  ORDER BY property_id,
           strata_lot_number,
           dealing_number DESC,
           date_provided DESC;

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
    SELECT DISTINCT ON (property_id, effective_date)
           property_id, effective_date, source_id as ps_row_b_id
      FROM pg_temp.sourced_raw_property_sales_b
      WHERE strata_lot_number IS NULL)

SELECT r.*,
       (m.ps_row_b_id IS NOT NULL) AS seen_in_modern_psi
  INTO TEMP pg_temp.sourced_raw_property_sales_b_legacy
  FROM unique_pairs r
  LEFT JOIN relevant_modern_psi m USING (property_id, effective_date);

CREATE INDEX idx_sourced_raw_property_sales_b_legacy_property_id
    ON pg_temp.sourced_raw_property_sales_b_legacy(property_id);

