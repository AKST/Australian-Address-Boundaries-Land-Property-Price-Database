--
-- # Populate NSW LRS
--
-- ## Temporary Views and Tables
--
-- I'm going to have to do these links a ton may as well
-- create them all at once here and clean them up below.

CREATE TEMP TABLE pg_temp.sourced_raw_land_values AS
SELECT sfl.source_id, sf.source_file_id, r.*
  FROM nsw_vg_raw.land_value_row as r
  LEFT JOIN meta.source_file AS sf ON sf.file_path = r.source_file_name
  LEFT JOIN meta.source_file_line AS sfl
         ON sfl.source_file_line = r.source_line_number
        AND sfl.source_file_id = sf.source_file_id;

CREATE INDEX idx_sourced_raw_land_values_source_date_property_id
    ON pg_temp.sourced_raw_land_values(source_date, property_id);

CREATE TEMP TABLE pg_temp.sourced_raw_property_sales_a_legacy AS
SELECT sbp.source_id, sf.source_file_id, sf.date_published, r.*
  FROM nsw_vg_raw.ps_row_a_legacy as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE INDEX idx_sourced_raw_property_sales_a_legacy_source_file_id
    ON pg_temp.sourced_raw_property_sales_a_legacy(source_file_id);

CREATE TEMP TABLE pg_temp.sourced_raw_property_sales_b_legacy AS
SELECT sbp.source_id, sf.source_file_id, sf.date_published, r.*
  FROM nsw_vg_raw.ps_row_b_legacy as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE INDEX idx_sourced_raw_property_sales_b_legacy_property_id
    ON pg_temp.sourced_raw_property_sales_b_legacy(property_id);

CREATE TEMP VIEW pg_temp.sourced_raw_property_sales_a AS
SELECT sbp.source_id, sf.source_file_id, sf.date_published, r.*
  FROM nsw_vg_raw.ps_row_a as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE TEMP TABLE pg_temp.sourced_raw_property_sales_b AS
SELECT sbp.source_id, sf.source_file_id, sf.date_published, r.*
  FROM nsw_vg_raw.ps_row_b as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE INDEX idx_sourced_raw_property_sales_b_sale_counter
    ON pg_temp.sourced_raw_property_sales_b(sale_counter);

WITH sourced_raw_property_sales_c AS (
    SELECT sbp.source_id, sf.source_file_id, sf.date_published, r.*
      FROM nsw_vg_raw.ps_row_c as r
      LEFT JOIN meta.source_file AS sf USING (file_path)
      LEFT JOIN meta.source_byte_position AS sbp
             ON sbp.source_byte_position = r.position
            AND sbp.source_file_id = sf.source_file_id)
SELECT
    source_file_id,
    sale_counter,
    property_id,
    STRING_AGG(c.property_description, '' ORDER BY c.position) AS full_property_description
  INTO TEMP TABLE pg_temp.consolidated_property_description_c
  FROM sourced_raw_property_sales_c as c
  WHERE property_description IS NOT NULL
    AND sale_counter IS NOT NULL
    AND property_id IS NOT NULL
  GROUP BY (source_file_id, sale_counter, property_id);

CREATE TEMP VIEW pg_temp.sourced_raw_property_sales_d AS
SELECT sbp.source_id, sf.source_file_id, sf.date_published, r.*
  FROM nsw_vg_raw.ps_row_d as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

--
-- ### Create Temp tables
--
-- TODO: check if the property description is recorded on contract.
-- TODO: document data loss from (sale_counter IS NOT NULL)
-- TODO: revisit `COALESCE(contract_date, settlement_date, date_provided)`
--

WITH with_effective_date AS (
    SELECT source_file_id, source_id, property_id, sale_counter, strata_lot_number,
           COALESCE(contract_date, settlement_date, date_provided) AS effective_date,
           date_provided
      FROM pg_temp.sourced_raw_property_sales_b
      WHERE sale_counter IS NOT NULL)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    b_rows.*,
    (lv.property_id IS NOT NULL) as also_in_lv
  INTO TEMP TABLE pg_temp.sourced_raw_property_sales_b_dates
  FROM with_effective_date as b_rows
  LEFT JOIN pg_temp.sourced_raw_land_values as lv
      ON lv.source_date = b_rows.effective_date
     AND lv.property_id = b_rows.property_id
  ORDER BY effective_date,
           property_id,
           strata_lot_number,
           date_provided DESC;

WITH with_effective_date AS (
    SELECT source_file_id, b.source_id, property_id,
           COALESCE(contract_date, date_provided) AS effective_date,
           a.date_provided
      FROM pg_temp.sourced_raw_property_sales_b_legacy as b
      LEFT JOIN pg_temp.sourced_raw_property_sales_a_legacy as a USING (source_file_id)
      WHERE property_id IS NOT NULL)
SELECT DISTINCT ON (effective_date, property_id)
    b_rows.*,
    (modern.property_id IS NOT NULL) as also_in_modern
  INTO TEMP TABLE pg_temp.sourced_raw_property_sales_b_dates_legacy
  FROM with_effective_date as b_rows
  LEFT JOIN pg_temp.sourced_raw_property_sales_b_dates as modern
      ON b_rows.effective_date = modern.effective_date
      AND b_rows.property_id = modern.property_id
  ORDER BY effective_date,
           property_id,
           date_provided DESC;

--
-- ## Populate `nsw_lrs.property`
--
-- This step doesn't really require the meta data
-- but both will be  needed for subsequent steps.
--

INSERT INTO nsw_lrs.property(property_id)
SELECT *
  FROM (
      SELECT property_id FROM nsw_vg_raw.land_value_row
       UNION
      SELECT property_id FROM nsw_vg_raw.ps_row_b
       UNION
      SELECT property_id FROM nsw_vg_raw.ps_row_b_legacy
  ) as t
  WHERE property_id IS NOT NULL
    ON CONFLICT (property_id) DO NOTHING;

--
-- ## Ingest Property Description
--
-- ### Ingest from Land Values
--
-- There should only be one entry per property_id.
--

INSERT INTO nsw_lrs.legal_description(source_id, effective_date, property_id, legal_description)
SELECT source_id, source_date, property_id, property_description
  FROM pg_temp.sourced_raw_land_values WHERE property_description IS NOT NULL;

--
-- ### Ingest from Property Sales
--
-- The earlier temp tables were created to deal
-- with some edge cases. We also handle property
-- descriptions different for properties with
-- strata plans.
--

INSERT INTO nsw_lrs.legal_description(
  source_id,
  effective_date,
  property_id,
  legal_description)
SELECT
    b.source_id,
    b.effective_date,
    property_id,
    c.full_property_description
  FROM pg_temp.sourced_raw_property_sales_b_dates as b
  LEFT JOIN pg_temp.consolidated_property_description_c as c USING (source_file_id, sale_counter, property_id)
  WHERE full_property_description IS NOT NULL
    AND strata_lot_number IS NULL
    AND NOT b.also_in_lv;

INSERT INTO nsw_lrs.legal_description(source_id, effective_date, property_id, legal_description)
SELECT
    source_id,
    dates.effective_date,
    property_id,
    b.land_description
  FROM pg_temp.sourced_raw_property_sales_b_dates_legacy as dates
  LEFT JOIN pg_temp.sourced_raw_property_sales_b_legacy as b USING (source_id, property_id)
  WHERE b.land_description IS NOT NULL
    AND NOT dates.also_in_modern;

INSERT INTO nsw_lrs.legal_description_by_strata_lot(source_id, effective_date, property_id, property_strata_lot, legal_description)
SELECT
    b.source_id,
    b.effective_date,
    property_id,
    b.strata_lot_number,
    c.full_property_description
  FROM pg_temp.sourced_raw_property_sales_b_dates as b
  LEFT JOIN pg_temp.consolidated_property_description_c as c USING (source_file_id, sale_counter, property_id)
  WHERE full_property_description IS NOT NULL
    AND strata_lot_number IS NOT NULL;

--
-- ## Ingest Property Area
--

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

INSERT INTO nsw_lrs.property_area(source_id, effective_date, property_id, sqm_area)
SELECT source_id, source_date, property_id, pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_land_values
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL;

INSERT INTO nsw_lrs.property_area(
    source_id,
    effective_date,
    property_id,
    sqm_area)
SELECT
    source_id,
    effective_date,
    property_id,
    pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_property_sales_b_dates
  LEFT JOIN pg_temp.sourced_raw_property_sales_b
      USING (source_id, sale_counter, property_id, strata_lot_number)
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
    AND strata_lot_number IS NULL
    AND property_id IS NOT NULL
    AND NOT also_in_lv;

INSERT INTO nsw_lrs.property_area_by_strata_lot(
    source_id,
    effective_date,
    property_id,
    property_strata_lot,
    sqm_area)
SELECT
    source_id,
    effective_date,
    property_id,
    strata_lot_number,
    pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_property_sales_b_dates
  LEFT JOIN pg_temp.sourced_raw_property_sales_b
      USING (source_id, sale_counter, property_id, strata_lot_number)
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
    AND strata_lot_number IS NOT NULL
    AND property_id IS NOT NULL;

INSERT INTO nsw_lrs.property_area(source_id, effective_date, property_id, sqm_area)
SELECT source_id, effective_date, property_id, pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_property_sales_b_dates_legacy
  LEFT JOIN pg_temp.sourced_raw_property_sales_b_legacy USING (source_id, property_id)
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
    AND property_id IS NOT NULL
    AND NOT also_in_modern;

--
-- ### Ingest Legacy Property Dimensions
--

INSERT INTO nsw_lrs.described_dimensions(source_id, effective_date, property_id, dimension_description)
SELECT source_id, effective_date, property_id, dimensions
  FROM pg_temp.sourced_raw_property_sales_b_dates_legacy
  LEFT JOIN pg_temp.sourced_raw_property_sales_b_legacy USING (source_id, property_id)
  WHERE property_id IS NOT NULL
    AND dimensions IS NOT NULL;

DROP FUNCTION pg_temp.sqm_area;
DROP TABLE pg_temp.sourced_raw_property_sales_b_dates_legacy;
DROP TABLE pg_temp.sourced_raw_property_sales_b_dates;
DROP TABLE pg_temp.consolidated_property_description_c;
DROP VIEW pg_temp.sourced_raw_property_sales_d;
DROP TABLE pg_temp.sourced_raw_property_sales_b;
DROP VIEW pg_temp.sourced_raw_property_sales_a;
DROP TABLE pg_temp.sourced_raw_property_sales_b_legacy;
DROP TABLE pg_temp.sourced_raw_property_sales_a_legacy;
DROP TABLE pg_temp.sourced_raw_land_values;
