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

CREATE TEMP TABLE pg_temp.sourced_raw_land_values AS
SELECT sfl.source_id, fs.file_source_id, r.*
  FROM nsw_vg_raw.land_value_row as r
  LEFT JOIN meta.file_source AS fs ON fs.file_path = r.source_file_name
  LEFT JOIN meta.source_file_line AS sfl
         ON sfl.source_file_line = r.source_line_number
        AND sfl.file_source_id = fs.file_source_id;

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
SELECT source_id, source_date, property_id, property_description, '> 2004-08-17'
  FROM pg_temp.sourced_raw_land_values WHERE property_description IS NOT NULL;

INSERT INTO nsw_lrs.property_area(source_id, effective_date, property_id, sqm_area)
SELECT source_id, source_date, property_id, pg_temp.sqm_area(area, area_type)
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
    zone_code,
    land_value)
SELECT lv_ids.source_id,
       lv_ids.source_date,
       lv_ids.district_code,
       lv_ids.property_id,
       lv_entries.base_date_1,
       lv_entries.basis_1,
       lv_entries.authority_1,
       CASE
         WHEN lv_ids.zone_standard = 'ep&a_2006' THEN lv_ids.zone_code
         ELSE NULL
       END,
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

DROP TABLE pg_temp.sourced_raw_land_values;

--
-- # Ingest PSI
--
-- ## Init Temp tables
--

CREATE TEMP VIEW pg_temp.sourced_raw_property_sales_a AS
SELECT sbp.source_id, fs.file_source_id, fs.date_published, r.*
  FROM nsw_vg_raw.ps_row_a as r
  LEFT JOIN meta.file_source AS fs USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.file_source_id = fs.file_source_id;

CREATE TEMP TABLE pg_temp.sourced_raw_property_sales_b AS
SELECT sbp.source_id, fs.file_source_id, fs.date_published, r.*
  FROM nsw_vg_raw.ps_row_b as r
  LEFT JOIN meta.file_source AS fs USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.file_source_id = fs.file_source_id;

CREATE INDEX idx_sourced_raw_property_sales_b_sale_counter
    ON pg_temp.sourced_raw_property_sales_b(sale_counter);

--
-- ## Concatenciate long property descriptions
--

WITH sourced_raw_property_sales_c AS (
    SELECT sbp.source_id, fs.file_source_id, fs.date_published, r.*
      FROM nsw_vg_raw.ps_row_c as r
      LEFT JOIN meta.file_source AS fs USING (file_path)
      LEFT JOIN meta.source_byte_position AS sbp
             ON sbp.source_byte_position = r.position
            AND sbp.file_source_id = fs.file_source_id)
SELECT
    MIN(c.source_id) as source_id,
    MIN(c.date_provided) as date_provided,
    file_source_id,
    sale_counter,
    property_id,
    STRING_AGG(c.property_description, '' ORDER BY c.position) AS full_property_description
  INTO TEMP TABLE pg_temp.consolidated_property_description_c
  FROM sourced_raw_property_sales_c as c
  WHERE property_description IS NOT NULL
    AND sale_counter IS NOT NULL
    AND property_id IS NOT NULL
  GROUP BY (file_source_id, sale_counter, property_id);

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
SELECT DISTINCT ON (c.date_provided, property_id, full_property_description)
    c.source_id,
    c.date_provided,
    property_id,
    full_property_description,
    (case
      when c.date_provided > '2004-08-17' then '> 2004-08-17'
      else 'initial'
    end)::nsw_lrs.legal_description_kind
  FROM pg_temp.consolidated_property_description_c as c
  LEFT JOIN pg_temp.sourced_raw_property_sales_b as b USING (file_source_id, sale_counter, property_id)
  WHERE full_property_description IS NOT NULL
    AND b.strata_lot_number IS NULL
  ORDER BY c.date_provided, property_id, full_property_description, c.source_id;

INSERT INTO nsw_lrs.legal_description_by_strata_lot(
  source_id,
  effective_date,
  property_id,
  property_strata_lot,
  legal_description,
  legal_description_kind)
SELECT DISTINCT ON (c.date_provided, property_id, strata_lot_number, full_property_description)
    c.source_id,
    c.date_provided,
    property_id,
    strata_lot_number,
    full_property_description,
    (case
      when c.date_provided > '2004-08-17' then '> 2004-08-17'
      else 'initial'
    end)::nsw_lrs.legal_description_kind
  FROM pg_temp.consolidated_property_description_c as c
  LEFT JOIN pg_temp.sourced_raw_property_sales_b as b USING (file_source_id, sale_counter, property_id)
  WHERE full_property_description IS NOT NULL
    AND strata_lot_number IS NOT NULL
  ORDER BY c.date_provided, property_id, strata_lot_number, full_property_description, c.source_id;

DROP TABLE pg_temp.consolidated_property_description_c;

--
-- ### Ingest Property Area
--

INSERT INTO nsw_lrs.property_area(
    source_id,
    effective_date,
    property_id,
    sqm_area)
SELECT DISTINCT ON (date_published, property_id)
    source_id,
    date_published,
    property_id,
    pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_property_sales_b
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
    AND strata_lot_number IS NULL
    AND property_id IS NOT NULL;
    -- AND NOT also_in_lv;

INSERT INTO nsw_lrs.property_area_by_strata_lot(
    source_id,
    effective_date,
    property_id,
    property_strata_lot,
    sqm_area)
SELECT DISTINCT ON (date_published, property_id, strata_lot_number)
    source_id,
    date_published,
    property_id,
    strata_lot_number,
    pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_property_sales_b
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
       AND strata_lot_number IS NOT NULL
       AND property_id IS NOT NULL;

DROP TABLE pg_temp.sourced_raw_property_sales_b;
DROP VIEW pg_temp.sourced_raw_property_sales_a;

--
-- # Ingest Legacy Property Sales Information
--
-- ## Init Temp tables
--

CREATE TEMP TABLE pg_temp.sourced_raw_property_sales_a_legacy AS
SELECT sbp.source_id, fs.file_source_id, fs.date_published, r.*
  FROM nsw_vg_raw.ps_row_a_legacy as r
  LEFT JOIN meta.file_source AS fs USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.file_source_id = fs.file_source_id;

CREATE TEMP TABLE pg_temp.sourced_raw_property_sales_b_legacy AS
SELECT sbp.source_id, fs.file_source_id, fs.date_published, r.*
  FROM nsw_vg_raw.ps_row_b_legacy as r
  LEFT JOIN meta.file_source AS fs USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.file_source_id = fs.file_source_id;

CREATE INDEX idx_sourced_raw_property_sales_b_legacy_property_id
    ON pg_temp.sourced_raw_property_sales_b_legacy(property_id);

CREATE INDEX idx_sourced_raw_property_sales_a_legacy_file_source_id
    ON pg_temp.sourced_raw_property_sales_a_legacy(file_source_id);

--
-- ## Ingest Legacy PSI from sourced tables
--

INSERT INTO nsw_lrs.archived_legal_description(source_id, effective_date, property_id, legal_description)
SELECT
    source_id,
    date_published,
    property_id,
    b.land_description
  FROM pg_temp.sourced_raw_property_sales_b_legacy as b
  WHERE b.land_description IS NOT NULL;

INSERT INTO nsw_lrs.property_area(source_id, effective_date, property_id, sqm_area)
SELECT source_id, date_published, property_id, pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_property_sales_b_legacy
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
    AND property_id IS NOT NULL;

INSERT INTO nsw_lrs.described_dimensions(source_id, effective_date, property_id, dimension_description)
SELECT source_id, date_published, property_id, dimensions
  FROM pg_temp.sourced_raw_property_sales_b_legacy
  WHERE property_id IS NOT NULL
    AND dimensions IS NOT NULL;

DROP TABLE pg_temp.sourced_raw_property_sales_b_legacy;
DROP TABLE pg_temp.sourced_raw_property_sales_a_legacy;


