TRUNCATE nsw_lrs.described_dimensions CASCADE;
TRUNCATE nsw_lrs.property_area CASCADE;
TRUNCATE nsw_lrs.legal_description_by_strata CASCADE;
TRUNCATE nsw_lrs.legal_description CASCADE;
TRUNCATE nsw_lrs.property CASCADE;

--
-- # Populate NSW LRS
--
-- ## Populate `nsw_lrs.property`
--
-- This step doesn't really require the meta data
-- but both will be  needed for subsequent steps.
--

INSERT INTO nsw_lrs.property(property_id)
SELECT * FROM
   (SELECT property_id FROM nsw_vg_raw.land_value_row
     UNION
    SELECT property_id FROM nsw_vg_raw.ps_row_b
     UNION
    SELECT property_id FROM nsw_vg_raw.ps_row_b_legacy) as t
  WHERE property_id IS NOT NULL
    ON CONFLICT (property_id) DO NOTHING;

--
-- # Temporary Views and Tables
--
-- I'm going to have to do these links a ton may as well
-- create them all at once here and clean them up below.

CREATE TEMP VIEW sourced_raw_land_values AS
SELECT sfl.source_id, sf.source_file_id, r.*
  FROM nsw_vg_raw.land_value_row as r
  LEFT JOIN meta.source_file AS sf ON sf.file_path = r.source_file_name
  LEFT JOIN meta.source_file_line AS sfl
         ON sfl.source_file_line = r.source_line_number
        AND sfl.source_file_id = sf.source_file_id;

CREATE TEMP VIEW sourced_raw_property_sales_a_legacy AS
SELECT sbp.source_id, sf.source_file_id, r.*
  FROM nsw_vg_raw.ps_row_a_legacy as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE TEMP TABLE sourced_raw_property_sales_b_legacy AS
SELECT sbp.source_id, sf.source_file_id, r.*
  FROM nsw_vg_raw.ps_row_b_legacy as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE TEMP VIEW sourced_raw_property_sales_a AS
SELECT sbp.source_id, sf.source_file_id, r.*
  FROM nsw_vg_raw.ps_row_a as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE TEMP TABLE sourced_raw_property_sales_b AS
SELECT sbp.source_id, sf.source_file_id, r.*
  FROM nsw_vg_raw.ps_row_b as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE TEMP TABLE sourced_raw_property_sales_c AS
SELECT sbp.source_id, sf.source_file_id, r.*
  FROM nsw_vg_raw.ps_row_c as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE TEMP VIEW sourced_raw_property_sales_d AS
SELECT sbp.source_id, sf.source_file_id, r.*
  FROM nsw_vg_raw.ps_row_d as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;


--
-- ## Ingest Property Description

--
-- ### Create Temp tables
--
-- TODO: check if the property description is recorded on contract.
-- TODO: document data loss from (sale_counter IS NOT NULL)
-- TODO: revisit `COALESCE(contract_date, settlement_date, date_provided)`

CREATE TEMP TABLE consolidated_property_description_c AS
SELECT source_file_id,
       sale_counter,
       property_id,
       STRING_AGG(c.property_description, '' ORDER BY c.position) AS full_property_description
  FROM sourced_raw_property_sales_c as c
  WHERE property_description IS NOT NULL
    AND sale_counter IS NOT NULL
    AND property_id IS NOT NULL
  GROUP BY (source_file_id, sale_counter, property_id);


CREATE TEMP TABLE sourced_raw_property_sales_b_dates AS
    SELECT DISTINCT ON (effective_date, property_id, strata_lot_number) bb.*
      FROM (SELECT source_file_id, source_id, property_id, sale_counter, strata_lot_number,
	                 COALESCE(contract_date, settlement_date, date_provided) AS effective_date,
	                 date_provided
	            FROM sourced_raw_property_sales_b
	            WHERE sale_counter IS NOT NULL) as bb
	  ORDER BY effective_date,
             property_id,
             strata_lot_number,
             date_provided DESC;

CREATE TEMP TABLE sourced_raw_property_sales_b_dates_legacy AS
    SELECT DISTINCT ON (effective_date, property_id) bb.*
      FROM (SELECT source_file_id, b.source_id, property_id,
	                 COALESCE(contract_date, date_provided) AS effective_date,
	                 date_provided,
                   land_description
	            FROM sourced_raw_property_sales_b_legacy as b
	            LEFT JOIN sourced_raw_property_sales_a_legacy as a USING (source_file_id)
	            WHERE property_id IS NOT NULL) as bb
	  ORDER BY effective_date,
             property_id,
             date_provided DESC;

--
-- ### Ingest from Land Values
--
-- There should only be one entry per property_id.
--

INSERT INTO nsw_lrs.legal_description(source_id, effective_date, property_id, legal_description)
SELECT source_id, source_date, property_id, property_description
  FROM sourced_raw_land_values WHERE property_description IS NOT NULL;

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
SELECT b.source_id, b.effective_date, property_id, c.full_property_description
  FROM sourced_raw_property_sales_b_dates as b
  LEFT JOIN consolidated_property_description_c as c USING (source_file_id, sale_counter, property_id)
  WHERE full_property_description IS NOT NULL
    AND strata_lot_number IS NULL
    --
    -- It's very possible that there is information already
    -- provied by the land value data we can skip that.
    --
    AND NOT EXISTS (SELECT 1 FROM sourced_raw_land_values as lv
        WHERE lv.source_date = b.effective_date
          AND lv.property_id = b.property_id);

INSERT INTO nsw_lrs.legal_description(source_id, effective_date, property_id, legal_description)
SELECT source_id, effective_date, property_id, land_description
  FROM sourced_raw_property_sales_b_dates_legacy as legacy
  WHERE land_description IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM sourced_raw_property_sales_b_dates as modern
        WHERE modern.effective_date = legacy.effective_date
          AND modern.property_id = legacy.property_id);

INSERT INTO nsw_lrs.legal_description_by_strata(
  source_id,
  effective_date,
  property_id,
  property_strata_lot,
  legal_description)
SELECT b.source_id, b.effective_date, property_id, b.strata_lot_number, c.full_property_description
  FROM sourced_raw_property_sales_b_dates as b
  LEFT JOIN consolidated_property_description_c as c USING (source_file_id, sale_counter, property_id)
  WHERE full_property_description IS NOT NULL
    AND strata_lot_number IS NOT NULL;

--
-- ## Ingest Legacy Property Description
--

--
-- ## Ingest Property Area & Dimensions
--

INSERT INTO nsw_lrs.property_area(source_id, effective_date, property_id, sqm_area)
SELECT * FROM
   (SELECT source_id,
           base_date_1 as recording_date,
           property_id,
           (CASE
              WHEN a_query.area_type = 'H' THEN a_query.area * 10000
              WHEN a_query.area_type = 'M' THEN a_query.area
              WHEN a_query.area_type = 'U' THEN NULL
              WHEN a_query.area_type IS NULL THEN NULL
           END) as area
      FROM
       (SELECT source_id, base_date_1, property_id, area, area_type
          FROM sourced_raw_land_values UNION
        SELECT source_id, contract_date, property_id, area, area_type
          FROM sourced_raw_property_sales_b_legacy UNION
        SELECT source_id, contract_date, property_id, area, area_type
          FROM sourced_raw_property_sales_b) as a_query) as b_query
  WHERE recording_date IS NOT NULL
    AND property_id IS NOT NULL
    AND area IS NOT NULL;

INSERT INTO nsw_lrs.described_dimensions(source_id, effective_date, property_id, dimension_description)
SELECT source_id, contract_date, property_id, dimensions
  FROM sourced_raw_property_sales_b_legacy
  WHERE recording_date IS NOT NULL
    AND property_id IS NOT NULL
    AND dimensions IS NOT NULL;


--
-- ## Ingest names and addresses
--



--
-- # Clean up
--
-- We created a few temporary tables, they'll be cleaned up
-- at the end of the session, but whose to say the session
-- will end after this script ends.
--

DROP TABLE sourced_raw_property_sales_b_dates_legacy;
DROP TABLE sourced_raw_property_sales_b_dates;
DROP TABLE consolidated_property_description_c;
DROP VIEW sourced_raw_property_sales_d;
DROP TABLE sourced_raw_property_sales_c;
DROP TABLE sourced_raw_property_sales_b;
DROP VIEW sourced_raw_property_sales_a;
DROP TABLE sourced_raw_property_sales_b_legacy;
DROP VIEW sourced_raw_property_sales_a_legacy;
DROP VIEW sourced_raw_land_values;
