TRUNCATE nsw_lrs.legal_description CASCADE;
TRUNCATE nsw_lrs.property CASCADE;
TRUNCATE meta.event CASCADE;
TRUNCATE nsw_gnb.address CASCADE;
TRUNCATE nsw_gnb.street CASCADE;
TRUNCATE nsw_gnb.locality CASCADE;

--
-- # Establish Street & Suburb Names
--

INSERT INTO nsw_gnb.locality (locality_name)
SELECT DISTINCT upper(t.locality_name) FROM
   (SELECT locality_name FROM nsw_vg_raw.ps_row_b_legacy UNION
    SELECT locality_name FROM nsw_vg_raw.ps_row_b UNION
    SELECT suburb_name FROM nsw_vg_raw.land_value_row) as t
 WHERE t.locality_name != NULL
    ON CONFLICT (locality_name) DO NOTHING;

INSERT INTO nsw_gnb.street (street_name, locality_id)
SELECT DISTINCT upper(t.street_name), l.locality_id FROM
   (SELECT street_name, locality_name FROM nsw_vg_raw.ps_row_b_legacy UNION
    SELECT street_name, locality_name FROM nsw_vg_raw.ps_row_b UNION
    SELECT street_name, suburb_name FROM nsw_vg_raw.land_value_row) as t
  LEFT JOIN nsw_gnb.locality l
         ON upper(l.locality_name) = upper(t.locality_name)
 WHERE t.street_name IS NOT NULL
    ON CONFLICT (street_name, locality_id) DO NOTHING;

--
-- # Addresses
--

INSERT INTO nsw_gnb.address (property_name, unit_number, street_number, street_id)
SELECT DISTINCT t.property_name, t.unit_number, t.house_number, s.street_id
  FROM
   (SELECT property_name, unit_number, house_number, street_name, locality_name
      FROM nsw_vg_raw.ps_row_b UNION

    SELECT NULL, unit_number, house_number, street_name, locality_name
      FROM nsw_vg_raw.ps_row_b_legacy UNION

    SELECT property_name, unit_number, house_number, street_name, suburb_name
      FROM nsw_vg_raw.land_value_row) as t

  LEFT JOIN nsw_gnb.locality l
         ON upper(l.locality_name) = upper(t.locality_name)

  LEFT JOIN nsw_gnb.street s
         ON upper(s.street_name) = upper(t.street_name)
        AND s.locality_id = l.locality_id;

--
-- # Temporary Tables
--
-- I'm going to have to do these links a ton may as well
-- create them all at once here and clean them up below.

CREATE TEMP TABLE sourced_raw_land_values AS
SELECT sfl.source_id, r.*
  FROM nsw_vg_raw.land_value_row as r
  LEFT JOIN meta.source_file AS sf ON sf.file_path = r.source_file_name
  LEFT JOIN meta.source_file_line AS sfl
         ON sfl.source_file_line = r.source_line_number
        AND sfl.source_file_id = sf.source_file_id;

CREATE TEMP TABLE sourced_raw_property_sales_a_legacy AS
SELECT sbp.source_id, r.*
  FROM nsw_vg_raw.ps_row_a_legacy as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE TEMP TABLE sourced_raw_property_sales_b_legacy AS
SELECT sbp.source_id, r.*
  FROM nsw_vg_raw.ps_row_b_legacy as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE TEMP TABLE sourced_raw_property_sales_a AS
SELECT sbp.source_id, r.*
  FROM nsw_vg_raw.ps_row_a as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE TEMP TABLE sourced_raw_property_sales_b AS
SELECT sbp.source_id, r.*
  FROM nsw_vg_raw.ps_row_b as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE TEMP TABLE sourced_raw_property_sales_c AS
SELECT sbp.source_id, r.*
  FROM nsw_vg_raw.ps_row_c as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

CREATE TEMP TABLE sourced_raw_property_sales_d AS
SELECT sbp.source_id, r.*
  FROM nsw_vg_raw.ps_row_d as r
  LEFT JOIN meta.source_file AS sf USING (file_path)
  LEFT JOIN meta.source_byte_position AS sbp
         ON sbp.source_byte_position = r.position
        AND sbp.source_file_id = sf.source_file_id;

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
-- ## Ingest Property Description
--

INSERT INTO nsw_lrs.legal_description(source_id, effective_date, property_id, legal_description)
SELECT * FROM
   (SELECT source_id, source_date, property_id, property_description
      FROM sourced_raw_land_values
     UNION
    SELECT c.source_id, a.date_provided, c.property_id, c.property_description
      FROM sourced_raw_property_sales_c as c
      LEFT JOIN nsw_vg_raw.ps_row_a as a USING (file_path)) as temp
  WHERE source_date IS NOT NULL;

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

DROP TABLE sourced_raw_land_values;
DROP TABLE sourced_raw_property_sales_a_legacy;
DROP TABLE sourced_raw_property_sales_b_legacy;
DROP TABLE sourced_raw_property_sales_a;
DROP TABLE sourced_raw_property_sales_b;
DROP TABLE sourced_raw_property_sales_c;
DROP TABLE sourced_raw_property_sales_d;

