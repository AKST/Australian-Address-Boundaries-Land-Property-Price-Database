--
-- # Establish source for Land Values
-- ## Requires
--
-- 1. Populating the contents of meta.file_source
-- 2. Generating ids for the source table
--


SET session_replication_role = 'replica';

INSERT INTO nsw_vg_raw.land_value_row_complement(property_id, source_date, effective_date, source_id)
  SELECT property_id,
         source_date,
         COALESCE(base_date_1, source_date),
         uuid_generate_v4()
  FROM nsw_vg_raw.land_value_row;

INSERT INTO meta.source(source_id) SELECT source_id FROM nsw_vg_raw.land_value_row_complement;

CREATE TEMP TABLE pg_temp.lv_uningested_files AS
  WITH unique_files AS (
    SELECT DISTINCT ON (source_file_name) source_file_name, source_date
    FROM nsw_vg_raw.land_value_row_complement
    LEFT JOIN nsw_vg_raw.land_value_row USING (property_id, source_date))
  SELECT *, uuid_generate_v4() AS file_source_id
  FROM unique_files;

INSERT INTO meta.file_source(file_source_id, file_path, date_recorded, date_published)
  SELECT file_source_id, source_file_name, CURRENT_DATE, source_date
  FROM pg_temp.lv_uningested_files;

INSERT INTO meta.source_file_line(source_id, file_source_id, source_file_line)
  SELECT source_id, file_source_id, source_line_number
  FROM nsw_vg_raw.land_value_row_complement
  LEFT JOIN nsw_vg_raw.land_value_row USING (property_id, source_date)
  JOIN pg_temp.lv_uningested_files USING (source_file_name);

DROP TABLE pg_temp.lv_uningested_files;

--
-- # Ingest PSI
--
-- ## Create Source links
--

INSERT INTO nsw_vg_raw.ps_row_a_legacy_source(ps_row_a_legacy_id, source_id)
  SELECT ps_row_a_legacy_id, uuid_generate_v4()
  FROM nsw_vg_raw.ps_row_a_legacy;

INSERT INTO nsw_vg_raw.ps_row_b_legacy_source(ps_row_b_legacy_id, source_id)
  SELECT ps_row_b_legacy_id, uuid_generate_v4()
  FROM nsw_vg_raw.ps_row_b_legacy;

INSERT INTO nsw_vg_raw.ps_row_a_source(ps_row_a_id, source_id)
  SELECT ps_row_a_id, uuid_generate_v4()
  FROM nsw_vg_raw.ps_row_a;

INSERT INTO nsw_vg_raw.ps_row_b_source(ps_row_b_id, source_id)
  SELECT ps_row_b_id, uuid_generate_v4()
  FROM nsw_vg_raw.ps_row_b;

INSERT INTO nsw_vg_raw.ps_row_c_source(ps_row_c_id, source_id)
  SELECT ps_row_c_id, uuid_generate_v4()
  FROM nsw_vg_raw.ps_row_c;

INSERT INTO nsw_vg_raw.ps_row_d_source(ps_row_d_id, source_id)
  SELECT ps_row_d_id, uuid_generate_v4()
  FROM nsw_vg_raw.ps_row_d;

--
-- ## Create Sources
--

INSERT INTO meta.source(source_id) SELECT source_id FROM nsw_vg_raw.ps_row_a_legacy_source;
INSERT INTO meta.source(source_id) SELECT source_id FROM nsw_vg_raw.ps_row_b_legacy_source;
INSERT INTO meta.source(source_id) SELECT source_id FROM nsw_vg_raw.ps_row_a_source;
INSERT INTO meta.source(source_id) SELECT source_id FROM nsw_vg_raw.ps_row_b_source;
INSERT INTO meta.source(source_id) SELECT source_id FROM nsw_vg_raw.ps_row_c_source;
INSERT INTO meta.source(source_id) SELECT source_id FROM nsw_vg_raw.ps_row_d_source;

--
-- ## Create Temp Table
--

CREATE TEMP TABLE pg_temp.psl_uningested_files AS
  WITH unique_files AS (
    SELECT DISTINCT ON (file_path) file_path, date_provided
    FROM nsw_vg_raw.ps_row_a_legacy_source
    LEFT JOIN nsw_vg_raw.ps_row_a_legacy USING (ps_row_a_legacy_id))
  SELECT *, uuid_generate_v4() AS file_source_id
  FROM unique_files;

CREATE TEMP TABLE pg_temp.ps_uningested_files AS
  WITH unique_files AS (
    SELECT DISTINCT ON (file_path) file_path, date_provided
    FROM nsw_vg_raw.ps_row_a_source
    LEFT JOIN nsw_vg_raw.ps_row_a USING (ps_row_a_id))
  SELECT *, uuid_generate_v4() AS file_source_id
  FROM unique_files;

--
-- ## Create File Source
--

INSERT INTO meta.file_source(file_source_id, file_path, date_recorded, date_published)
  SELECT file_source_id, file_path, CURRENT_DATE, date_provided
  FROM pg_temp.psl_uningested_files;

INSERT INTO meta.file_source(file_source_id, file_path, date_recorded, date_published)
  SELECT file_source_id, file_path, CURRENT_DATE, date_provided
  FROM pg_temp.ps_uningested_files;

--
-- ## Create Position Entries
--

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_a_legacy_source
  LEFT JOIN nsw_vg_raw.ps_row_a_legacy USING (ps_row_a_legacy_id)
  JOIN pg_temp.psl_uningested_files USING (file_path);

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_b_legacy_source
  LEFT JOIN nsw_vg_raw.ps_row_b_legacy USING (ps_row_b_legacy_id)
  JOIN pg_temp.psl_uningested_files USING (file_path);

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_a_source
  LEFT JOIN nsw_vg_raw.ps_row_a USING (ps_row_a_id)
  JOIN pg_temp.ps_uningested_files USING (file_path);

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_b_source
  LEFT JOIN nsw_vg_raw.ps_row_b USING (ps_row_b_id)
  JOIN pg_temp.ps_uningested_files USING (file_path);

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_c_source
  LEFT JOIN nsw_vg_raw.ps_row_c USING (ps_row_c_id)
  JOIN pg_temp.ps_uningested_files USING (file_path);

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_d_source
  LEFT JOIN nsw_vg_raw.ps_row_d USING (ps_row_d_id)
  JOIN pg_temp.ps_uningested_files USING (file_path);

--
-- ## Clean up
--

DROP TABLE pg_temp.psl_uningested_files;
DROP TABLE pg_temp.ps_uningested_files;

--
-- # End
--
-- Validate constraints, get names of constraints via this SQL
--
-- ```sql
-- SELECT constraint_name, constraint_type
--   FROM information_schema.table_constraints
--   WHERE table_name = 'your_table_name'
--     AND table_schema = 'your_schema_name';
-- ```
--

SET session_replication_role = 'origin';

SELECT meta.check_constraints('nsw_vg_raw', 'land_value_row_complement');
SELECT meta.check_constraints('nsw_vg_raw', 'ps_row_a_legacy_source');
SELECT meta.check_constraints('nsw_vg_raw', 'ps_row_b_legacy_source');
SELECT meta.check_constraints('nsw_vg_raw', 'ps_row_a_source');
SELECT meta.check_constraints('nsw_vg_raw', 'ps_row_b_source');
SELECT meta.check_constraints('nsw_vg_raw', 'ps_row_c_source');
SELECT meta.check_constraints('nsw_vg_raw', 'ps_row_d_source');

