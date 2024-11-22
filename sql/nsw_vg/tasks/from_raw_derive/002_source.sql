SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

--
-- # Establish source for Land Values
-- ## Requires
--
-- 1. Populating the contents of meta.file_source
-- 2. Generating ids for the source table
--


SET session_replication_role = 'replica';

INSERT INTO nsw_vg_raw.land_value_row_source(land_value_row_id, source_id)
  SELECT land_value_row_id, nextval('meta.source_source_id_seq')
  FROM nsw_vg_raw.land_value_row a;

INSERT INTO meta.source(source_id)
  SELECT source_id
  FROM nsw_vg_raw.land_value_row_source a;

SET session_replication_role = 'origin';

CREATE TEMP TABLE pg_temp.lv_uningested_files AS
  WITH unique_files AS (
    SELECT DISTINCT ON (source_file_name) source_file_name, source_date
    FROM nsw_vg_raw.land_value_row_source
    LEFT JOIN nsw_vg_raw.land_value_row USING (land_value_row_id))
  SELECT *, nextval('meta.file_source_file_source_id_seq') AS file_source_id
  FROM unique_files;

INSERT INTO meta.file_source(file_source_id, file_path, date_recorded, date_published)
  SELECT file_source_id, source_file_name, CURRENT_DATE, source_date
  FROM pg_temp.lv_uningested_files;

INSERT INTO meta.source_file_line(source_id, file_source_id, source_file_line)
  SELECT source_id, file_source_id, source_line_number
  FROM nsw_vg_raw.land_value_row_source
  LEFT JOIN nsw_vg_raw.land_value_row USING (land_value_row_id)
  JOIN pg_temp.lv_uningested_files USING (source_file_name);

DROP TABLE pg_temp.lv_uningested_files;

--
-- # Ingest PSI
--
-- ## Create Source links
--

SET session_replication_role = 'replica';

INSERT INTO nsw_vg_raw.ps_row_a_legacy_source(ps_row_a_legacy_id, source_id)
  SELECT ps_row_a_legacy_id, nextval('meta.source_source_id_seq')
  FROM nsw_vg_raw.ps_row_a_legacy;

INSERT INTO nsw_vg_raw.ps_row_b_legacy_source(ps_row_b_legacy_id, source_id)
  SELECT ps_row_b_legacy_id, nextval('meta.source_source_id_seq')
  FROM nsw_vg_raw.ps_row_b_legacy;

INSERT INTO nsw_vg_raw.ps_row_a_source(ps_row_a_id, source_id)
  SELECT ps_row_a_id, nextval('meta.source_source_id_seq')
  FROM nsw_vg_raw.ps_row_a;

INSERT INTO nsw_vg_raw.ps_row_b_source(ps_row_b_id, source_id)
  SELECT ps_row_b_id, nextval('meta.source_source_id_seq')
  FROM nsw_vg_raw.ps_row_b;

INSERT INTO nsw_vg_raw.ps_row_c_source(ps_row_c_id, source_id)
  SELECT ps_row_c_id, nextval('meta.source_source_id_seq')
  FROM nsw_vg_raw.ps_row_c;

INSERT INTO nsw_vg_raw.ps_row_d_source(ps_row_d_id, source_id)
  SELECT ps_row_d_id, nextval('meta.source_source_id_seq')
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

SET session_replication_role = 'origin';

--
-- ## Create Temp Table
--

CREATE TEMP TABLE pg_temp.psl_uningested_files AS
  WITH unique_files AS (
    SELECT DISTINCT ON (file_path) file_path, date_provided
    FROM nsw_vg_raw.ps_row_a_legacy_source
    LEFT JOIN nsw_vg_raw.ps_row_a_legacy USING (ps_row_a_legacy_id))
  SELECT *, nextval('meta.file_source_file_source_id_seq') AS file_source_id
  FROM unique_files;

CREATE TEMP TABLE pg_temp.ps_uningested_files AS
  WITH unique_files AS (
    SELECT DISTINCT ON (file_path) file_path, date_provided
    FROM nsw_vg_raw.ps_row_a_source
    LEFT JOIN nsw_vg_raw.ps_row_a USING (ps_row_a_id))
  SELECT *, nextval('meta.file_source_file_source_id_seq') AS file_source_id
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

ALTER TABLE nsw_vg_raw.land_value_row_source VALIDATE CONSTRAINT land_value_row_source_source_id_fkey;
ALTER TABLE nsw_vg_raw.land_value_row_source VALIDATE CONSTRAINT land_value_row_source_land_value_row_id_fkey;

ALTER TABLE nsw_vg_raw.ps_row_a_legacy_source VALIDATE CONSTRAINT ps_row_a_legacy_source_ps_row_a_legacy_id_fkey;
ALTER TABLE nsw_vg_raw.ps_row_a_legacy_source VALIDATE CONSTRAINT ps_row_a_legacy_source_source_id_fkey;
ALTER TABLE nsw_vg_raw.ps_row_b_legacy_source VALIDATE CONSTRAINT ps_row_b_legacy_source_ps_row_b_legacy_id_fkey;
ALTER TABLE nsw_vg_raw.ps_row_b_legacy_source VALIDATE CONSTRAINT ps_row_b_legacy_source_source_id_fkey;

ALTER TABLE nsw_vg_raw.ps_row_a_source VALIDATE CONSTRAINT ps_row_a_source_ps_row_a_id_fkey;
ALTER TABLE nsw_vg_raw.ps_row_a_source VALIDATE CONSTRAINT ps_row_a_source_source_id_fkey;
ALTER TABLE nsw_vg_raw.ps_row_b_source VALIDATE CONSTRAINT ps_row_b_source_ps_row_b_id_fkey;
ALTER TABLE nsw_vg_raw.ps_row_b_source VALIDATE CONSTRAINT ps_row_b_source_source_id_fkey;
ALTER TABLE nsw_vg_raw.ps_row_c_source VALIDATE CONSTRAINT ps_row_c_source_ps_row_c_id_fkey;
ALTER TABLE nsw_vg_raw.ps_row_c_source VALIDATE CONSTRAINT ps_row_c_source_source_id_fkey;
ALTER TABLE nsw_vg_raw.ps_row_d_source VALIDATE CONSTRAINT ps_row_d_source_ps_row_d_id_fkey;
ALTER TABLE nsw_vg_raw.ps_row_d_source VALIDATE CONSTRAINT ps_row_d_source_source_id_fkey;
