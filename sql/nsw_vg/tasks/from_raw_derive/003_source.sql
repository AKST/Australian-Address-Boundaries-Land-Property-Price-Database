--
-- # Establish sources
--
-- Many of the subsequent portions of the ingestion will
-- make assumption these exist. Pre populating will make
-- those steps simpler.
--
-- ## First populate `source_file`
--
-- Ensure a meta.source_file row exists for each piece of raw data
--

INSERT INTO meta.source_file(
  file_path,
  date_recorded,
  date_published)
SELECT t.source_file_name, CURRENT_DATE, t.source_date
  FROM
   (SELECT source_file_name, source_date
      FROM nsw_vg_raw.land_value_row
     UNION
    SELECT file_path, date_provided
      FROM nsw_vg_raw.ps_row_a_legacy
     UNION
    SELECT file_path, date_provided
      FROM nsw_vg_raw.ps_row_a) as t
  WHERE NOT EXISTS (
    SELECT 1 FROM meta.source_file sf
     WHERE sf.file_path = t.source_file_name);

--
-- ## Next populate source_file_line for land values
--

CREATE TEMP TABLE nsw_vg_lv_missing_source AS WITH
  lvr_missing_source AS
   (SELECT sf.source_file_id,
           r.source_line_number,
           ROW_NUMBER() OVER () AS rn
      FROM nsw_vg_raw.land_value_row as r
      LEFT JOIN meta.source_file AS sf
             ON sf.file_path = r.source_file_name
     WHERE NOT EXISTS (
       SELECT 1 FROM meta.source_file_line sfl
        WHERE sfl.source_file_id = sf.source_file_id
          AND sfl.source_file_line = r.source_line_number)),

  source_ids AS
   (SELECT nextval('meta.source_source_id_seq') AS source_id,
           ROW_NUMBER() OVER () AS rn
      FROM generate_series(1, (SELECT COUNT(*) FROM lvr_missing_source)))

  SELECT lvr.*, source_ids.source_id
    FROM lvr_missing_source as lvr
    LEFT JOIN source_ids USING (rn);

INSERT INTO meta.source(source_id)
SELECT source_id FROM nsw_vg_lv_missing_source;

INSERT INTO meta.source_file_line(source_id, source_file_id, source_file_line)
SELECT source_id, source_file_id, source_line_number
  FROM nsw_vg_lv_missing_source;

--
-- ## Then populate source_byte_position for property sales
--

CREATE TEMP TABLE nsw_vg_ps_missing_source AS WITH
  psr_missing_source AS
   (SELECT sf.source_file_id,
           r.position,
           ROW_NUMBER() OVER () AS rn
      FROM (SELECT file_path, position, 'al' FROM nsw_vg_raw.ps_row_a_legacy UNION
            SELECT file_path, position, 'bl' FROM nsw_vg_raw.ps_row_b_legacy UNION
            SELECT file_path, position, 'a' FROM nsw_vg_raw.ps_row_a UNION
            SELECT file_path, position, 'b' FROM nsw_vg_raw.ps_row_b UNION
            SELECT file_path, position, 'c' FROM nsw_vg_raw.ps_row_c UNION
            SELECT file_path, position, 'd' FROM nsw_vg_raw.ps_row_d) AS r
      LEFT JOIN meta.source_file AS sf USING (file_path)
     WHERE NOT EXISTS (
       SELECT 1 FROM meta.source_byte_position sfp
        WHERE sfp.source_file_id = sf.source_file_id
          AND sfp.source_byte_position = r.position)),

  source_ids AS
   (SELECT nextval('meta.source_source_id_seq') AS source_id,
           ROW_NUMBER() OVER () AS rn
      FROM generate_series(1, (SELECT COUNT(*) FROM psr_missing_source)))

  SELECT psr.*, source_ids.source_id
    FROM psr_missing_source as psr
    LEFT JOIN source_ids USING (rn);

INSERT INTO meta.source(source_id)
SELECT source_id FROM nsw_vg_ps_missing_source;

INSERT INTO meta.source_byte_position(source_id, source_file_id, source_byte_position)
SELECT source_id, source_file_id, position
  FROM nsw_vg_ps_missing_source;

DROP TABLE nsw_vg_lv_missing_source;
DROP TABLE nsw_vg_ps_missing_source;

