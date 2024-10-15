CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.source (
  source_id BIGSERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS meta.file_source (
  file_source_id SERIAL PRIMARY KEY,
  file_path TEXT NOT NULL UNIQUE,

  --
  -- This is the date in which this source file
  -- was ingested into the database
  --
  date_recorded DATE,

  --
  -- This is the date in which this source was
  -- made public OR internally recorded on the
  -- side of the source, which ever comes first.
  --
  date_published DATE NOT NULL
);

CREATE INDEX idx_file_path_file_source
    ON meta.file_source(file_path);

CREATE TABLE IF NOT EXISTS meta.source_file_line (
  source_id bigint NOT NULL,
  file_source_id bigint NOT NULL,
  source_file_line int NOT NULL,
  UNIQUE (source_id, file_source_id),
  FOREIGN KEY (source_id) REFERENCES meta.source(source_id),
  FOREIGN KEY (file_source_id) REFERENCES meta.file_source(file_source_id)
);

CREATE INDEX idx_file_source_id_file_line_source_file_line
    ON meta.source_file_line(file_source_id, source_file_line);

CREATE TABLE IF NOT EXISTS meta.source_byte_position (
  source_id bigint NOT NULL,
  file_source_id bigint NOT NULL,
  source_byte_position bigint NOT NULL,
  UNIQUE (source_id, file_source_id),
  FOREIGN KEY (source_id) REFERENCES meta.source(source_id),
  FOREIGN KEY (file_source_id) REFERENCES meta.file_source(file_source_id)
);

CREATE INDEX idx_file_source_id_byte_position_source_byte_position
    ON meta.source_byte_position(file_source_id, source_byte_position);

-- CREATE VIEW

-- This is a general data type to record the effective date of an
-- observation, here are some examples:
--
--   1. You're processing property sales and there are attributes such as
--      a zoning for the piece of land the property was on.
--
--      - The observation is the association of the zoning with that land.
--      - The event is when the sale of the land.
--      - The effective_date is the date this is recorded and known to be true.
--
CREATE TABLE IF NOT EXISTS meta.event (
  source_id bigint NOT NULL,
  effective_date DATE NOT NULL,
  FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);
