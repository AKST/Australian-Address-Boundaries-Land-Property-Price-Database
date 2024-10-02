CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.source_file (
  source_file_id SERIAL PRIMARY KEY,
  file_name TEXT NOT NULL UNIQUE,
  date_recorded DATE NOT NULL,
  date_published DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS meta.source (
  source_id BIGSERIAL PRIMARY KEY,
  src_file_id INT NOT NULL,
  file_position int,

  FOREIGN KEY (src_file_id) REFERENCES meta.source_file(source_file_id)
);

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
  id BIGSERIAL PRIMARY KEY,
  effective_date DATE NOT NULL,
  source_id bigint NOT NULL,

  FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

