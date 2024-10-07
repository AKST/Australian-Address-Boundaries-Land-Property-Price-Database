CREATE SCHEMA IF NOT EXISTS nsw_vg;

CREATE TYPE nsw_vg.zoning_standard AS ENUM (
  'legacy_vg_2011',
  'ep&a_2006',
  'unknown'
);

CREATE TYPE nsw_vg.sale_participant AS ENUM ('V', 'P');

CREATE SCHEMA IF NOT EXISTS nsw_vg_raw;
