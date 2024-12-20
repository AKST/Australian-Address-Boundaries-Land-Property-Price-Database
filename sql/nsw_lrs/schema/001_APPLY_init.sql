CREATE SCHEMA IF NOT EXISTS nsw_lrs;

--
-- This exists for the sake of joining data with property &
-- strata_lot_num information. In SQL typically null != null
-- so joining like like this:
--
--   SELECT *
--     FROM nsw_lrs.property_area
--     LEFT JOIN nsw_lrs.primary_purpose USING (property_id, strata_lot_num)
--
-- would fail for anything without a strata lot (any non appartment building).
--
CREATE TYPE nsw_lrs.normalised_property_id AS (property_id INT, strata_lot_num INT);

CREATE TYPE nsw_lrs.sale_participant AS ENUM ('V', 'P');
CREATE TYPE nsw_lrs.property_nature AS ENUM (
  'Residence',
  'Vaccant',
  'Other'
);

--
-- ## How are properties defined
--
-- A property is one or more structure at least one
-- portion of a parcel of land. I guess it's a you
-- know it when you see it kind of thing.
--
CREATE TABLE IF NOT EXISTS nsw_lrs.property (
  property_id INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS nsw_lrs.primary_purpose(
  primary_purpose_id SERIAL PRIMARY KEY,
  primary_purpose varchar(20) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS nsw_lrs.parcel (
  parcel_id varchar(20) PRIMARY KEY,
  parcel_plan varchar(9) NOT NULL,
  parcel_section varchar(4),
  parcel_lot varchar(5) NOT NULL,
  UNIQUE (parcel_plan, parcel_section, parcel_lot)
);

