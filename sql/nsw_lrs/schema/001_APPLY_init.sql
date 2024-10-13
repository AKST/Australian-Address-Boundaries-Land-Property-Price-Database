CREATE SCHEMA IF NOT EXISTS nsw_lrs;

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

CREATE TABLE IF NOT EXISTS nsw_lrs.parcel (
  parcel_id BIGSERIAL PRIMARY KEY,
  parcel_plan varchar(6),
  parcel_section varchar(2),
  parcel_lot varchar(5),
  UNIQUE (parcel_plan, parcel_section, parcel_lot)
);

