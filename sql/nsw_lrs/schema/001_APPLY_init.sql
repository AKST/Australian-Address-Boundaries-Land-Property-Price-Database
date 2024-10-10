CREATE SCHEMA IF NOT EXISTS nsw_lrs;

CREATE TABLE IF NOT EXISTS nsw_lrs.property (
  property_id INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS nsw_lrs.parcel (
  parcel_id BIGSERIAL PRIMARY KEY,
  plan varchar(6),
  section varchar(2),
  lot varchar(5),
  UNIQUE (plan, section, lot)
);

