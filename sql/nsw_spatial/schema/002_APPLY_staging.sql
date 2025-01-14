
-- https://portal.spatial.nsw.gov.au/server/rest/services/NSW_Land_Parcel_Property_Theme/FeatureServer/12
CREATE TABLE IF NOT EXISTS nsw_spatial_lppt_raw.property_feature_layer (
  property_feature_layer_row_id BIGSERIAL PRIMARY KEY,
  rid BIGINT NOT NULL,
  property_id int NOT NULL,
  principal_address_site_oid int,
  address_string_oid int,
  property_type int NOT NULL,
  super_lot varchar(1) NOT NULL,
  address TEXT,
  principle_address_type INT,
  create_date TIMESTAMP NOT NULL,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP,
  last_update TIMESTAMP NOT NULL,
  shape_uuid TEXT NOT NULL,
  change_type TEXT NOT NULL,
  shape_length float,
  shape_area float,
  geometry GEOMETRY(MultiPolygon, 7844)
);

-- https://portal.spatial.nsw.gov.au/server/rest/services/NSW_Land_Parcel_Property_Theme/FeatureServer/8
CREATE TABLE IF NOT EXISTS nsw_spatial_lppt_raw.lot_feature_layer (
  lot_feature_layer_row_id BIGSERIAL PRIMARY KEY,
  object_id BIGINT NOT NULL,
  lot_id_string TEXT NOT NULL,
  controlling_authority_oid INT,
  cad_id int NOT NULL,
  plan_oid int,
  plan_number int,
  plan_label TEXT NOT NULL,
  its_title_status INT,
  its_lot_id INT,
  stratum_level int NOT NULL,
  has_stratum smallint NOT NULL,
  class_subtype INT,
  lot_number varchar(5),
  section_number varchar(4),
  plan_lot_area float,
  plan_lot_area_units TEXT,
  create_date TIMESTAMP NOT NULL,
  modified_date TIMESTAMP,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP,
  last_update TIMESTAMP NOT NULL,
  shape_uuid TEXT NOT NULL,
  change_type TEXT,
  shape_length float,
  shape_area float,
  geometry GEOMETRY(MultiPolygon, 7844)
);
