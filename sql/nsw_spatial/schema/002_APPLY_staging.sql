
-- https://portal.spatial.nsw.gov.au/server/rest/services/NSW_Land_Parcel_Property_Theme/FeatureServer/12
CREATE TABLE IF NOT EXISTS nsw_spatial_lppt_raw.property_feature_layer (
  property_feature_layer_row_id BIGSERIAL PRIMARY KEY,
  rid int
  property_id int NOT NULL,
  principal_address_site_oid int NOT NULL,
  addresss_string_oid int NOT NULL,
  property_type int NOT NULL,
  super_lot varchar(1) NOT NULL,
  address TEXT NOT NULL,
  principle_addresss_type INT NOT NULL,
  created_date DATETIME NOT NULL,
  start_date DATETIME NOT NULL,
  end_date DATETIME NOT NULL,
  last_update DATETIME NOT NULL,
  shape_uuid TEXT NOT NULL,
  change_type TEXT NOT NULL,
  shape_length float,
  shape_area float,
  geometry GEOMETRY(MultiPolygon, 7844)
);

-- https://portal.spatial.nsw.gov.au/server/rest/services/NSW_Land_Parcel_Property_Theme/FeatureServer/8
CREATE TABLE IF NOT EXISTS nsw_spatial_lppt_raw.lot_feature_layer (
  lot_feature_layer_row_id BIGSERIAL PRIMARY KEY,
  object_id int NOT NULL,
  lot_id_string TEXT NOT NULL,
  controlling_authority_oid NOT NULL,
  cad_id int NOT NULL,
  plan_oid int NOT NULL,
  plan_number int NOT NULL,
  plan_label TEXT NOT NULL,
  stratum_level int NOT NULL,
  has_stratum BOOL NOT NULL,
  lot_number int NOT NULL,
  section_number int,
  plan_lot_area float,
  plan_lot_area_units TEXT,
  created_date DATETIME NOT NULL,
  modified_date DATETIME,
  start_date DATETIME NOT NULL,
  end_date DATETIME NOT NULL,
  last_update DATETIME NOT NULL,
  shape_uuid TEXT NOT NULL,
  change_type TEXT NOT NULL,
  shape_length float,
  shape_area float,
  geometry GEOMETRY(MultiPolygon, 7844)
);
