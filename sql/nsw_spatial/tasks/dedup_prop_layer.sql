WITH unique_properties AS (
  SELECT DISTINCT ON (property_id) property_id
   FROM nsw_spatial_lppt_raw.property_feature_layer
   WHERE principle_address_type = 1
)
INSERT INTO nsw_lrs.property(property_id)
SELECT property_id FROM unique_properties u
 WHERE NOT EXISTS (
    SELECT 1 FROM nsw_lrs.property p
     WHERE p.property_id = u.property_id);

WITH unique_properties AS (
  SELECT DISTINCT ON (property_id) rid
   FROM nsw_spatial_lppt_raw.property_feature_layer
   WHERE principle_address_type = 1
   ORDER BY property_id, address_string_oid)
INSERT INTO nsw_lrs.property_geometry(property_id, geometry)
SELECT property_id, geometry
  FROM unique_properties u
  LEFT JOIN nsw_spatial_lppt_raw.property_feature_layer p USING (rid);
