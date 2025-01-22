CREATE FUNCTION pg_temp.cleanup_parcel_id(in_text text) RETURNS text
LANGUAGE
  plpgsql IMMUTABLE
  PARALLEL SAFE
AS $$
DECLARE
  single_slash text;
BEGIN
  single_slash := REGEXP_REPLACE(in_text, '/{2,}', '/', 'g');
  RETURN REGEXP_REPLACE(single_slash, '/DP([0-9]+)', '/\1', 'g');
END;
$$;



INSERT INTO nsw_lrs.base_parcel(base_parcel_id, base_parcel_kind)
SELECT base_parcel_id, base_parcel_kind
  FROM (SELECT DISTINCT ON (lot_id_string)
             nsw_lrs.get_base_parcel_id(lot_id_string) as base_parcel_id,
             nsw_lrs.get_base_parcel_kind(lot_id_string) as base_parcel_kind
        FROM nsw_spatial_lppt_raw.lot_feature_layer
        ORDER BY lot_id_string, last_update DESC) u
   WHERE NOT EXISTS (
    SELECT 1 FROM nsw_lrs.base_parcel p
     WHERE p.base_parcel_id = u.base_parcel_id);



INSERT INTO nsw_lrs.base_parcel_geometry(base_parcel_id, geometry)
SELECT nsw_lrs.get_base_parcel_id(lot_id_string), geometry
FROM (
    SELECT DISTINCT ON (lot_id_string) lot_id_string, geometry
    FROM nsw_spatial_lppt_raw.lot_feature_layer
    ORDER BY lot_id_string, last_update DESC
) t;

DROP FUNCTION pg_temp.cleanup_parcel_id;
