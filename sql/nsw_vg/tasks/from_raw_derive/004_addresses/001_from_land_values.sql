--
-- NOTE THIS WILL EVENTUALLY BE REPLACED ONCE A MORE
-- RELIABLE SOURCE OF ADDRESS INFORMATION BECOMES
-- AVAILABLE TO LINK TO Valuations & Sales DATA. THE
-- DATA FROM THE BULK VALUATIONS & PSI IS PRETTY BAD.
--
-- # Ingest into General Naming board
--
-- ## Establish Street & Suburb Names
--
-- TODO consume PSI data


INSERT INTO nsw_gnb.locality(locality_name)
SELECT * FROM
   (SELECT DISTINCT upper(suburb_name) as locality_name
      FROM nsw_vg_raw.land_value_row) as t
  WHERE t.locality_name IS NOT NULL
     ON CONFLICT (locality_name) DO NOTHING;

INSERT INTO nsw_gnb.street (street_name, locality_id)
SELECT DISTINCT t.street_name, l.locality_id FROM
   (SELECT DISTINCT
           upper(street_name) as street_name,
           upper(suburb_name) as locality_name
      FROM nsw_vg_raw.land_value_row) as t
  LEFT JOIN nsw_gnb.locality l USING (locality_name)
  WHERE t.street_name IS NOT NULL
     ON CONFLICT (street_name, locality_id) DO NOTHING;

--
-- ## Addresses
--
-- TODO consume PSI data

WITH sourced_raw_land_values AS (
  SELECT source_id, property_id, property_name,
         unit_number, house_number as street_number,
         upper(street_name) as street_name,
         upper(suburb_name) as locality_name,
         source_date, postcode
    FROM nsw_vg_raw.land_value_row as r
    LEFT JOIN nsw_vg_raw.land_value_row_source USING (land_value_row_id))

INSERT INTO nsw_gnb.address (
  source_id,
  effective_date,
  property_id,
  property_name,
  unit_number,
  street_number,
  street_id,
  locality_id,
  postcode)
SELECT source_id,
       source_date,
       property_id,
       property_name,
       unit_number,
       street_number,
       street_id,
       locality_id,
       postcode
  FROM sourced_raw_land_values
  LEFT JOIN nsw_gnb.locality l USING (locality_name)
  LEFT JOIN nsw_gnb.street s USING (street_name, locality_id);

