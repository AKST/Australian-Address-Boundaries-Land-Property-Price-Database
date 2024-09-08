
INSERT
  INTO nsw_valuer_general.source_file (source_file_name, source_date)
SELECT DISTINCT source_file_name, source_date
  FROM nsw_valuer_general.raw_entries_lv;

INSERT
  INTO nsw_valuer_general.source (source_file_id, source_file_position)
SELECT DISTINCT s.source_file_id, re.source_file_position
  FROM nsw_valuer_general.raw_entries_lv re
  LEFT JOIN nsw_valuer_general.source_file s
    ON s.source_file_name = re.source_file_name;

-- Creates district table from unique district rows in raw_entries_lv
INSERT
  INTO nsw_valuer_general.district (district_code, district_name)
SELECT DISTINCT district_code, district_name
  FROM nsw_valuer_general.raw_entries_lv;

-- Creates suburbs from unique suburbs from raw_entries_lv
INSERT 
  INTO nsw_valuer_general.suburb (suburb_name, district_code)
SELECT DISTINCT suburb_name, district_code
  FROM nsw_valuer_general.raw_entries_lv;

-- Creates streets from unique streets from raw_entries_lv
INSERT
  INTO nsw_valuer_general.street (street_name, suburb_id, postcode, district_code)
SELECT DISTINCT re.street_name, s.suburb_id, re.postcode, re.district_code
  FROM nsw_valuer_general.raw_entries_lv re
  LEFT JOIN nsw_valuer_general.suburb s
    ON re.suburb_name = s.suburb_name
   AND re.district_code = s.district_code
 WHERE re.street_name IS NOT NULL;

-- -- Creates properties
INSERT INTO nsw_valuer_general.property (
    property_id, district_code, property_type, property_name, unit_number,
    house_number, street_id, suburb_id, postcode,
    zone_code, area, source_id)
SELECT
    re.property_id, re.district_code, re.property_type,
    re.property_name, re.unit_number, re.house_number,
    st.street_id, su.suburb_id, re.postcode, re.zone_code,
    CASE
       WHEN re.area_type = 'H' THEN re.area * 10000
       WHEN re.area_type = 'M' THEN re.area
       WHEN re.area_type IS NULL THEN re.area
    END,
    src.source_id
  FROM nsw_valuer_general.raw_entries_lv re
  LEFT JOIN nsw_valuer_general.suburb su
    ON re.suburb_name = su.suburb_name  
   AND re.district_code = su.district_code                
  LEFT JOIN nsw_valuer_general.street st 
    ON re.street_name = st.street_name 
   AND st.suburb_id = su.suburb_id
   AND re.postcode = st.postcode
   AND re.district_code = su.district_code
  LEFT JOIN nsw_valuer_general.source_file sf
    ON sf.source_file_name = re.source_file_name
  LEFT JOIN nsw_valuer_general.source src
    ON src.source_file_id = sf.source_file_id
   AND src.source_file_position = re.source_file_position;

-- Creates property descriptions
INSERT INTO nsw_valuer_general.property_description (
    property_id, property_description)
SELECT
    re.property_id, re.property_description
  FROM nsw_valuer_general.raw_entries_lv re;


INSERT INTO nsw_valuer_general.valuations (
    property_id, base_date, land_value, authority, basis)
SELECT property_id, base_date_1, land_value_1, authority_1, basis_1
  FROM nsw_valuer_general.raw_entries_lv
 WHERE base_date_1 IS NOT NULL;

INSERT INTO nsw_valuer_general.valuations (
    property_id, base_date, land_value, authority, basis)
SELECT property_id, base_date_2, land_value_2, authority_2, basis_2
  FROM nsw_valuer_general.raw_entries_lv
 WHERE base_date_2 IS NOT NULL;

INSERT INTO nsw_valuer_general.valuations (
    property_id, base_date, land_value, authority, basis)
SELECT property_id, base_date_3, land_value_3, authority_3, basis_3
  FROM nsw_valuer_general.raw_entries_lv
 WHERE base_date_3 IS NOT NULL;

INSERT INTO nsw_valuer_general.valuations (
    property_id, base_date, land_value, authority, basis)
SELECT property_id, base_date_4, land_value_4, authority_4, basis_4
  FROM nsw_valuer_general.raw_entries_lv
 WHERE base_date_4 IS NOT NULL;

INSERT INTO nsw_valuer_general.valuations (
    property_id, base_date, land_value, authority, basis)
SELECT property_id, base_date_5, land_value_5, authority_5, basis_5
  FROM nsw_valuer_general.raw_entries_lv
 WHERE base_date_5 IS NOT NULL;