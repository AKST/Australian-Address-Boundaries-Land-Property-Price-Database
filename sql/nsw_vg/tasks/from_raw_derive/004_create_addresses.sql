--
-- # Establish Street & Suburb Names
--

INSERT INTO nsw_gnb.locality (locality_name)
SELECT DISTINCT upper(t.locality_name) FROM
   (SELECT locality_name FROM nsw_vg_raw.ps_row_b_legacy UNION
    SELECT locality_name FROM nsw_vg_raw.ps_row_b UNION
    SELECT suburb_name FROM nsw_vg_raw.land_value_row) as t
  WHERE t.locality_name != NULL
     ON CONFLICT (locality_name) DO NOTHING;

INSERT INTO nsw_gnb.street (street_name, locality_id)
SELECT DISTINCT upper(t.street_name), l.locality_id FROM
   (SELECT street_name, locality_name FROM nsw_vg_raw.ps_row_b_legacy UNION
    SELECT street_name, locality_name FROM nsw_vg_raw.ps_row_b UNION
    SELECT street_name, suburb_name FROM nsw_vg_raw.land_value_row) as t
  LEFT JOIN nsw_gnb.locality l
         ON upper(l.locality_name) = upper(t.locality_name)
  WHERE t.street_name IS NOT NULL
     ON CONFLICT (street_name, locality_id) DO NOTHING;

--
-- # Addresses
--

INSERT INTO nsw_gnb.address (property_name, unit_number, street_number, street_id)
SELECT DISTINCT t.property_name, t.unit_number, t.house_number, s.street_id
  FROM
   (SELECT property_name, unit_number, house_number, street_name, locality_name
      FROM nsw_vg_raw.ps_row_b UNION

    SELECT NULL, unit_number, house_number, street_name, locality_name
      FROM nsw_vg_raw.ps_row_b_legacy UNION

    SELECT property_name, unit_number, house_number, street_name, suburb_name
      FROM nsw_vg_raw.land_value_row) as t

  LEFT JOIN nsw_gnb.locality l
         ON upper(l.locality_name) = upper(t.locality_name)

  LEFT JOIN nsw_gnb.street s
         ON upper(s.street_name) = upper(t.street_name)
        AND s.locality_id = l.locality_id;


