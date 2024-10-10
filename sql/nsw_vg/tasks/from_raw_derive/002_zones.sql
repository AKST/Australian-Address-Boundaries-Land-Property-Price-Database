--
-- # Record Zones
--
-- We can do this before setting up meta data, this
-- is mostly for establishing these identifiers.
--

INSERT INTO nsw_planning.epa_2006_zone(zone_code)
SELECT * FROM
   (SELECT zone_code FROM nsw_vg_raw.land_value_row
     WHERE zone_standard = 'ep&a_2006'
     UNION
    SELECT zone_code FROM nsw_vg_raw.ps_row_b
     WHERE zone_standard = 'ep&a_2006') as t
   ON CONFLICT (zone_code) DO NOTHING;

