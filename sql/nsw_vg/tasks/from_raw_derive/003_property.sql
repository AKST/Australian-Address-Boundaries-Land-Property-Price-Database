
--
-- # Populate `nsw_lrs.property`
--
-- This step doesn't really require the meta data
-- but both will be  needed for subsequent steps.
--

SET session_replication_role = 'replica';

INSERT INTO nsw_lrs.property(property_id)
SELECT *
  FROM (
      SELECT property_id FROM nsw_vg_raw.land_value_row
       UNION
      SELECT property_id FROM nsw_vg_raw.ps_row_b
       UNION
      SELECT property_id FROM nsw_vg_raw.ps_row_b_legacy
  ) as t
  WHERE property_id IS NOT NULL
    ON CONFLICT (property_id) DO NOTHING;

SET session_replication_role = 'origin';

SELECT meta.check_constraints('nsw_lrs', 'property');
