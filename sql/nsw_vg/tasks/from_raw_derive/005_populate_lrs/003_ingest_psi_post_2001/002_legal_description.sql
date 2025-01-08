--
-- ## Ingest Property Description
--

SET session_replication_role = 'replica';

INSERT INTO nsw_lrs.legal_description(
  source_id,
  effective_date,
  property_id,
  strata_lot_number,
  legal_description,
  legal_description_id,
  legal_description_kind)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    source_id,
    effective_date,
    property_id,
    strata_lot_number,
    full_property_description,
    uuid_generate_v4(),
    (case
      when date_provided > '2004-08-17' then '> 2004-08-17'
      else 'initial'
    end)::nsw_lrs.legal_description_kind
  FROM pg_temp.consolidated_property_description_c as c
  WHERE full_property_description IS NOT NULL
    AND NOT seen_in_land_values
  ORDER BY effective_date,
           property_id,
           strata_lot_number,
           date_provided DESC;

SET session_replication_role = 'origin';
SELECT meta.check_constraints('nsw_lrs', 'legal_description');

