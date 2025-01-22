--
-- # Property Data Meta
--
-- Not to be confused with the property description itself
-- but this schema is for stuff relating to metadata derived
-- from the legal property description.
--

--
-- ## property parcel association
--
-- A property can be made up of multiple parcels as well
-- as partial parcels shared between different properties.
--
CREATE TABLE IF NOT EXISTS nsw_lrs.property_parcel_assoc (
  LIKE meta.event INCLUDING ALL,
  property_id INT NOT NULL,
  parcel_id varchar(20) NOT NULL,
  partial BOOLEAN NOT NULL,

  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id),
  FOREIGN KEY (parcel_id) REFERENCES nsw_lrs.parcel(parcel_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS nsw_lrs_property_parcel_unique_parcel_id_when_not_partial
  ON nsw_lrs.property_parcel_assoc(parcel_id, effective_date)
  WHERE partial = FALSE;

CREATE UNIQUE INDEX IF NOT EXISTS nsw_lrs_property_parcel_unique_parcel_id_when_partial
  ON nsw_lrs.property_parcel_assoc(property_id, parcel_id, effective_date)
  WHERE partial = TRUE;

--
-- This is the remains after processing the property description.
--
CREATE TABLE IF NOT EXISTS nsw_lrs.legal_description_remains (
  legal_description_id UUID NOT NULL,
  legal_description_remains TEXT NOT NULL

  -- FOREIGN KEY (legal_description_id)
  --     REFERENCES nsw_lrs.legal_description(legal_description_id)
);

--
-- ## Utiltity Function for ingestion
--

CREATE FUNCTION nsw_lrs.get_base_parcel_id(input_text TEXT) RETURNS TEXT AS $$
DECLARE
  plan_part TEXT;
BEGIN
  input_text := REGEXP_REPLACE(input_text, '//+', '/');
  plan_part := REGEXP_REPLACE(input_text, '.*?([^/]+)$', '\1');

  IF plan_part ~ '^SP\d+$' THEN
    RETURN plan_part;
  END IF;

  plan_part := REGEXP_REPLACE(plan_part, '^DP', '');
  RETURN REGEXP_REPLACE(input_text, '[^/]+$', plan_part);
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION nsw_lrs.get_base_parcel_kind(input_text TEXT) RETURNS nsw_lrs.base_parcel_kind AS $$
DECLARE
  plan_part TEXT := substring(REGEXP_REPLACE(input_text, '/+', '/', 'g') FROM '([^/]+)$');
BEGIN
  RETURN CASE
    WHEN plan_part ~ '^SP\d+$' THEN 'strata_plan'
    ELSE 'deposit_lot'
  END;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;


