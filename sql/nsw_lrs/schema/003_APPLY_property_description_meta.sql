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
  property_id INT NOT NULL,
  parcel_id varchar(20) NOT NULL,
  partial BOOLEAN NOT NULL,

  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id),
  FOREIGN KEY (parcel_id) REFERENCES nsw_lrs.parcel(parcel_id)
) INHERITS (meta.event);

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
  legal_description_id bigint NOT NULL,
  legal_description_remains TEXT NOT NULL,
);
