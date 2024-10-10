--
-- # Observations and Events
--
-- ## legal_description
--
-- We don't have access to the on going history of the property
-- description of these properties, observed property descriptions
-- at different points in time, so it makes sense to treat this
-- as event.
--
-- Representing this as an event allows capture of this information over time.

CREATE TABLE IF NOT EXISTS nsw_lrs.legal_description (
  legal_description_id BIGSERIAL PRIMARY KEY,
  legal_description TEXT NOT NULL,
  property_id INT NOT NULL,

  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id),

  UNIQUE (property_id, effective_date)
) INHERITS (meta.event);

--
-- ## property parcel association
--
-- A property can be made up of multiple parcels as well
-- as partial parcels shared between different properties.
--
CREATE TABLE IF NOT EXISTS nsw_lrs.property_parcel_assoc (
  property_id INT NOT NULL,
  parcel_id INT NOT NULL,
  partial BOOLEAN NOT NULL,

  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id),
  FOREIGN KEY (parcel_id) REFERENCES nsw_lrs.parcel(parcel_id)
) INHERITS (meta.event);

CREATE UNIQUE INDEX IF NOT EXISTS nsw_lrs_property_parcel_unique_parcel_id_when_not_partial
  ON nsw_lrs.property_parcel_assoc(parcel_id, effective_date)
  WHERE partial = FALSE;

CREATE UNIQUE INDEX IF NOT EXISTS nsw_lrs_property_parcel_unique_parcel_id_when_not_partial
  ON nsw_lrs.property_parcel_assoc(property_id, parcel_id, effective_date)
  WHERE partial = TRUE;

--
-- ## Described Dimensions
--
-- These are descriptions of dimensions, mostly found
-- in older property sales data (as of writing this).
--

CREATE TABLE IF NOT EXISTS nsw_lrs.described_dimensions (
  property_id INT NOT NULL,
  dimension_description TEXT NOT NULL,
  UNIQUE (property_id, dimension_description, effective_date),

  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
) inherits (meta.event);

