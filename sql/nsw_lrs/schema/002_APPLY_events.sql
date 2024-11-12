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


CREATE INDEX idx_property_id_legal_description
    ON nsw_lrs.legal_description(property_id);
CREATE INDEX idx_effective_date_legal_description
    ON nsw_lrs.legal_description(effective_date DESC);


CREATE TABLE IF NOT EXISTS nsw_lrs.legal_description_by_strata_lot (
  legal_description_by_strata_id BIGSERIAL PRIMARY KEY,
  legal_description TEXT NOT NULL,
  property_id INT NOT NULL,
  property_strata_lot INT,

  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id),
  UNIQUE (property_id, effective_date, property_strata_lot)
) INHERITS (meta.event);


CREATE INDEX idx_property_id_legal_description_by_strata_lot
    ON nsw_lrs.legal_description_by_strata_lot(property_id);
CREATE INDEX idx_effective_date_legal_description_by_strata_lot
    ON nsw_lrs.legal_description_by_strata_lot(effective_date DESC);


--
-- ## Property Area
--
-- I think it's possible this could change over time due
-- to how cadastra stuff works.
--

CREATE TABLE IF NOT EXISTS nsw_lrs.property_area (
  property_id INT NOT NULL,
  sqm_area FLOAT NOT NULL,

  UNIQUE (property_id, effective_date),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
) inherits (meta.event);

CREATE INDEX idx_property_id_property_area
    ON nsw_lrs.property_area(property_id);

CREATE INDEX idx_effective_date_property_area
    ON nsw_lrs.property_area(effective_date DESC);

CREATE TABLE IF NOT EXISTS nsw_lrs.property_area_by_strata_lot (
  property_id INT NOT NULL,
  property_strata_lot INT NOT NULL,
  sqm_area FLOAT NOT NULL,

  UNIQUE (property_id, property_strata_lot, effective_date),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
) inherits (meta.event);

CREATE INDEX idx_property_id_property_area_by_strata_lot
    ON nsw_lrs.property_area_by_strata_lot(property_id);

CREATE INDEX idx_effective_date_property_area_by_strata_lot
    ON nsw_lrs.property_area_by_strata_lot(effective_date DESC);

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

--
-- ## Notice of Sale
--






