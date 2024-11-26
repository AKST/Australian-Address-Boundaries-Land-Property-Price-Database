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

CREATE TYPE nsw_lrs.legal_description_kind AS ENUM ('initial', '> 2004-08-17');

CREATE TABLE IF NOT EXISTS nsw_lrs.legal_description (
  legal_description_id BIGSERIAL PRIMARY KEY,
  legal_description TEXT NOT NULL,
  legal_description_kind nsw_lrs.legal_description_kind NOT NULL,
  property_id INT NOT NULL,

  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id),
  UNIQUE (property_id, effective_date)
) INHERITS (meta.event);

CREATE TABLE IF NOT EXISTS nsw_lrs.archived_legal_description (
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
  legal_description_kind nsw_lrs.legal_description_kind NOT NULL,
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

CREATE TABLE IF NOT EXISTS nsw_lrs.property_area_by_strata_lot (
  property_id INT NOT NULL,
  property_strata_lot INT NOT NULL,
  sqm_area FLOAT NOT NULL,

  UNIQUE (property_id, property_strata_lot, effective_date),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
) inherits (meta.event);

CREATE INDEX idx_property_id_property_area
    ON nsw_lrs.property_area(property_id);

CREATE INDEX idx_effective_date_property_area
    ON nsw_lrs.property_area(effective_date DESC);

CREATE INDEX idx_property_id_property_area_by_strata_lot
    ON nsw_lrs.property_area_by_strata_lot(property_id);

CREATE INDEX idx_effective_date_property_area_by_strata_lot
    ON nsw_lrs.property_area_by_strata_lot(effective_date DESC);

--
-- ## Has Strata Plan
--

CREATE TABLE IF NOT EXISTS nsw_lrs.property_under_strata_plan(
  property_id INT NOT NULL,
  under_strata_plan BOOL NOT NULL,

  UNIQUE (property_id, effective_date),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
) inherits (meta.event);

--
-- ## Nature of Property
--

CREATE TABLE IF NOT EXISTS nsw_lrs.nature_of_property(
  property_id INT NOT NULL,
  nature_of_property nsw_lrs.property_nature NOT NULL,
  strata_lot_no INT,

  UNIQUE (property_id, effective_date, strata_lot_no),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
) inherits (meta.event);

--
-- ## Primary Purpose
--

CREATE TABLE IF NOT EXISTS nsw_lrs.property_primary_purpose(
  primary_purpose_id INT NOT NULL,
  property_id INT NOT NULL,
  strata_lot_no INT,

  UNIQUE (property_id, effective_date, strata_lot_no),
  FOREIGN KEY (primary_purpose_id) REFERENCES nsw_lrs.primary_purpose(primary_purpose_id),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
) inherits (meta.event);

--
-- ## Record of Zone
--

CREATE TABLE IF NOT EXISTS nsw_lrs.zone_observation(
  property_id INT NOT NULL,
  zone_code varchar(4) NOT NULL,
  UNIQUE (property_id, effective_date),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
) inherits (meta.event);

CREATE INDEX idx_effecitve_date_zone_observation
    ON nsw_lrs.zone_observation(effective_date DESC);

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

CREATE TABLE IF NOT EXISTS nsw_lrs.notice_of_sale (
  notice_of_sale_id BIGSERIAL PRIMARY KEY,
  property_id int NOT NULL,
  purchase_price FLOAT,
  contract_date DATE,
  strata_lot_number INT,
  dealing_number varchar(10) NOT NULL,
  settlement_date DATE,
  interest_of_sale INT,
  sale_participants nsw_lrs.sale_participant[] NOT NULL,

  -- what are these? I don't know!
  sale_code varchar(3),
  comp_code varchar(3),

  UNIQUE (dealing_number, property_id, strata_lot_number),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
) INHERITS (meta.event);

CREATE TABLE IF NOT EXISTS nsw_lrs.notice_of_sale_archived (
  notice_of_sale_archived_id BIGSERIAL PRIMARY KEY,
  property_id int NOT NULL,
  purchase_price FLOAT,
  contract_date DATE,

  -- what is this? I don't know!
  valuation_number varchar(16),
  comp_code varchar(3),

  UNIQUE (property_id, contract_date, purchase_price),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
) INHERITS (meta.event);

