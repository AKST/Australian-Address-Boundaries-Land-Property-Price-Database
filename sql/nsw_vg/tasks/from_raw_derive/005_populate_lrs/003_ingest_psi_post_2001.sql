--
-- # Ingest PSI
--
-- ## Property Sales
--

WITH
  --
  -- Group sale participants by their sale counter (unique to a file)
  -- and file_source_id, and place them in an array containing the different
  -- participant kinds.
  --
  sale_participant_groupings AS (
    SELECT file_source_id, sale_counter, ARRAY_AGG(d.participant) as participants
      FROM nsw_vg_raw.ps_row_d d
      LEFT JOIN nsw_vg_raw.ps_row_d_source USING (ps_row_d_id)
      LEFT JOIN meta.source_file USING (source_id)
      WHERE property_id IS NOT NULL
        AND sale_counter IS NOT NULL
      GROUP BY file_source_id, sale_counter),

  --
  -- Whilst reducing the ranked data to rows with a rank of 1, it also
  -- ensures sale participants are linked as well, reducing the need
  -- for an additional join later on.
  --
  with_sale_partipants AS (
    SELECT ps_row_b_id,
           COALESCE(participants, '{}'::nsw_lrs.sale_participant[]) as participants
      FROM pg_temp.sourced_raw_property_sales_b b
      LEFT JOIN sale_participant_groupings USING (file_source_id, sale_counter)
      WHERE b.ps_row_b_id IS NOT NULL)

INSERT INTO nsw_lrs.notice_of_sale(
  source_id, effective_date, property_id, strata_lot_number,
  dealing_number, purchase_price, contract_date, settlement_date,
  interest_of_sale, sale_participants, comp_code, sale_code)
SELECT b.source_id,
       COALESCE(b.contract_date, b.settlement_date),
       b.property_id, b.strata_lot_number, b.dealing_number,
       b.purchase_price, b.contract_date, b.settlement_date,
       b.interest_of_sale, p.participants, b.comp_code, b.sale_code
  FROM pg_temp.sourced_raw_property_sales_b b
  LEFT JOIN with_sale_partipants p USING (ps_row_b_id)
  WHERE b.ps_row_b_id IS NOT NULL;

--
-- ## Ingest Property Description
--

INSERT INTO nsw_lrs.legal_description(
  source_id,
  effective_date,
  property_id,
  strata_lot_number,
  legal_description,
  legal_description_kind)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    source_id,
    effective_date,
    property_id,
    strata_lot_number,
    full_property_description,
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

--
-- ## Ingest Property Area
--

INSERT INTO nsw_lrs.property_area(
    source_id,
    effective_date,
    property_id,
    strata_lot_number,
    sqm_area)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    source_id,
    effective_date,
    property_id,
    strata_lot_number,
    pg_temp.sqm_area(area, area_type)
  FROM pg_temp.sourced_raw_property_sales_b
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
    AND NOT seen_in_land_values
    AND property_id IS NOT NULL
  ORDER BY effective_date,
           property_id,
           strata_lot_number,
           date_provided DESC;

--
-- ## Ingest Primary Purpose
--

INSERT INTO nsw_lrs.property_primary_purpose(
    source_id,
    effective_date,
    primary_purpose_id,
    property_id,
    strata_lot_number)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    source_id,
    effective_date,
    primary_purpose_id,
    property_id,
    strata_lot_number
  FROM pg_temp.sourced_raw_property_sales_b b
  LEFT JOIN nsw_lrs.primary_purpose USING (primary_purpose)
  WHERE b.primary_purpose IS NOT NULL
    AND property_id IS NOT NULL;

--
-- ## Ingest Nature of property
--

INSERT INTO nsw_lrs.nature_of_property(
    source_id,
    effective_date,
    property_id,
    nature_of_property,
    strata_lot_number)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    source_id,
    effective_date,
    property_id,
    (CASE
      WHEN nature_of_property = 'V' THEN 'Vaccant'
      WHEN nature_of_property = 'R' THEN 'Residence'

      -- do not ask me why they use 3 for this.
      WHEN nature_of_property = '3' THEN 'Other'
    END)::nsw_lrs.property_nature,
    strata_lot_number
  FROM pg_temp.sourced_raw_property_sales_b b
  WHERE nature_of_property IS NOT NULL
    AND property_id IS NOT NULL;

--
-- ## Zones
--

INSERT INTO nsw_lrs.zone_observation(
    source_id,
    effective_date,
    property_id,
    zone_code)
SELECT DISTINCT ON (effective_date, property_id)
       source_id,
       effective_date,
       property_id,
       zone_code
  FROM pg_temp.sourced_raw_property_sales_b
  WHERE zone_standard = 'ep&a_2006'
    AND NOT seen_in_land_values
    AND strata_lot_number IS NULL
  ORDER BY effective_date, property_id, date_provided DESC;

