--
-- # Ingest PSI
--
-- ## Property Sales
--

SET session_replication_role = 'replica';

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

SET session_replication_role = 'origin';
SELECT meta.check_constraints('nsw_lrs', 'notice_of_sale');

