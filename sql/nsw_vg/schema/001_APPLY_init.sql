CREATE SCHEMA IF NOT EXISTS nsw_vg;
CREATE SCHEMA IF NOT EXISTS nsw_vg_stage;


DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'zoning_standard' AND typnamespace = 'nsw_vg'::regnamespace) THEN
    CREATE TYPE nsw_vg.zoning_standard AS ENUM ('legacy', '2011.08');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'sale_participant' AND typnamespace = 'nsw_vg'::regnamespace) THEN
    CREATE TYPE nsw_vg.sale_participant AS ENUM ('V', 'P');
  END IF;
END
$$;
