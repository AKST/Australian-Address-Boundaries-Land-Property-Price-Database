CREATE SCHEMA IF NOT EXISTS nsw_environment;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'zoning_standard' AND typnamespace = 'nsw_environment'::regnamespace) THEN
    CREATE TYPE nsw_environment.zoning_standard AS ENUM (
      'legacy_vg_2011',
      'ep&a_2006',
      'unknown'
    );
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS nsw_environment.zone (
  zone_id SERIAL PRIMARY KEY,
  zone_code varchar(3) NOT NULL,
  zone_std nsw_environment.zoning_standard NOT NULL,
  UNIQUE (zone_code, zone_std)
);
