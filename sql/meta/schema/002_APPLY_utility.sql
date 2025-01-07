CREATE OR REPLACE FUNCTION meta.check_constraints(schema_name TEXT, table_name TEXT)
RETURNS VOID AS $$
DECLARE
  constraint_name TEXT;
  constraint_type CHAR;
  constraint_def  TEXT;
  full_table_name TEXT := format('%I.%I', schema_name, table_name);
BEGIN
  FOR constraint_name, constraint_type, constraint_def IN
    SELECT conname, contype, pg_get_constraintdef(oid)
    FROM pg_constraint
    WHERE conrelid = format('%s.%s', schema_name, table_name)::regclass
  LOOP
    IF constraint_type = 'c' THEN
      EXECUTE format('SELECT * FROM %s WHERE NOT (%s)',
                     full_table_name,
                     substring(constraint_def FROM 'CHECK \((.+)\)$'))
      INTO constraint_def;

      IF FOUND THEN
        RAISE EXCEPTION 'Violation found in CHECK constraint: %', constraint_name;
      END IF;
    END IF;

    -- Handle other constraints (e.g., UNIQUE, FOREIGN KEY) here
  END LOOP;
END;
$$ LANGUAGE plpgsql;
