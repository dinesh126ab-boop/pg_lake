CREATE OR REPLACE FUNCTION lake_table.get_ftoption(options text[], key text)
RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT split_part(opt, '=', 2)
  FROM unnest(options) opt
  WHERE split_part(opt, '=', 1) = key
$$;


CREATE OR REPLACE FUNCTION lake_table.get_table_schema(p_table regclass)
RETURNS text AS $$
BEGIN
  RETURN (
    SELECT n.nspname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_table
  );
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION lake_table.get_table_name(p_table regclass)
RETURNS text AS $$
BEGIN
  RETURN (
    SELECT c.relname
    FROM pg_class c
    WHERE c.oid = p_table
  );
END;
$$ LANGUAGE plpgsql;
