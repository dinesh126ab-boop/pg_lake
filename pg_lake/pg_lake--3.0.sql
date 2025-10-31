-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_lake" to load this file. \quit

CREATE SCHEMA lake;
GRANT USAGE ON SCHEMA lake TO public;

-- lake.version()
CREATE FUNCTION lake.version()
	RETURNS TEXT
	LANGUAGE C
	IMMUTABLE PARALLEL SAFE STRICT
	AS 'MODULE_PATHNAME', 'pg_lake_version';
COMMENT ON FUNCTION lake.version()
IS 'pg_lake installed version';

GRANT EXECUTE ON FUNCTION lake.version() TO public;
