ALTER SCHEMA crunchy_data_warehouse RENAME TO lake;

-- recreate all C udfs to replace their library name and internal function name
CREATE OR REPLACE FUNCTION lake.version()
	RETURNS TEXT
	LANGUAGE C
	IMMUTABLE PARALLEL SAFE STRICT
	AS 'MODULE_PATHNAME', 'pg_lake_version';
COMMENT ON FUNCTION lake.version()
IS 'pg_lake installed version';
