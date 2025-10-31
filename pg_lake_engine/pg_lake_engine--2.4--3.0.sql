-- move all objects to new internal schema
ALTER SCHEMA __crunchy__internal__nsp__ RENAME TO __lake__internal__nsp__;

-- rename functions
ALTER FUNCTION __lake__internal__nsp__.__crunchy_read_table RENAME TO __lake_read_table;
ALTER FUNCTION __lake__internal__nsp__.crunchy_nth_suffix RENAME TO lake_nth_suffix;
ALTER FUNCTION __lake__internal__nsp__.__crunchy_now() RENAME TO __lake_now;
ALTER FUNCTION __lake__internal__nsp__.crunchy_regexp_matches(text,text,bool) RENAME TO lake_regexp_matches;

-- rename schemas
ALTER SCHEMA crunchy_query_engine RENAME TO lake_engine;
ALTER SCHEMA crunchy_struct RENAME TO lake_struct;

-- recreate all C udfs to replace their library name and internal function name
CREATE OR REPLACE FUNCTION __lake__internal__nsp__.time_bucket(interval, timestamp, timestamp)
 RETURNS timestamp
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.time_bucket(interval, timestamptz, timestamptz)
 RETURNS timestamptz
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION pg_catalog.to_date(double precision)
 RETURNS date
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_to_date$function$;
COMMENT ON FUNCTION to_date(double precision)
IS 'convert days since UNIX epoch to date';

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.substring_pg(text, int)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.substring_pg(text, int, int)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.nullify_any_type(record)
 RETURNS record
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.__lake_now()
 RETURNS timestamptz
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.map_extract("any","any")
 RETURNS "any"
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION lake_engine.flush_deletion_queue(table_name regclass)
 RETURNS SETOF text
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $function$flush_deletion_queue$function$;

CREATE OR REPLACE FUNCTION lake_engine.flush_in_progress_queue()
 RETURNS SETOF text
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $function$flush_in_progress_queue$function$;

CREATE OR REPLACE FUNCTION pg_catalog.to_postgres(val "any")
 RETURNS "any"
 LANGUAGE c
 STRICT STABLE PARALLEL SAFE
AS 'MODULE_PATHNAME', $function$pg_lake_to_postgres$function$;
COMMENT ON FUNCTION pg_catalog.to_postgres("any") IS 'force pulling data into postgres';

/*
 * We need to push down `regexp_like` and rename to `regexp_matches`;
 * unfortunately postgres also has `regexp_matches` but it has the wrong
 * signature, so we need stubs here to lookup as our replacement.
 */

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.lake_regexp_matches(text,text,bool)
 RETURNS bool
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.xor(int2,int2)
 RETURNS int2
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.xor(int4,int4)
 RETURNS int4
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.xor(int8,int8)
 RETURNS int8
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

/*
 * DuckDB name for div
 */
CREATE OR REPLACE FUNCTION __lake__internal__nsp__.fdiv(numeric, numeric)
 RETURNS numeric
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

/*
 * DuckDB name for mod
 */
CREATE OR REPLACE FUNCTION __lake__internal__nsp__.fmod(numeric, numeric)
 RETURNS numeric
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

---------------------------

/*
 * DuckDB integer division
 */
CREATE OR REPLACE FUNCTION __lake__internal__nsp__.divide(int2, int2)
 RETURNS int2
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.divide(int2, int4)
 RETURNS int2
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.divide(int2, int8)
 RETURNS int2
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.divide(int4, int2)
 RETURNS int4
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.divide(int4, int4)
 RETURNS int4
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.divide(int4, int8)
 RETURNS int4
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.divide(int8, int2)
 RETURNS int8
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.divide(int8, int4)
 RETURNS int8
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.divide(int8, int8)
 RETURNS int8
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__."trim"(text)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__."trim"(text, text)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.strftime(timestamp, text)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.strftime(timestamptz, text)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.printf(text, variadic "any")
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.lake_nth_suffix(int)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.json_array_length(jsonb)
 RETURNS int
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

-- create wrapper jsonb() function for all the types
-- that can be casted to jsonb. This function corresponds
-- to the macro with the same name in pgduck_server/Duckdb
CREATE OR REPLACE FUNCTION __lake__internal__nsp__.jsonb(jsonb)
 RETURNS jsonb
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.jsonb(text)
 RETURNS jsonb
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.jsonb(json)
 RETURNS jsonb
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.jsonb(cstring)
 RETURNS jsonb
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.jsonb(varchar)
 RETURNS jsonb
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.jsonb(bpchar)
 RETURNS jsonb
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

-- skeletons for the substring function to be used in the deparser
CREATE OR REPLACE FUNCTION __lake__internal__nsp__.generate_series_int(int, int)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.generate_series_int_step(int, int, int)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;
