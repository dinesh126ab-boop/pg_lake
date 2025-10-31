-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION map" to load this file. \quit

CREATE SCHEMA map_type;
GRANT USAGE ON SCHEMA map_type TO public;

CREATE OR REPLACE FUNCTION
    map_type.create(keytype regtype, valtype regtype, typname text DEFAULT NULL)
    RETURNS regtype
    AS 'MODULE_PATHNAME', 'map_create'
    LANGUAGE 'c';

REVOKE EXECUTE ON FUNCTION map_type.create(regtype, regtype, text)
FROM public;
