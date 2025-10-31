-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION map" to load this file. \quit

ALTER SCHEMA crunchy_map RENAME TO map_type;

-- recreate all C udfs to replace their library name and internal function name
CREATE OR REPLACE FUNCTION
    map_type.create(keytype regtype, valtype regtype, typname text DEFAULT NULL)
    RETURNS regtype
    AS 'MODULE_PATHNAME', 'map_create'
    LANGUAGE 'c';
