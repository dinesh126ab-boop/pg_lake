CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_AsWKB(geometry)
 RETURNS bytea
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Xmin(geometry)
 RETURNS float8
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Xmax(geometry)
 RETURNS float8
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Ymin(geometry)
 RETURNS float8
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Ymax(geometry)
 RETURNS float8
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Zmin(geometry)
 RETURNS float8
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Zmax(geometry)
 RETURNS float8
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Transform(geometry, text, text, bool)
 RETURNS geometry
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Intersects_Extent(geometry, geometry)
 RETURNS bool
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Area_Spheroid(geometry)
 RETURNS double precision
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

/* this function returns enum, but if sent over the wire we reinterpret it as text to match geometrytype() */
CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_GeometryType(geometry)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_DWithin_Spheroid(geometry,geometry,double precision)
 RETURNS boolean
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Distance_Spheroid(geometry,geometry)
 RETURNS double precision
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Length_Spheroid(geometry)
 RETURNS double precision
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Perimeter_Spheroid(geometry)
 RETURNS real
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;

-- Custom aggregates to be pushed down to DuckDB

-- Postgis lacks a ST_Envelope aggregate function

-- CREATE AGGREGATE __lake__internal__nsp__.ST_Envelope_Agg(geometry) (
--     SFUNC = ST_Envelope,
--     STYPE = geometry
-- );

CREATE AGGREGATE __lake__internal__nsp__.ST_Intersection_Agg(geometry) (
    SFUNC = ST_Union,           -- we do not call this function, we only need the aggregate defined succesfully, so since we are lacking a signature for ST_intersection() we are just using this instead.
    STYPE = geometry
);

CREATE AGGREGATE __lake__internal__nsp__.ST_Union_Agg(geometry) (
    SFUNC = ST_Union,
    STYPE = geometry
);

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Distance_Sphere(geometry,geometry)
 RETURNS double precision
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;
