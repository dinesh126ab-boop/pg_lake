-- recreate all C udfs to replace their library name and internal function name
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

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.ST_Distance_Sphere(geometry,geometry)
 RETURNS double precision
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_spatial_dummy_function$function$;
