/* pg_lake_table--3.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_lake_table" to load this file. \quit

CREATE FUNCTION pg_lake_table_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION pg_lake_table_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pg_lake_table
  HANDLER pg_lake_table_handler
  VALIDATOR pg_lake_table_validator;

-- we create 1 server that is pre-defined
-- we also support 3 different "format" options
-- at create foreign table time users can specify
-- the format as on of the: parquet, json and csv
-- also, we automatically use connection string information
-- from cdw_common.engine_host variable
CREATE SERVER pg_lake
  FOREIGN DATA WRAPPER pg_lake_table;

GRANT USAGE ON FOREIGN SERVER pg_lake TO lake_read;

/*
 * In terms of API, we prefer to distinguish writable 
 * iceberg tables from the other analytics tables. Hence,
 * we add a new SERVER, which uses the same handler but
 * has its own validator. 
 * 
 * Using the same handler provides the same fdw callbacks
 * and a different validator provides applying different
 * checks on the fdw options.
*/
CREATE FUNCTION pg_lake_iceberg_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pg_lake_iceberg
  HANDLER pg_lake_table_handler
  VALIDATOR pg_lake_iceberg_validator;

CREATE SERVER pg_lake_iceberg
  FOREIGN DATA WRAPPER pg_lake_iceberg;

GRANT USAGE ON FOREIGN SERVER pg_lake_iceberg TO lake_read_write;

CREATE SCHEMA lake_file_cache;
GRANT USAGE ON SCHEMA lake_file_cache TO lake_read;

/* lake_file_cache.add adds a file to the local cache */
CREATE FUNCTION lake_file_cache.add(path text, refresh bool default false)
 RETURNS bigint
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_add_file_to_cache$function$;

COMMENT ON FUNCTION lake_file_cache.add(text,bool) IS 'add a file to the local cache';
REVOKE ALL ON FUNCTION lake_file_cache.add(text,bool) FROM public;
GRANT EXECUTE ON FUNCTION lake_file_cache.add(text,bool) TO lake_read;

/* lake_file_cache.remove removes a file from the local cache */
CREATE FUNCTION lake_file_cache.remove(path text)
 RETURNS bool
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_remove_file_from_cache$function$;

COMMENT ON FUNCTION lake_file_cache.remove(text) IS 'remove a file from the local cache';
REVOKE ALL ON FUNCTION lake_file_cache.remove(text) FROM public;
GRANT EXECUTE ON FUNCTION lake_file_cache.remove(text) TO lake_read;

/* lake_file_cache.list lists all the files in the cache */
CREATE FUNCTION lake_file_cache.list(OUT path text, OUT file_size bigint, OUT last_access_time timestamp)
 RETURNS SETOF record
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_list_files_in_cache$function$;

COMMENT ON FUNCTION lake_file_cache.list() IS 'list the files in the local cache';
REVOKE ALL ON FUNCTION lake_file_cache.list() FROM public;
GRANT EXECUTE ON FUNCTION lake_file_cache.list() TO lake_read;

CREATE SCHEMA lake_table;
CREATE TABLE lake_table.files (
    -- unique ID for the file
    id bigserial UNIQUE NOT NULL,

    -- OID/name of the table
    table_name regclass not null,

    -- URL of the file
    path text not null,

    -- Number of rows in the file
    row_count bigint not null default -1,

    -- time at which the file was added/removed
    updated_time timestamptz not null default now(),

    -- size of the file in bytes
    file_size bigint not null CHECK (file_size >= 0),

    -- file content, 0 (data) or 1 (delete)
    content int not null default 0,

    -- number of deleted rows in the file
    deleted_row_count bigint not null default 0,

    -- When a file is first added to a table with row_ids, the row IDs will only
    -- be stored in row_id_mappings and not in the file itself. In that case, we
    -- set first_row_id to the lower bound of the range to make it explicit.
    --
    -- See also: Row lineage in Iceberg v3
    first_row_id bigint,

    -- currently we enforce that a file can be in a table at most once
    PRIMARY KEY (table_name, path)
);

GRANT SELECT ON lake_table.files TO lake_read;
GRANT USAGE ON SEQUENCE lake_table.files_id_seq TO lake_write;

/* this schema contains user-facing utiltiy functions */
CREATE SCHEMA lake_file;
GRANT USAGE ON SCHEMA lake_file TO lake_read;

CREATE OR REPLACE FUNCTION lake_file.list(url_wildcard text,
										OUT path text,
										OUT file_size bigint,
										OUT last_modified_time timestamptz,
										OUT etag text)
RETURNS SETOF record
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_list_files$function$;

CREATE TABLE lake_table.deletion_file_map (
	/* ID of the table of which deletion file is part */
	table_name regclass not null,

	/* path of the deletion file */
	path text not null,

	/* path of the data file from which deletion file deletes */
	deleted_from text not null,

	PRIMARY KEY (table_name, deleted_from, path),

	/* if the deletion file is removed, also delete the mapping */
	CONSTRAINT path_fk
  FOREIGN KEY (table_name, deleted_from)
  REFERENCES lake_table.files(table_name, path)
  ON UPDATE CASCADE ON DELETE CASCADE,

	/* if the data file is removed, also delete the mapping */
	CONSTRAINT deleted_from_fk 
	FOREIGN KEY (table_name, path)
	REFERENCES lake_table.files (table_name, path)
	ON DELETE CASCADE
);

GRANT SELECT ON lake_table.deletion_file_map TO lake_read;

CREATE OR REPLACE FUNCTION lake_file.size(path text)
 RETURNS bigint
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_file_size$function$;

CREATE OR REPLACE FUNCTION lake_file.exists(path text)
 RETURNS boolean
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_file_exists$function$;

/* file_preview() returns a structured description of the source file */
CREATE OR REPLACE FUNCTION lake_file.preview(url text, format text DEFAULT NULL, compression text DEFAULT NULL, out column_name text, out column_type text)
 RETURNS SETOF record
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE
AS 'MODULE_PATHNAME', $function$pg_lake_file_preview$function$;

/* 
 * a placeholder access method to allow users use the below syntax:
 * - `CREATE TABLE USING pg_lake_iceberg AS SELECT`
 * - `CREATE TABLE USING pg_lake_iceberg`
 *
 * We use the access method to translate `CREATE TABLE USING` statements to
 * `CREATE FOREIGN TABLE SERVER` statements for ease of use and more familiar
 * syntax for the users.
 */
CREATE OR REPLACE FUNCTION lake_iceberg.pg_lake_iceberg_am_handler(internal)
    RETURNS table_am_handler
    LANGUAGE C
AS 'MODULE_PATHNAME', $function$pg_lake_iceberg_am_handler$function$;
CREATE ACCESS METHOD pg_lake_iceberg TYPE TABLE HANDLER lake_iceberg.pg_lake_iceberg_am_handler;
COMMENT ON ACCESS METHOD pg_lake_iceberg IS 'pg_lake_iceberg table access method, a placeholder for internal usage';

CREATE ACCESS METHOD iceberg TYPE TABLE HANDLER lake_iceberg.pg_lake_iceberg_am_handler;
COMMENT ON ACCESS METHOD iceberg IS 'iceberg table access method, alias for pg_lake_iceberg table access method';

CREATE FUNCTION lake_file.delete(url text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_delete_file$function$;

CREATE FUNCTION lake_iceberg.table_size(table_name regclass)
 RETURNS bigint
 LANGUAGE C
 PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_table_size$function$;
COMMENT ON FUNCTION lake_iceberg.table_size(regclass) IS 'total size of the current data files of the table';

CREATE PROCEDURE lake_table.finish_postgres_recovery()
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$pg_lake_finish_postgres_recovery$function$;
REVOKE ALL ON PROCEDURE lake_table.finish_postgres_recovery() FROM public;

CREATE PROCEDURE lake_table.finish_postgres_recovery_in_db()
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$pg_lake_finish_postgres_recovery_in_db$function$;
REVOKE ALL ON PROCEDURE lake_table.finish_postgres_recovery_in_db() FROM public;

CREATE TABLE lake_table.field_id_mappings (

	/* each (sub) field is stored */
	table_name regclass not null,
	field_id int not null,

	/* attribute number, both parent and children points the same value */
	pg_attnum smallint NOT NULL,

	/* NULL for top-level fields */
	parent_field_id int,

	/* only filled for top-level fields */
	initial_default text,
	write_default text,

	field_pg_type regtype not null,

	field_pg_typemod int not null,

	/* each (sub)-field id is unique */
	CONSTRAINT unique_attribute UNIQUE (table_name, field_id),

	/* if the table is dropped, remove all field id mappings */
	CONSTRAINT table_name_fk FOREIGN KEY (table_name)
	REFERENCES lake_iceberg.tables_internal (table_name)
	ON DELETE CASCADE,

	/* make sure we never have orphaned field_ids */
	CONSTRAINT enforce_field_hierarchy FOREIGN KEY (table_name, parent_field_id)
	REFERENCES lake_table.field_id_mappings (table_name, field_id)
);


CREATE TABLE lake_table.data_file_column_stats
 (
 	-- table name of the file
 	table_name regclass not null,

 	-- path of the file
 	path text not null,

 	-- field id of the column
 	field_id bigint not null,

 	-- text representation of lower bound value for the field type
 	-- we rely on parquet_metadata and parquet_schema to fill it
 	lower_bound text,

 	-- text representation of upper bound value for the field type
 	-- we rely on parquet_metadata and parquet_schema to fill it
 	upper_bound text,

 	-- removes table column stats if the table's data file is removed
 	foreign key (table_name, path) references lake_table.files (table_name, path)
 		on delete cascade,

	primary key (table_name, field_id, path)
 );

CREATE FUNCTION lake_iceberg.vacuum(internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'pg_lake_iceberg_vacuum'
LANGUAGE C STRICT;

SELECT extension_base.register_worker('iceberg vacuum worker', 'lake_iceberg.vacuum');

-- This table contains rowid mappings to file/linenumber ranges in a compressed
-- fashion; this allows us to assign/store rowids a single time and have them
-- persist while we compact data files to avoid having to make additional
-- changes to the replay tables.

CREATE TABLE lake_table.row_id_mappings (
    -- table to which the row ID range belongs
    table_name regclass not null,

    -- start of a range of row IDs
    row_id_range int8range not null,

    -- file ID and offset at which the row ID range resides; the first row in
    -- this range starts at this offset.  For the chunk at the start of the
    -- file, this will be 0 and there should be no overlaps in a given file.
    -- (As well, the sum of rows in the given file should match the size of the
    -- ranges here, but this is harder to express as a database invariant.)
    file_id bigint not null,
    file_row_number bigint not null,

    -- removes row ID mappings for files that are removed.
    foreign key (file_id) references lake_table.files (id)
        on delete cascade,

    -- ensure that row_id_range are always '[)' in this table
    CHECK (lower_inc(row_id_range) AND NOT upper_inc(row_id_range))
);
GRANT SELECT ON lake_table.row_id_mappings TO public;

ALTER TABLE lake_table.row_id_mappings
  ADD CONSTRAINT row_id_mapping_no_overlap
  EXCLUDE USING gist (
    table_name WITH =,
    row_id_range WITH &&
  ),
  REPLICA IDENTITY FULL
;

-- Add an extra index on file_id, mainly used for cascade deletions
CREATE INDEX ON lake_table.row_id_mappings (file_id);

-- Schema for Iceberg write-related objects
CREATE SCHEMA __pg_lake_table_writes;
GRANT USAGE ON SCHEMA __pg_lake_table_writes TO public;

CREATE VIEW __pg_lake_table_writes.row_id_mappings AS SELECT * FROM lake_table.row_id_mappings;
GRANT SELECT ON __pg_lake_table_writes.row_id_mappings TO public;

-- represent partitioning for Iceberg tables
CREATE TABLE lake_table.partition_specs
(
    -- the partition spec belongs to table
    table_name regclass,

    spec_id INT,

    /* each spec_id is unique for a given table */
    CONSTRAINT unique_partition_spec_id UNIQUE (table_name, spec_id),

    /* if the table is dropped, remove all partition specs */
    CONSTRAINT table_name_fk FOREIGN KEY (table_name)
    REFERENCES lake_iceberg.tables_internal (table_name)
    ON DELETE CASCADE
);


CREATE TABLE lake_table.partition_fields
(
    -- the partition spec belongs to table
    table_name regclass,
    spec_id INT,

    source_field_id INT,

    -- Iceberg creates a pseduo field per partition transform
    -- and it is called partition_field in the spec
    partition_field_id INT,

    -- the auto generated name for the partition field
    -- e.g., created_at_day or user_id_bucket_1000
    partition_field_name TEXT,

    -- transform's name
    -- e.g., month or bucket[10]
    transform_name TEXT,

    -- partition_field_id is unique for the table
    CONSTRAINT unique_partition_field_id UNIQUE (table_name, partition_field_id),

    -- make sure we never have non-existing field ids
    CONSTRAINT enforce_partition_field_existence FOREIGN KEY (table_name, source_field_id)
    REFERENCES lake_table.field_id_mappings (table_name, field_id),

    -- never have partition field for a non-existing spec
    CONSTRAINT spec_id_fkey FOREIGN KEY (table_name, spec_id)
    REFERENCES lake_table.partition_specs (table_name, spec_id),

    -- if the table is dropped, remove all partition fields
    CONSTRAINT table_name_fk FOREIGN KEY (table_name)
    REFERENCES lake_iceberg.tables_internal (table_name)
    ON DELETE CASCADE
);

CREATE TABLE lake_table.data_file_partition_values
(
       table_name regclass NOT NULL,
       id bigint NOT NULL, -- refers to data_file.id
       partition_field_id INT NOT NULL,

       /* postgres text representation of data_file->partition->value in manifest */
       value text,

       -- same partition field cannot be used twice for a given table
       CONSTRAINT unique_partition_field_value UNIQUE (table_name, id, partition_field_id),

       -- if the data file is dropped, remove all partition fields
       CONSTRAINT data_file_id_p_fk FOREIGN KEY (id)
       REFERENCES lake_table.files (id)
       ON DELETE CASCADE,

       -- if the table is dropped, remove all partition fields
       CONSTRAINT table_name_fk FOREIGN KEY (table_name)
       REFERENCES lake_iceberg.tables_internal (table_name)
       ON DELETE CASCADE,

       -- prevent bogus partition_field_id
       CONSTRAINT partition_field_fk FOREIGN KEY (table_name, partition_field_id)
       REFERENCES lake_table.partition_fields (table_name, partition_field_id)
);

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.is_object_created_by_lake(regclass,oid)
RETURNS bool
LANGUAGE C
IMMUTABLE PARALLEL SAFE
AS 'MODULE_PATHNAME', $$is_object_created_by_lake$$;
GRANT EXECUTE ON FUNCTION __lake__internal__nsp__.is_object_created_by_lake(regclass,oid) TO public;
