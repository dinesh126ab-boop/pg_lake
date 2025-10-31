-- move all objects to the new iceberg schema
ALTER SCHEMA crunchy_iceberg RENAME TO lake_iceberg;

-- recreate all C udfs to replace their library name and internal function name
CREATE OR REPLACE FUNCTION lake_iceberg.metadata(metadata_uri TEXT)
	RETURNS JSONB
	LANGUAGE C
	IMMUTABLE PARALLEL SAFE STRICT
	AS 'MODULE_PATHNAME', 'iceberg_metadata';

CREATE OR REPLACE FUNCTION lake_iceberg.files(metadata_uri TEXT)
	RETURNS TABLE(
		manifest_path TEXT,
		content TEXT,
		file_path TEXT,
		file_format TEXT,
		spec_id BIGINT,
		record_count BIGINT,
		file_size_in_bytes BIGINT
	)
	LANGUAGE C
	IMMUTABLE PARALLEL SAFE STRICT
	AS 'MODULE_PATHNAME', 'iceberg_files';

CREATE OR REPLACE FUNCTION lake_iceberg.snapshots(metadata_uri TEXT)
	RETURNS TABLE(
		sequence_number BIGINT,
		snapshot_id BIGINT,
		timestamp_ms TIMESTAMP,
		manifest_list TEXT
	)
	LANGUAGE C
	IMMUTABLE PARALLEL SAFE STRICT
	AS 'MODULE_PATHNAME', 'iceberg_snapshots';

CREATE OR REPLACE FUNCTION lake_iceberg.external_catalog_modification() RETURNS trigger
    AS 'MODULE_PATHNAME'
    LANGUAGE C;

CREATE OR REPLACE FUNCTION lake_iceberg.data_file_stats(metadata_location text)
RETURNS TABLE(path text, sequence_number bigint, lower_bounds json, upper_bounds json)
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$pg_lake_read_data_file_stats$$;
