-- move all objects to the new base schema
ALTER SCHEMA crunchy_base RENAME TO extension_base;

-- recreate all C udfs to replace their library name and internal function name
CREATE OR REPLACE FUNCTION extension_base.list_preload_libraries(OUT extension_name text, OUT library_name text)
 RETURNS SETOF record
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_list_preload_libraries$function$;
COMMENT ON FUNCTION extension_base.list_preload_libraries()
 IS 'list all libraries that are preloaded by pg_extension_base';

CREATE OR REPLACE FUNCTION extension_base.run_attached(command text, dbname text DEFAULT current_database(), OUT command_id int, OUT command_tag text)
 RETURNS SETOF record
 LANGUAGE c STRICT
AS 'MODULE_PATHNAME', $function$pg_extension_base_run_attached_worker$function$;

CREATE OR REPLACE FUNCTION extension_base.register_worker(worker_name text, entry_point regproc)
 RETURNS bigint
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_register_worker$function$;

CREATE OR REPLACE FUNCTION extension_base.list_base_workers(OUT database_id oid, OUT worker_id int, OUT extension_id oid, OUT pid int, OUT needs_restart bool)
 RETURNS SETOF record
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_list_base_workers$function$;

CREATE OR REPLACE FUNCTION extension_base.list_database_starters(OUT database_id oid, OUT pid int, OUT needs_restart bool)
 RETURNS SETOF record
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_list_database_starters$function$;

CREATE OR REPLACE FUNCTION extension_base.deregister_worker(worker_name text)
 RETURNS void
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_deregister_worker$function$;
COMMENT ON FUNCTION extension_base.deregister_worker(text)
 IS 'deregister a base worker';
