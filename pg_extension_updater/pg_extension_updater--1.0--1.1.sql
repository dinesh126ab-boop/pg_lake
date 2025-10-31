ALTER SCHEMA crunchy_extension_updater RENAME TO extension_updater;

CREATE OR REPLACE FUNCTION extension_updater.main(internal)
 RETURNS internal
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_updater_main$function$;
COMMENT ON FUNCTION extension_updater.main(internal)
    IS 'main entry point for extension_updater';


-- register back with the correct name
UPDATE pg_extension_base.workers SET entry_point_schema = 'extension_updater', worker_name = 'pg_extension_updater_main' WHERE worker_name = 'crunchy_extension_updater_main';