CREATE SCHEMA extension_updater;

CREATE FUNCTION extension_updater.main(internal)
 RETURNS internal
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_updater_main$function$;
COMMENT ON FUNCTION extension_updater.main(internal)
    IS 'main entry point for pg_extension_updater';

SELECT extension_base.register_worker('pg_extension_updater_main', 'extension_updater.main');
