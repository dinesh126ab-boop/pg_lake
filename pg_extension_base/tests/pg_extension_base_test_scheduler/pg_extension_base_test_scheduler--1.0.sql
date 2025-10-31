CREATE SCHEMA extension_base_scheduler;

CREATE FUNCTION extension_base_scheduler.main_worker(internal)
 RETURNS internal
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_test_scheduler_main_worker$function$;
COMMENT ON FUNCTION extension_base_scheduler.main_worker(internal)
    IS 'main entry point for pg_extension_base_scheduler';

SELECT extension_base.register_worker('pg_extension_base_test_scheduler_main_worker', 'extension_base_scheduler.main_worker');
