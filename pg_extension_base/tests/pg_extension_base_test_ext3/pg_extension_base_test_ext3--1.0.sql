CREATE SCHEMA extension_base_test_ext3;

/* call C initialization */
CREATE FUNCTION extension_base_test_ext3.initialize_extension()
 RETURNS void
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_test_ext3_initialize_extension$function$;
SELECT extension_base_test_ext3.initialize_extension();
DROP FUNCTION extension_base_test_ext3.initialize_extension();
