CREATE SCHEMA extension_base_test_ext1;

/* call C initialization */
CREATE FUNCTION extension_base_test_ext1.initialize_extension()
 RETURNS void
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_test_ext1_initialize_extension$function$;
SELECT extension_base_test_ext1.initialize_extension();
DROP FUNCTION extension_base_test_ext1.initialize_extension();
