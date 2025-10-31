CREATE SCHEMA extension_base_test_ext2;

/* call C initialization */
CREATE FUNCTION extension_base_test_ext2.initialize_extension()
 RETURNS void
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_test_ext2_initialize_extension$function$;
SELECT extension_base_test_ext2.initialize_extension();
DROP FUNCTION extension_base_test_ext2.initialize_extension();
