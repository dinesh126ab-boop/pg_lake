setup
{
    CREATE TABLE test_iceberg_add_data_file (key int, value text) USING pg_lake_iceberg;
    ALTER FOREIGN TABLE test_iceberg_add_data_file OPTIONS (ADD autovacuum_enabled 'false');

    copy (SELECT i FROM generate_serieS(0,100)i) TO 's3://testbucketcdw/isolation/data_load_0.parquet';
    copy (SELECT i FROM generate_serieS(0,100)i) TO 's3://testbucketcdw/isolation/data_load_1.parquet';

    CREATE OR REPLACE PROCEDURE lake_iceberg.add_files_to_table(
        table_name REGCLASS,
        file_paths TEXT[])
    LANGUAGE C
    AS 'pg_lake_table', $function$add_files_to_table$function$;
}

teardown
{
    DROP PROCEDURE lake_iceberg.add_files_to_table;
    DROP TABLE IF EXISTS test_iceberg_add_data_file CASCADE;
}

session "s1"


step "s1-begin"
{
    BEGIN;
}


step "s1-insert"
{
    INSERT INTO test_iceberg_add_data_file VALUES(1);
}


step "s1-update"
{
    UPDATE test_iceberg_add_data_file SET key = 15 WHERE key = 1;
}

step "s1-delete"
{
    DELETE FROM test_iceberg_add_data_file WHERE key = 1;
}

step "s1-add-data-file-0"
{
    CALL lake_iceberg.add_files_to_table('test_iceberg_add_data_file', ARRAY['s3://testbucketcdw/isolation/data_load_0.parquet']);
}

step "s1-vacuum"
{
    VACUUM test_iceberg_add_data_file;
}

step "s1-truncate"
{
    TRUNCATE test_iceberg_add_data_file;
}

step "s1-rollback"
{
    ROLLBACK;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-add-data-file-1"
{
    CALL lake_iceberg.add_files_to_table('test_iceberg_add_data_file', ARRAY['s3://testbucketcdw/isolation/data_load_1.parquet']);
}

step "s2-rollback"
{
    ROLLBACK;
}


# two add files block each other
permutation "s1-begin" "s1-add-data-file-0" "s2-begin" "s2-add-data-file-1" "s1-rollback" "s2-rollback"

# add file is blocked by insert/update/delete
permutation "s1-insert" "s1-begin" "s1-insert" "s2-begin" "s2-add-data-file-1" "s1-rollback" "s2-rollback"
permutation "s1-insert" "s1-begin" "s1-update" "s2-begin" "s2-add-data-file-1" "s1-rollback" "s2-rollback"
permutation "s1-insert" "s1-begin" "s1-delete" "s2-begin" "s2-add-data-file-1" "s1-rollback" "s2-rollback"

permutation "s2-begin" "s2-add-data-file-1" "s1-begin" "s1-truncate" "s2-rollback" "s1-rollback"
permutation "s2-begin" "s2-add-data-file-1" "s1-vacuum" "s2-rollback"
