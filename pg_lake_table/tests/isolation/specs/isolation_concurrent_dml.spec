setup
{
    CREATE TABLE test_iceberg_dml (key int, value text) USING iceberg;
}

teardown
{
    DROP TABLE IF EXISTS test_iceberg_dml CASCADE;
}

session "s1"


step "s1-begin"
{
    BEGIN;
}

step "s1-insert"
{
    INSERT INTO test_iceberg_dml VALUES(1);
}

step "s1-update"
{
    UPDATE test_iceberg_dml SET key = 15 WHERE key = 1;
}

step "s1-select-all"
{
   SELECT * FROM test_iceberg_dml;
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-update"
{
    UPDATE test_iceberg_dml SET key = 16 WHERE key = 15;
}


step "s2-commit"
{
    COMMIT;
}


permutation "s1-insert" "s1-begin" "s1-update" "s2-update" "s1-commit" "s1-select-all"
