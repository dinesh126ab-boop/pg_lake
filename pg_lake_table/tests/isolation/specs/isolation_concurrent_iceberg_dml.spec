setup
{
    CREATE TABLE test_iceberg_dml (key int, value text) USING pg_lake_iceberg;
    ALTER FOREIGN TABLE test_iceberg_dml OPTIONS (ADD autovacuum_enabled 'false');

    CREATE TABLE test_iceberg_dml_2 (key int, value text) USING pg_lake_iceberg;
    ALTER FOREIGN TABLE test_iceberg_dml_2 OPTIONS (ADD autovacuum_enabled 'false');
}

teardown
{
    DROP TABLE IF EXISTS test_iceberg_dml, test_iceberg_dml_2 CASCADE;
}

session "s1"


step "s1-begin"
{
    BEGIN;
}

step "s1-insert-select-1"
{
    INSERT INTO test_iceberg_dml SELECT * FROM test_iceberg_dml_2;
}

step "s1-insert-select-2"
{
    INSERT INTO test_iceberg_dml_2 SELECT * FROM test_iceberg_dml;
}

step "s1-insert"
{
    INSERT INTO test_iceberg_dml VALUES(1);
}

step "s1-insert-2"
{
    INSERT INTO test_iceberg_dml_2 VALUES(1);
}

step "s1-update-1"
{
    UPDATE test_iceberg_dml SET key = 15 WHERE key = 1;
}

step "s1-delete"
{
    DELETE FROM test_iceberg_dml WHERE key = 1;
}

step "s1-select-all"
{
   SELECT * FROM test_iceberg_dml ORDER BY 1,2;
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

step "s2-insert-select-1"
{
    INSERT INTO test_iceberg_dml SELECT * FROM test_iceberg_dml_2;
}

step "s2-insert-select-2"
{
    INSERT INTO test_iceberg_dml_2 SELECT * FROM test_iceberg_dml;
}

step "s2-insert"
{
    INSERT INTO test_iceberg_dml VALUES(2);
}

step "s2-update-1"
{
    UPDATE test_iceberg_dml SET key = 16 WHERE key = 1;
}

step "s2-update-15"
{
    UPDATE test_iceberg_dml SET key = 16 WHERE key = 15;
}

step "s2-delete"
{
    DELETE FROM test_iceberg_dml WHERE key = 1;
}

step "s2-select-all"
{
   SELECT * FROM test_iceberg_dml ORDER BY 1,2;
}

step "s2-commit"
{
    COMMIT;
}

# although not very intuitive, 2 INSERTs block each other as due to iceberg catalog update
permutation "s1-begin" "s1-insert" "s2-insert" "s1-commit" "s1-select-all"

# delete and update block each other
permutation "s1-begin" "s1-delete" "s2-delete" "s1-commit" "s1-select-all"
permutation "s1-begin" "s1-update-1" "s2-delete" "s1-commit" "s1-select-all"
permutation "s1-begin" "s1-update-1" "s2-update-1" "s1-commit" "s1-select-all"

# if deletion/update doesn't touch any rows, it does not block aginst insert
# note that update/delete doesn't see INSERT yet, hence doesn't touch any rows
permutation "s1-begin" "s1-insert" "s2-delete" "s1-commit" "s1-select-all"
permutation "s1-begin" "s1-insert" "s2-update-1" "s1-commit" "s1-select-all"

# if deletion/update touches any rows, it does block against insert
permutation "s1-insert" "s1-begin" "s1-insert" "s2-delete" "s1-commit" "s1-select-all"
permutation "s1-insert" "s1-begin" "s1-insert" "s2-update-1" "s1-commit" "s1-select-all"


# selects do not block each other
permutation "s1-begin" "s1-select-all" "s2-select-all" "s1-commit"

# selects are not blocked by insert/update/delete
permutation "s1-insert" "s1-begin" "s1-select-all" "s2-delete" "s1-commit"
permutation "s1-insert" "s1-begin" "s1-select-all" "s2-update-1" "s1-commit"
permutation "s1-insert" "s1-begin" "s1-select-all" "s2-insert" "s1-commit"

# insert-selects do not lock block each other when the tables are empty
# the behavior is the same for pushdown and non-pushdown cases
permutation "s1-begin" "s1-insert-select-1" "s2-insert-select-1" "s1-commit"

# insert-selects on different tables do not block each other
permutation "s1-begin" "s1-insert-select-1" "s2-insert-select-2" "s1-commit"

# insert-selects block each other when the tables not are empty
permutation "s1-insert-2" "s1-begin" "s1-insert-select-1" "s2-insert-select-1" "s1-commit"
permutation "s1-insert-2" "s1-begin" "s1-insert-select-1" "s2-insert-select-1" "s1-commit"


