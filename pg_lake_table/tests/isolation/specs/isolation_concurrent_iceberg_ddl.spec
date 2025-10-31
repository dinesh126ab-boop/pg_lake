setup
{
    CREATE TABLE test_iceberg_dml (key int, value text) USING pg_lake_iceberg;
    ALTER FOREIGN TABLE test_iceberg_dml OPTIONS (ADD autovacuum_enabled 'false');
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

step "s1-delete"
{
    DELETE FROM test_iceberg_dml WHERE key = 1;
}

step "s1-vacuum"
{
    VACUUM test_iceberg_dml;
}

step "s1-truncate"
{
    TRUNCATE test_iceberg_dml;
}

step "s1-ddl"
{
    ALTER TABLE test_iceberg_dml ADD CONSTRAINT check_a_positive_2 CHECK (key > 0);
}

step "s1-add-column"
{
    ALTER TABLE test_iceberg_dml ADD COLUMN new_col INT;
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-select"
{
    SELECT * FROM test_iceberg_dml;
}

step "s2-vacuum"
{
    VACUUM test_iceberg_dml;
}

step "s2-truncate"
{
    TRUNCATE test_iceberg_dml;
}

step "s2-ddl"
{
    ALTER TABLE test_iceberg_dml ADD CONSTRAINT check_a_positive CHECK (key > 0);
}

step "s2-begin"
{
    BEGIN;
}

step "s2-commit"
{
    COMMIT;
}

permutation "s1-begin" "s1-insert" "s2-vacuum" "s1-commit"
permutation "s1-begin" "s1-update" "s2-vacuum" "s1-commit"
permutation "s1-begin" "s1-delete" "s2-vacuum" "s1-commit"

permutation "s1-begin" "s1-insert" "s2-truncate" "s1-commit"
permutation "s1-begin" "s1-update" "s2-truncate" "s1-commit"
permutation "s1-begin" "s1-delete" "s2-truncate" "s1-commit"

permutation "s1-begin" "s1-insert" "s2-ddl" "s1-commit"
permutation "s1-begin" "s1-update" "s2-ddl" "s1-commit"
permutation "s1-begin" "s1-delete" "s2-ddl" "s1-commit"

# we cannot run VACUUM in tx, so exclude such permutations
permutation "s1-begin" "s1-truncate" "s2-vacuum" "s1-commit"
permutation "s1-begin" "s1-ddl" "s2-vacuum" "s1-commit"

permutation "s1-begin" "s1-truncate" "s2-truncate" "s1-commit"
permutation "s1-begin" "s1-ddl" "s2-truncate" "s1-commit"

permutation "s1-begin" "s1-truncate" "s2-ddl" "s1-commit"
permutation "s1-begin" "s1-ddl" "s2-ddl" "s1-commit"

permutation "s1-begin" "s1-add-column" "s2-select" "s1-commit"
permutation "s2-begin" "s2-select" "s1-add-column" "s2-commit" "s2-select"

