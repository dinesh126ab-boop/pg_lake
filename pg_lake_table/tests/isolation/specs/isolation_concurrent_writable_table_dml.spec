setup
{
    CREATE FOREIGN TABLE test_writable_dml (key int, value text) SERVER pg_lake OPTIONS (format 'parquet', location 's3://testbucketcdw/writable_isolation', writable 'true') ;
}

teardown
{
    DROP TABLE IF EXISTS test_writable_dml CASCADE;
}

session "s1"


step "s1-begin"
{
    BEGIN;
}

step "s1-insert"
{
    INSERT INTO test_writable_dml VALUES(1);
}

step "s1-update-key-1"
{
    UPDATE test_writable_dml SET key = 15 WHERE key = 1;
}

step "s1-delete-key-1"
{
    DELETE FROM test_writable_dml WHERE key = 1;
}

step "s1-select-all"
{
   SELECT * FROM test_writable_dml ORDER BY 1,2;
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

step "s2-insert"
{
    INSERT INTO test_writable_dml VALUES(2);
}

step "s2-update-key-1"
{
    UPDATE test_writable_dml SET key = 16 WHERE key = 1;
}

step "s2-delete-key-1"
{
    DELETE FROM test_writable_dml WHERE key = 1;
}

step "s2-update-key-2"
{
    UPDATE test_writable_dml SET key = 16 WHERE key = 2;
}


step "s2-commit"
{
    COMMIT;
}

# even if we update different rows, we acquire table locks so block each other 
permutation "s1-insert" "s1-begin" "s1-update-key-1" "s2-update-key-1" "s1-commit" 
permutation "s1-insert" "s1-begin" "s1-update-key-1" "s2-update-key-2" "s1-commit" 
permutation "s1-insert" "s1-begin" "s1-delete-key-1" "s2-update-key-2" "s1-commit" 
permutation "s1-insert" "s1-begin" "s1-update-key-1" "s2-delete-key-1" "s1-commit" 


# 2 INSERTs do not block each other
permutation "s1-begin" "s1-insert" "s2-insert" "s1-commit" "s1-select-all"
