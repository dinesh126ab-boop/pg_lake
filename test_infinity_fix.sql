-- Test script for infinity date/timestamp fix (Issue #27)
-- This should be run inside the Docker container after rebuilding

-- Setup
CREATE EXTENSION IF NOT EXISTS pg_lake CASCADE;
SET pg_lake_iceberg.default_location_prefix TO 's3://testbucket';

-- Test 1: Date with day partition (original issue)
\echo '=== Test 1: Date with infinity and day partition ==='
CREATE TABLE test_date_day(a date) USING iceberg WITH (partition_by = 'day(a)');
\echo 'Attempting to insert infinity (should FAIL)...'
INSERT INTO test_date_day VALUES ('infinity');
-- Expected: ERROR:  +-Infinity dates are not allowed in iceberg tables

\echo 'Attempting to insert -infinity (should FAIL)...'
INSERT INTO test_date_day VALUES ('-infinity');
-- Expected: ERROR:  +-Infinity dates are not allowed in iceberg tables

\echo 'Attempting to insert valid date (should SUCCEED)...'
INSERT INTO test_date_day VALUES ('2024-01-01');
SELECT * FROM test_date_day;
DROP TABLE test_date_day;

-- Test 2: Timestamp with month partition
\echo '=== Test 2: Timestamp with infinity and month partition ==='
CREATE TABLE test_ts_month(a timestamp) USING iceberg WITH (partition_by = 'month(a)');
\echo 'Attempting to insert infinity timestamp (should FAIL)...'
INSERT INTO test_ts_month VALUES ('infinity');
-- Expected: ERROR:  +-Infinity timestamps are not allowed in iceberg tables

\echo 'Attempting to insert valid timestamp (should SUCCEED)...'
INSERT INTO test_ts_month VALUES ('2024-01-01 12:00:00');
SELECT * FROM test_ts_month;
DROP TABLE test_ts_month;

-- Test 3: TimestampTZ with year partition
\echo '=== Test 3: TimestampTZ with infinity and year partition ==='
CREATE TABLE test_tstz_year(a timestamptz) USING iceberg WITH (partition_by = 'year(a)');
\echo 'Attempting to insert infinity timestamptz (should FAIL)...'
INSERT INTO test_tstz_year VALUES ('infinity');
-- Expected: ERROR:  +-Infinity timestamps are not allowed in iceberg tables

\echo 'Attempting to insert valid timestamptz (should SUCCEED)...'
INSERT INTO test_tstz_year VALUES ('2024-01-01 12:00:00+00');
SELECT * FROM test_tstz_year;
DROP TABLE test_tstz_year;

-- Test 4: Date with bucket partition
\echo '=== Test 4: Date with infinity and bucket partition ==='
CREATE TABLE test_date_bucket(a date) USING iceberg WITH (partition_by = 'bucket(10, a)');
\echo 'Attempting to insert infinity date (should FAIL)...'
INSERT INTO test_date_bucket VALUES ('infinity');
-- Expected: ERROR:  +-Infinity dates are not allowed in iceberg tables

\echo 'Attempting to insert valid date (should SUCCEED)...'
INSERT INTO test_date_bucket VALUES ('2024-01-01');
SELECT * FROM test_date_bucket;
DROP TABLE test_date_bucket;

\echo '=== All tests completed! ==='
\echo 'Expected: All infinity inserts should have FAILED with appropriate error messages'
\echo 'Expected: All valid date/timestamp inserts should have SUCCEEDED'
