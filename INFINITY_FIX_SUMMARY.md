# Fix for Issue #27: OverflowError on Infinity Dates

## Problem
When inserting `'infinity'` date/timestamp values into Iceberg tables with partitions, the values were converted to extreme numeric values that caused `OverflowError` when read by other query engines like Spark.

## Root Cause
PostgreSQL's infinity dates/timestamps were being converted to Unix epoch values without validation, producing numbers that overflow in Java-based engines (Spark, etc).

## Solution
Added explicit infinity checks before conversion in two critical code paths:

### 1. Binary Serializer (`pg_lake_iceberg/src/iceberg/iceberg_type_binary_serde.c`)
- Lines 220-231: Added `DATE_NOT_FINITE` check for DATE values
- Lines 240-251: Added `TIMESTAMP_NOT_FINITE` check for TIMESTAMP values  
- Lines 260-271: Added `TIMESTAMP_NOT_FINITE` check for TIMESTAMPTZ values

### 2. Partition Transform (`pg_lake_table/src/fdw/partition_transform.c`)
- Lines 715-727: Added `DATE_NOT_FINITE` check for DATE bucket partition
- Lines 733-745: Added `TIMESTAMP_NOT_FINITE` check for TIMESTAMP bucket partition
- Lines 748-760: Added `TIMESTAMP_NOT_FINITE` check for TIMESTAMPTZ bucket partition

## Error Message
```
ERROR:  +-Infinity dates are not allowed in iceberg tables
HINT:  Delete or replace +-Infinity values.
```

This matches the existing error from `temporal_utils.c` for consistency.

## Testing Instructions

### Using Docker (Recommended for Windows)

1. Navigate to docker directory:
   ```powershell
   cd docker
   ```

2. Rebuild containers with changes:
   ```powershell
   docker compose build
   docker compose up -d
   ```

3. Connect to PostgreSQL:
   ```powershell
   docker exec -it pg_lake psql
   ```

4. Run the test script:
   ```sql
   \i /workspace/test_infinity_fix.sql
   ```

   Or test manually:
   ```sql
   CREATE EXTENSION pg_lake CASCADE;
   SET pg_lake_iceberg.default_location_prefix TO 's3://testbucket';
   
   CREATE TABLE test(a date) USING iceberg WITH (partition_by = 'day(a)');
   INSERT INTO test VALUES ('infinity');
   -- Should fail with: ERROR:  +-Infinity dates are not allowed in iceberg tables
   
   INSERT INTO test VALUES ('2024-01-01');
   -- Should succeed
   SELECT * FROM test;
   ```

### Expected Behavior

**Before Fix:**
- `INSERT INTO test VALUES ('infinity')` would succeed
- Spark query would fail with `OverflowError`

**After Fix:**
- `INSERT INTO test VALUES ('infinity')` fails immediately with clear error
- Normal dates work fine
- Spark can read all created Iceberg files without overflow

## Files Changed

1. `pg_lake_iceberg/src/iceberg/iceberg_type_binary_serde.c`
   - Prevents writing infinity values to Iceberg binary format
   
2. `pg_lake_table/src/fdw/partition_transform.c`
   - Prevents computing partition values from infinity dates/timestamps

## Related Code
- `pg_lake_iceberg/src/utils/temporal_utils.c` - Contains `EnsureNotInfinityDate()` and `EnsureNotInfinityTimestamp()` helpers (already checked in some paths)
- These new checks extend the protection to serialization and partition transform paths

## Impact
- **Breaking Change:** Inserts with infinity dates/timestamps that previously succeeded will now fail
- **Benefit:** Prevents creating Iceberg files that crash other engines
- **Migration:** Users with existing infinity values need to clean/replace them before using partitioned Iceberg tables
