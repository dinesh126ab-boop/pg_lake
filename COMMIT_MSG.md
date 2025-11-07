Fix: Reject infinity dates/timestamps in Iceberg tables to prevent overflow errors

Fixes #27

## Problem
When inserting 'infinity' or '-infinity' date/timestamp values into 
partitioned Iceberg tables, these special PostgreSQL values were 
converted to extreme numeric epoch values. This caused OverflowError 
when other query engines (e.g., Spark) attempted to read the Iceberg 
files.

## Changes
Added infinity validation in two critical code paths:

1. **Binary Serializer** (pg_lake_iceberg/src/iceberg/iceberg_type_binary_serde.c)
   - Added DATE_NOT_FINITE check for DATE values before conversion
   - Added TIMESTAMP_NOT_FINITE check for TIMESTAMP values
   - Added TIMESTAMP_NOT_FINITE check for TIMESTAMPTZ values

2. **Partition Transform** (pg_lake_table/src/fdw/partition_transform.c)
   - Added infinity checks for DATE in bucket partition transform
   - Added infinity checks for TIMESTAMP in bucket partition transform  
   - Added infinity checks for TIMESTAMPTZ in bucket partition transform

## Error Message
Attempting to insert infinity now fails with:
```
ERROR:  +-Infinity dates are not allowed in iceberg tables
HINT:  Delete or replace +-Infinity values.
```

This matches the existing error messages in temporal_utils.c for 
consistency.

## Impact
- **Breaking Change**: INSERT operations with infinity dates/timestamps 
  that previously succeeded will now fail immediately
- **Benefit**: Prevents creating Iceberg files that cause overflow 
  errors in other query engines
- **Migration**: Users with existing infinity values must clean/replace 
  them before using partitioned Iceberg tables

## Testing
Run test_infinity_fix.sql to verify the fix works correctly for 
various partition transform types (day, month, year, bucket).
