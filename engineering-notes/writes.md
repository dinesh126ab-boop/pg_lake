# Writes to pg_lake tables

To support writes to pg_lake tables we implement the ExecForeignInsert/Update/Delete hooks in the foreign data wrapper implementation.

## Insert

For insert/COPY/INSERT..SELECT we follow these high-level steps:

1. Capture incoming inserts in ExecForeignInsert into a temporary CSV file.
2. When done, convert the CSV file to Parquet in S3 and add it to the metadata

The foreign data wrapper machinery takes care of most other concerns, except constraints. We manually check constraints in ExecForeignInsert.

## Update

For updates we follow these high-level steps:

1. If applicable, we process any `FROM` entries, which become separate foreign table scans.
2. We execute a scan on the result relation via `read_parquet()`, injecting a synthetic `ctid` which is structured as `(filename, row_number)` in a DuckDB struct.
3. As a result of the operations in steps 1 and 2, PostgreSQL identifies the tuples that have been modified in the target table.
4. For each modified tuple (as identified in step 3), `postgresExecForeignUpdate()` follows these steps:
   - For each tuple to be deleted, `DeleteSingleRow()` is executed. We capture the filenumber and row number of each deleted tuple, which are then written into `deleteDest/deleteFile` for each source file.
   - New rows generated as part of the update statement are written to a single temporary insert file. This step involves writing only the data; `ctid` is not included.
   - We additionally check and partition constraints.
5. When done, we apply the changes:
   - Remove source files with updates from the metadata.
   - For each deletion file, execute a query like `SELECT * FROM source WHERE ctid NOT IN (SELECT ctid FROM deleteFile)`. The results are then "COPY"ed into a new Parquet file, which is added to the metadata.
   - Add new rows into a new Parquet files.

## Delete

Deletes are mostly a subset of updates without generating new rows. A notable difference is that PostgreSQL does not provide the value of the old tuple, but does expect ExecForeignDelete to return the relevant RETURNING values. We address this by requesting the RETURNING Vars via AddForeignUpdateTargets, such that they are available via planSlot in ExecForeignDelete. We then them into the result slot of ExecForeignDelete.

The high-level execution steps are:

1. If applicable, we process any `USING` entries, which become separate foreign table scans.
2. We execute a scan on the result relation via `read_parquet()`, injecting a synthetic `ctid` which is structured as `(filename, row_number)` in a DuckDB struct.
3. As a result of the operations in steps 1 and 2, PostgreSQL identifies the tuples that have been modified in the target table.
4. For each modified tuple (as identified in step 3), `postgresExecForeignDelete()` follows these steps:
   - `DeleteSingleRow()` is executed. We capture the filenumber and row number of each deleted tuple, which are then written into `deleteDest/deleteFile` for each source file.
   - RETURNING values are copied from the planSlot.
5. When done, we apply the changes:
   - Remove source files with updates from the metadata.
   - For each deletion file, execute a query like `SELECT * FROM source WHERE ctid NOT IN (SELECT ctid FROM deleteFile)`. The results are then "COPY"ed into a new Parquet file, which is added to the metadata.
