Upgrading DuckDB version
========================

In order to upgrade the DuckDB version, there are multiple places that need to be considered/updated:

- duckdb_pglake submodule

    cd duckdb_pglake/duckdb && git fetch && git checkout $DUCKDB_VERSION

- DUCKDB_VERSION in Makefile
- duckdb version in Pipfile

    pipenv install duckdb==$DUCKDB_VERSION && pipenv lock

Updating plugins
----------------

In addition, there are different plugins/submodule references to update.  Annoyingly, there are several different ways these are managed.  (There is an outstanding task to merge approaches into a single one.)

However, the basic idea is the same: for all submodules you will need to find the upstream repo, and adjust any custom patches against the corresponding upstream source.

There are some components that need special handling:

- update specific SHA1 references in duckdb_pglake/extension_config.cmake:

  - duckdb_azure - remote sha1 ref to PgLake-managed repo with custom patches.


Testing
-------

All of the above is just getting the DuckDB version updated.  We also need to see what changes are needed for pg_lake code based on the changed DuckDB version.  This is basically running the test suite, seeing if anything breaks, and then fixing it.
