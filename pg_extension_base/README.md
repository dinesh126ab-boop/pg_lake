# pg_extension_base: A foundational PostgreSQL extension

The goal of pg_extension_base is to simplify extension development by providing common infrastructure for hard extension development problems.

Currently, pg_extension_base tackles the following problems:
- [Auto-load extensions that want to be auto-loaded](#library-preloader-auto-load-extensions-that-want-to-be-auto-loaded)
- [Base workers: Extension lifecycle background workers](#base-workers-extension-lifecycle-background-workers)
- [Create new dependencies and update existing ones](#create-new-dependencies-and-update-existing-ones)
- [Attached background workers](#attached-background-workers)

To run tests for pg_extension_base, you first need to install the test extensions:
```bash
make prepare-check # installs test extensions
make check
make finish-check # uninstalls test extensions
```

The reason for separating these commands is that (un)install might require superuser if your PostgreSQL installation directory is in /usr.

## Library preloader: Auto-load extensions that want to be auto-loaded

There are certain extensions that require or benefit from their module being loaded on start-up. To do so, the platform administrator needs to modify shared_preload_libraries and put modules in the right order. However, extension developers and platform administrators are typically different teams with different deployment pipelines, which complicates the deployment of new extensions and new extension versions. 

With pg_extension_base, extension developers can add a line to their .control to signal that they should be loaded on start-up:
```
comment = 'Pg Extension Base Data Scheduler'
default_version = '1.0'
requires = 'pg_extension_base'
module_pathname = '$libdir/pg_extension_base_scheduler'
#!shared_preload_libraries
```
That way, administrators only need to set `shared_preload_libraries = 'pg_extension_base'` and extensions that use the special syntax will be loaded by pg_extension_base.

In addition, you can explicitly specify the libraries to load, including the extension's own module:

```
#!shared_preload_libraries = 'common_lib,$libdir/pg_extension_base_scheduler'
```

This is useful when multiple extensions depend on a common library.

## Base workers: Extension lifecycle background workers

Background workers can only access a single database, but extensions can exist in many databases. We ideally want a background worker to exist for the lifetime of an extension instance within a particular database, but there are several challenging scenarios:

- **Server restart**: Background workers should be be started on start-up. While we can start a background worker on start-up, we cannot access the database to determine whether an extension exists in a particular database.
- **CREATE EXTENSION**: The background worker should start immediately after a CREATE EXTENSION, unless it aborts.
- **DROP EXTENSION**: The background worker should stop immediately after a DROP EXTENSION, unless it aborts.
- **CREATE DATABASE .. TEMPLATE**: We want the background worker to start in databases that were created from a template database that already has the extension.
- **DROP DATABASE**: We need to kill background workers attached to a particular database before we can drop it, but the DROP DATABASE is still prone to fail if clients are connected to it, in which case the background workers should resume.

Operations on a background worker are not transactional, and nor are operations on database, which creates a challenging set of race conditions that need to be solved.

With pg_extension_base, extension developers do not have to solve these challenging systems problems over and over again to have a simple background worker. Instead, they can simply register an internal function to be run as a background worker:
```
-- Create a C function with a single internal argument that returns internal
CREATE FUNCTION extension_base_scheduler.main_worker(internal)
 RETURNS internal
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_test_scheduler_main_worker$function$;

SELECT extension_base.register_worker('pg_extension_base_test_scheduler_main_worker', 'extension_base_scheduler.main_worker');
```

The function will be called from a "base worker" when the server starts, the extension is created, or a database is created from a template that has the extension. The background worker will be automatically killed during DROP EXTENSION or DROP DATABASE, but will be restarted if these commands fail.

Internally, pg_extension_base uses 3 layers of background workers:
- **server starter** is not attached to a specific database and can therefore only access the shared pg_database catalog. Its main job is to start a database starter for each database on start-up. It keeps running in case a later DROP command fails or a new database is created with the extension.
- **database starter** is attached to a specific database and can therefore see whether the pg_extension_base extension exists and whether any base workers were registered. If so, it starts those workers and then exits.
- **base worker** is attached to a specific database and started by the corresponding database starter. The base worker calls an extension-defined function on start-up, outside of a transaction. The extension developer can decide what to do within the function (e.g. loop forever, start transactions, etc.)

The SQL function is declared with an internal argument to prevent calls from SQL. The implementation of the main worker function is provided with an integer argument containing the worker ID. The function is called from the specific database in which it is registered as the superuser. Unlike regular UDFs, the function is not called from a transaction, and should start its own transactions as needed. Some helper macros are provided in `base_workers.h`

```c
Datum
pg_extension_base_test_scheduler_main_worker(PG_FUNCTION_ARGS)
{
	int32 workerId = PG_GETARG_INT32(0);

	while (true)
	{
		START_TRANSACTION();
		{
			// Access the database
		}
		END_TRANSACTION();

		// Sleep, but wake up on signals
		LightSleep(1000);
	}

	PG_RETURN_VOID();
}

```

While the worker starts when the extension is created and is stopped before the extension is dropped, it is not strictly guaranteed that the extension exists 100% of the time when the worker is running. Stopping is done on a best-effort basis. Workers should remember to call `CHECK_FOR_INTERRUPTS()` during long-running operations to react to signals.

## Create new dependencies and update existing ones

When an extension adds a new dependency to the `requires` line in the extension control file, an ALTER EXTENSION .. UPDATE will fail if the new dependency does not exist yet. However, updating extension is often done by the control plane without knowledge of dependencies. Hence, it is useful if we can automatically create new dependencies during ALTER EXTENSION. We also prefer to update dependent extensions before finalizing the ALTER EXTENSION .. UPDATE, to avoid issues with older schema versions of dependencies.

## Attached background workers

Sometimes it's useful to be able to run a command in a background worker to commit it immediately or run it in a different database, and get feedback on errors and results. Attached background workers do just that and can be run via SQL or C:

```sql
select * from pg_extension_base.run_attached($$insert into records values ('commits immediately')$$);
┌────────────┬──────────────┐
│ command_id │ command_tag  │
├────────────┼──────────────┤
│          0 │ INSERT       │
└────────────┴──────────────┘
(1 row)

```

Workers are allocated in the memory context of the caller and automatically terminated in case of error. They are not meant for starting long-running background tasks (that would be a detached worker).
