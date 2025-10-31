# Postgres Extension Updater

The `pg_extension_updater` extension runs `ALTER EXTENSION .. UPDATE` for every extension that exists in the database on start-up. That way, PostgreSQL automatically catches up with new extension installations and the risk of SQL vs. binary mismatch is minimized. 

No configuration is necessary other than to create the extension in each database that requires automatic updates:

```sql
CREATE EXTENSION pg_extension_updater CASCADE;
```

It might make sense to do this in the `template1` database, such that all new databases have automatic updates. Note that it does not run in the template database itself, but will run in a new database immediately after creating it from a template.

When an `ALTER EXTENSION` fails, a warning is logged. The update is only attempted once.
