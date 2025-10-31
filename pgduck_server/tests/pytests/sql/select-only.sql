\set aid random(1, 1000 * :scale)
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;