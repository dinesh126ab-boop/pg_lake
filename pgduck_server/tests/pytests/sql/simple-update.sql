\set aid random(1, 1000 * :scale)
\set bid random(1,1000 * :scale)
\set tid random(1, 1000 * :scale)
\set delta random(-5000, 5000)

BEGIN;
UPDATE pgbench_accounts SET abalance = abalance::int + :delta::int WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;
