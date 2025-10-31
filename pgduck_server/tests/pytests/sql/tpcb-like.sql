
\set aid random(1,1000 * :scale)
\set bid random(1,1000* :scale)
\set tid random(1, 1000* :scale)
\set delta random(-5000, 5000)
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta::int WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
UPDATE pgbench_tellers SET tbalance = tbalance + :delta::int WHERE tid = :tid;
UPDATE pgbench_branches SET bbalance = bbalance + :delta::int WHERE bid = :bid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;