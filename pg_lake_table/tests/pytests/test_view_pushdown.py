import pytest
from utils_pytest import *

views_foreign_table = [
    "CREATE VIEW v1 AS SELECT DISTINCT id FROM Users_f",
    "CREATE VIEW v2 AS SELECT DISTINCT U.id FROM Users_f U JOIN (WITH cte_1 AS (SELECT * FROM v1) SELECT * FROM cte_1 ) as foo(a) ON (foo.a = U.id)",
    "CREATE VIEW v3 AS SELECT * FROM (WITH final_cte AS (WITH user_data AS (SELECT * FROM Users_f), event_data AS (SELECT id FROM Events_f, (SELECT DISTINCT metric1 FROM Users_f OFFSET 0) AS temp1 WHERE Events_f.id = temp1.metric1 AND Events_f.id IN (SELECT DISTINCT id FROM Users_f ORDER BY 1 LIMIT 3)) SELECT event_data.id FROM user_data JOIN event_data ON event_data.id = user_data.id) SELECT count(*) AS cnt FROM final_cte, (SELECT DISTINCT Users_f.id FROM Users_f, Events_f WHERE Users_f.id = Events_f.id AND Events_f.event_type IN (1, 2, 3, 4) ORDER BY 1 DESC LIMIT 5) AS temp2 WHERE temp2.id = final_cte.id) AS result_table, Users_f WHERE result_table.cnt > Users_f.metric2;",
]

queries = [
    "SELECT * FROM v1",
    "SELECT * FROM v2",
    "SELECT * FROM v3",
    "SELECT * FROM v1 WHERE id NOT IN (SELECT id FROM v2)",
    "SELECT * FROM v1, Users_f U, v3 WHERE U.id > v1.id AND v3.id > v1.id",
]


def test_view_pushdown(create_pushdown_tables, pg_conn):

    # first create the views
    for view in views_foreign_table:
        run_command(view, pg_conn)

    # then make sure we are actually pushing down the queries as-is
    for query in queries:
        results = run_query("EXPLAIN (VERBOSE) " + query, pg_conn)
        if not ("Custom Scan (Query Pushdown)" in str(results)):
            assert False, "view pushdown query failed:" + query

    # lets also do some prepared statement tests
    run_command("PREPARE p0 AS SELECT * FROM v3", pg_conn)
    run_command("PREPARE p1(int) AS SELECT * FROM v3 WHERE metric2 > $1", pg_conn)
    run_command(
        "PREPARE p2(int, int[]) AS SELECT * FROM v3, Users_f WHERE v3.metric2 = any($2) and Users_f.id = v3.id + $1",
        pg_conn,
    )

    for i in range(0, 8):
        run_command(f"EXECUTE p0;", pg_conn)
        run_command(f"EXECUTE p1({i})", pg_conn)

        # pass string and expect casting done automatically
        run_command(f"EXECUTE p1('{i}')", pg_conn)

        run_command(f"EXECUTE p2({i}, ARRAY[{i}, {i+1}, {i+2}])", pg_conn)

    run_command("ROLLBACK", pg_conn)


def test_materialized_view(create_pushdown_tables, pg_conn):

    run_command(
        "CREATE MATERIALIZED VIEW v1 AS SELECT DISTINCT id FROM Users_f", pg_conn
    )

    results = run_query("EXPLAIN (VERBOSE) SELECT * FROM v1", pg_conn)

    # as we have materialized view, we should not be able to pushdown
    if "Custom Scan (Query Pushdown)" in str(results):
        assert False, "materialized view pushdown query failed:" + query

    run_command("ROLLBACK", pg_conn)
