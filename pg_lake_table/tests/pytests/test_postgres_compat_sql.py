import pytest
import psycopg2
import time
import duckdb
import math
import datetime
from decimal import *
from utils_pytest import *


def test_fdw_pg_sql_compat(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/test_compressed_fdw_basic/data.parquet"

    run_command(
        f"""
        COPY (SELECT s AS id, CASE WHEN s > 3 THEN (s * 100)::text ELSE NULL END as value FROM generate_series(1,5) s) TO '{url}' WITH (FORMAT 'parquet');
    """,
        pg_conn,
    )

    # create the same table as an fdw and heap table
    run_command(
        """
                CREATE SCHEMA test_fdw_pg_sql_compat;

                CREATE FOREIGN TABLE test_fdw_pg_sql_compat.fdw () server pg_lake OPTIONS (path '{}');
                CREATE TABLE test_fdw_pg_sql_compat.heap () WITH (load_from='{}');

    """.format(
            url, url
        ),
        pg_conn,
    )

    tables = ["test_fdw_pg_sql_compat.fdw", "test_fdw_pg_sql_compat.heap"]
    success_queries = [
        """SELECT FROM {} as t1""",
        """SELECT t1 FROM {} as t1 ORDER BY 1""",
        """SELECT CASE WHEN (r1.*)::text IS NOT NULL THEN ROW(r1.id, r1.value) END from {} r1;""",
        """SELECT t1.* FROM {} as t1 ORDER BY 1""",
        """SELECT t1.id, t1.value FROM {} as t1 ORDER BY 1""",
        """SELECT row(id, value) FROM {} ORDER BY 1""",
        """SELECT id, s FROM {}, generate_series(0,10) s WHERE s = id ORDER BY 1""",
        """SELECT id, s FROM {}, (VALUES (1), (2)) as g(s) WHERE s = id ORDER BY 1""",
        """SELECT id, s FROM {} JOIN (VALUES (1), (2)) as g(s) on (s = id) ORDER BY 1""",
        """SELECT DISTINCT id FROM {} JOIN (VALUES (1), (2)) as g(id) USING (id) ORDER BY 1""",
        """SELECT id, s FROM {}, unnest(ARRAY[1, 2]) as g(s) WHERE s = id ORDER BY 1""",
        """SELECT id, s FROM {} JOIN unnest(ARRAY[1, 2]) as g(s) on (s = id) ORDER BY 1""",
        """SELECT DISTINCT id FROM {} JOIN unnest(ARRAY[1, 2]) as g(id) USING (id) ORDER BY 1""",
        """SELECT ARRAY[id], ARRAY[s] FROM {}, generate_series(0,10) s WHERE s = id ORDER BY 1""",
        """SELECT t1.id, t1.value, t2.aggregated_value FROM {} t1, LATERAL (SELECT SUM(id) as aggregated_value FROM test_fdw_pg_sql_compat.heap t2 WHERE t2.id = t1.id) t2 ORDER BY 1;""",
        """SELECT id, CASE WHEN id > 3 THEN 'High' ELSE 'Low' END AS value_category FROM {} ORDER BY 1""",
        """SELECT id, array_agg(value) OVER (PARTITION BY value ORDER BY id) AS values_by_category FROM {} ORDER BY 1;""",
        """SELECT 'Name: ' || value || ', Age: ' || id AS user_info FROM {} ORDER BY 1""",
        """SELECT COALESCE(value, 'No description provided.') FROM {} ORDER BY 1;""",
        """SELECT id FROM {} WHERE id IN (1,2,3) ORDER BY 1;""",
        """SELECT id FROM {} WHERE id = any('{{1,2,3}}') ORDER BY 1;""",
        """SELECT id FROM {} WHERE id = any(array[1,2,3]) ORDER BY 1;""",
        """SELECT id, (value IS NULL) AS active_status FROM {} ORDER BY 1;""",
        #'''SELECT id, ARRAY_AGG(value ORDER BY value) AS names FROM {} GROUP BY id''' requires #156,
        """SELECT generate_series(1,10) AS series, * FROM {} ORDER BY 1;""",
        """SELECT id, value, CASE WHEN value SIMILAR TO '%(SQL|PostgreSQL)%' THEN TRUE ELSE FALSE END AS matches_pattern FROM {} ORDER BY 1;""",
    ]
    pg_conn.commit()

    # make sure queries return the exact same results
    fdw_cur, heap_cur = pg_conn.cursor(), pg_conn.cursor()

    for execution_type in ["vectorize", "pushdown"]:

        if execution_type == "vectorize":
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )
        else:
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )

        for q in success_queries:
            fdw_cur.execute(q.format(tables[0]))
            fdw_result = fdw_cur.fetchall()

            heap_cur.execute(q.format(tables[1]))
            heap_result = heap_cur.fetchall()

            assert heap_result == fdw_result

    failure_queries = [
        """SELECT id/0 FROM {} as t1""",
    ]

    # make sure queries throw exact same exceptions
    run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)
    for q in failure_queries:
        e_fdw, e_heap = None, None
        try:
            fdw_cur.execute(q.format(tables[0]))
            fdw_result = fdw_cur.fetchall()

            # never expected to come here
            assert False
        except Exception as e:
            e_fdw = e
            pass

        pg_conn.rollback()

        try:
            heap_cur.execute(q.format(tables[1]))
            heap_result = heap_cur.fetchall()

            # never expected to come here
            assert False
        except Exception as e:
            e_heap = e
            pass

        assert str(e_fdw) == str(e_heap)

        pg_conn.rollback()

    # Cleanup
    cur = pg_conn.cursor()
    cur.execute("DROP SCHEMA test_fdw_pg_sql_compat CASCADE;")
    run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
    pg_conn.commit()
