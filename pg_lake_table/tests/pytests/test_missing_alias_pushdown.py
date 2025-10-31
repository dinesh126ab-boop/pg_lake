import pytest
from utils_pytest import *

queries = [
    # let Postgres pick column names column1 and column2 for the subquery
    "SELECT unnamed_subquery_2.column1, unnamed_subquery_2.column2 FROM (VALUES (1,2), (2,3), (3,4)) unnamed_subquery_2, Users_f WHERE unnamed_subquery_2.column1 = Users_f.id;",
    # let Postgres pick only column2 for the subquery
    "SELECT unnamed_subquery_2.a, unnamed_subquery_2.column2 FROM (VALUES (1,2), (2,3), (3,4)) unnamed_subquery_2(a), Users_f WHERE unnamed_subquery_2.column2 = Users_f.id;",
    # let's give the same column names Postgres would give for the VALUES
    "SELECT v.column1, v.column2 FROM (VALUES (1, 2), (2, 3), (3, 4)) AS v(column1, column2) JOIN Users_f ON v.column1 = Users_f.id;",
    # let Postgres pick column names column1 and column2 for the VALUES
    "SELECT v.column1, v.column2 FROM (VALUES (1, 2), (2, 3), (3, 4)) AS v JOIN Users_f ON v.column1 = Users_f.id;",
    # for subqueries with relations this we don't need to give alias names
    "SELECT DISTINCT unnamed_subquery.* FROM (SELECT * FROM Users_f) unnamed_subquery;",
    "WITH ValuePairs AS (VALUES (1, 2), (2, 3), (3, 4) ) SELECT vp.column1, vp.column2 FROM ValuePairs AS vp(column1, column2) JOIN Users_f ON vp.column1 = Users_f.id;",
    "WITH ValuePairs AS ( SELECT * FROM (VALUES (1, 2), (2, 3), (3, 4)) v) SELECT vp.column1, vp.column2 FROM ValuePairs AS vp(column1, column2) JOIN Users_f ON vp.column1 = Users_f.id;",
    "WITH ValuePairs AS ( SELECT * FROM (VALUES (1, 2), (2, 3), (3, 4)) v(a) ) SELECT vp.a, vp.column2 FROM ValuePairs AS vp(a, column2) JOIN Users_f ON vp.a = Users_f.id;",
    "WITH ValuePairs AS MATERIALIZED (  VALUES (1, 2), (2, 3), (3, 4)) SELECT vp.column1, vp.column2 FROM ValuePairs AS vp(column1, column2) JOIN Users_f ON vp.column1 = Users_f.id;",
]


def test_missing_alias_pushdown(create_pushdown_tables, pg_conn):

    # then make sure we are actually pushing down the queries as-is
    for query in queries:
        results = run_query("EXPLAIN (VERBOSE, ANALYZE) " + query, pg_conn)
        if not ("Custom Scan (Query Pushdown)" in str(results)):
            assert False, "view pushdown query failed:" + query
