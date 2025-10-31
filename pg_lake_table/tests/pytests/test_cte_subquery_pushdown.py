import pytest
from utils_pytest import *


queries = [
    # CTE in SELECT clause
    "SELECT u.*, event_details.max_metric FROM Users_f u, (WITH EventCTE AS (SELECT id, MAX(metric2) AS max_metric FROM Events_f GROUP BY id) SELECT id, max_metric FROM EventCTE) AS event_details WHERE u.id = event_details.id;",
    # Subquery in WHERE clause
    "SELECT DISTINCT * FROM Users_f WHERE EXISTS (SELECT 1 FROM Events_f WHERE Users_f.id = Events_f.id AND Events_f.event_type > 2);",
    # Join between subqueries
    "SELECT DISTINCT * FROM (SELECT id, metric1 FROM Users_f) AS u JOIN (SELECT id, metric2 FROM Events_f) AS e ON u.id = e.id WHERE e.metric2 > u.metric1;",
    # Deep nested subqueries
    "SELECT DISTINCT id, (SELECT AVG(alias_metric) FROM (SELECT DISTINCT metric2 FROM Events_f e WHERE e.id = u.id) AS sub_Events_f (alias_metric) ) AS avg_metric FROM Users_f u;",
    # Correlated subquery with CTE
    "WITH UserCTE (alias_id, alias_metric) AS (SELECT id, metric1 FROM Users_f) SELECT DISTINCT id, (SELECT AVG(alias_metric) FROM UserCTE ucte WHERE ucte.alias_id < Users_f.id) AS avg_prev_metric1 FROM Users_f;",
    # CTE in WHERE clause
    "SELECT DISTINCT id FROM Users_f WHERE id NOT IN (WITH RECURSIVE cte AS (SELECT id FROM Events_f WHERE event_type > 96 UNION ALL SELECT e.id FROM Events_f e JOIN cte ON e.id = cte.id) SELECT id FROM cte);",
    # Multiple CTEs with a join in main query
    "WITH UserMetrics AS (SELECT id, SUM(metric1) AS sum_metric1 FROM Users_f GROUP BY id), EventCounts AS (SELECT id, COUNT(*) AS event_count FROM Events_f GROUP BY id) SELECT DISTINCT um.id, um.sum_metric1, ec.event_count FROM UserMetrics um JOIN EventCounts ec ON um.id = ec.id;",
    # Correlated subquery inside a CTE
    "WITH UserEventCTE AS (SELECT u.id, (SELECT COUNT(*) FROM Events_f e WHERE e.id = u.id AND e.event_type != u.metric1) AS event_count FROM Users_f u) SELECT DISTINCT id, event_count FROM UserEventCTE WHERE event_count > 0;",
    # Complex CTE with multiple nested levels and joins
    "WITH FirstLevel AS (SELECT id, metric1 FROM Users_f), SecondLevel AS (SELECT f.id, f.metric1, e.event_type FROM FirstLevel f JOIN Events_f e ON f.id = e.id) SELECT DISTINCT s.id, AVG(s.metric1) AS avg_metric FROM SecondLevel s GROUP BY s.id;",
    # CTE with correlated subquery in the SELECT clause
    "WITH EventData(alias_id) AS (SELECT id, event_type FROM Events_f) SELECT DISTINCT u.id, (SELECT MAX(e.event_type) - min(e.alias_id+e.alias_id) FROM EventData e WHERE e.alias_id = u.id) AS max_event_type FROM Users_f u;",
    # Multiple CTEs with dependencies
    "WITH Users_fummary AS (SELECT id, SUM(metric1) AS total_metric1 FROM Users_f GROUP BY id), Events_fummary AS (SELECT id, SUM(float_value) AS total_float FROM Events_f GROUP BY id) SELECT DISTINCT us.id, us.total_metric1, es.total_float FROM Users_fummary us JOIN Events_fummary es ON us.id = es.id;",
    # Recursive CTE with subquery in the recursive part
    "WITH RECURSIVE cte AS (SELECT id, metric1 FROM Users_f WHERE metric1 > 5 UNION ALL SELECT u.id, u.metric1 FROM Users_f u JOIN cte ON u.metric1 = cte.metric1 + 1) SELECT DISTINCT * FROM cte;",
    # Correlated subquery inside WHERE clause of a CTE
    "WITH FilteredEvents_f AS (SELECT id, event_type FROM Events_f) SELECT DISTINCT * FROM Users_f WHERE NOT EXISTS (SELECT 1 FROM FilteredEvents_f fe WHERE fe.id = Users_f.id AND fe.event_type > Users_f.metric1);",
    # Materialized CTE to optimize expensive join operation
    "WITH Events_ftats AS MATERIALIZED (SELECT id, AVG(metric2) AS avg_metric, MAX(float_value) AS max_float FROM Events_f GROUP BY id) SELECT DISTINCT U.id, U.metric1, E.avg_metric, E.max_float FROM Users_f U JOIN Events_ftats E ON U.id = E.id;",
    # Subquery in HAVING clause with column renaming in a subquery
    "SELECT DISTINCT id, SUM(metric1) AS total_metric FROM Users_f GROUP BY id HAVING SUM(metric1) > (SELECT AVG(metric1) FROM Users_f) AND SUM(metric1) < (SELECT MAX(metric1) FROM Users_f) ORDER BY total_metric DESC;",
    # CTE with column renaming and nested subquery in the SELECT clause
    "WITH UserMetrics AS (SELECT id AS user_id, metric1 AS m1, metric2 AS m2 FROM Users_f) SELECT DISTINCT U.user_id, (SELECT AVG(metric2) FROM Events_f WHERE id = U.user_id) AS avg_event_metric FROM UserMetrics U WHERE U.m2 > 10;",
    # Deep nested subquery within a CTE and having clause
    # "WITH RECURSIVE RecursiveData AS (WITH SomeData AS (SELECT id, metric1 FROM Users_f WHERE id < 3) SELECT id, metric1 FROM SomeData UNION ALL SELECT U.id, U.metric1 FROM Users_f U JOIN RecursiveData R ON U.id = R.id WHERE R.id < 1 ) SELECT * FROM RecursiveData;",
    # Using a materialized CTE in conjunction with a complex WHERE clause
    "WITH FilteredUsers_f AS MATERIALIZED (SELECT id, metric1 FROM Users_f WHERE metric1 > 5) SELECT DISTINCT F.id, E.event_type FROM FilteredUsers_f F JOIN Events_f E ON F.id != E.id WHERE E.metric2 <= ALL (SELECT metric2 FROM Events_f WHERE event_type != F.id);",
    # Correlated subquery inside a CTE used in an outer join
    "WITH UserCalculations AS (SELECT id, (SELECT COUNT(*) FROM Events_f E WHERE E.id = U.id AND E.event_type > 2) AS event_count FROM Users_f U) SELECT DISTINCT UC.id, UC.event_count, E.timestamp FROM UserCalculations UC LEFT JOIN Events_f E ON UC.id = E.id AND UC.event_count > 5;",
    # Nested CTEs with HAVING clause involving a subquery
    "WITH BaseData AS (SELECT id, metric1, metric2 FROM Users_f), AggregateData AS (SELECT id, SUM(metric1) AS sum_metric1 FROM BaseData GROUP BY id) SELECT DISTINCT A.id, A.sum_metric1 FROM AggregateData A GROUP BY 1,2 HAVING A.sum_metric1 > (SELECT AVG(metric1) FROM BaseData WHERE metric2 > 20);",
    # CTE with a CASE statement and window function
    "WITH UserData AS (SELECT id, metric1, metric2, CASE WHEN metric2 > metric1 THEN 'High' ELSE 'Low' END AS category FROM Users_f) SELECT DISTINCT id, category, AVG(metric2) OVER (PARTITION BY category) AS avg_metric FROM UserData WHERE category = 'High';",
    # Complex expression in CTE with multiple aggregation levels
    "WITH Metrics AS (SELECT id, SUM(metric1) AS total_metric, AVG(metric2) AS avg_metric FROM Users_f GROUP BY id) SELECT DISTINCT M.id, M.total_metric, M.avg_metric FROM Metrics M WHERE M.avg_metric > (SELECT AVG(avg_metric) FROM Metrics);",
    # CTE with explicit column renaming, used in a complex ORDER BY clause
    "WITH DetailedMetrics AS (SELECT id AS user_id, metric1 AS user_metric1, metric2 AS user_metric2 FROM Users_f) SELECT DISTINCT DM.user_metric1 + DM.user_metric2 FROM DetailedMetrics DM ORDER BY (DM.user_metric1 + DM.user_metric2) DESC;",
    # cte in order by
    "SELECT id FROM Users_f ORDER BY 1 LIMIT (WITH cte_1 AS (SELECT count(*) as cnt FROM Events_f) SELECT cnt FROM cte_1)",
]


def test_cte_subquery_pushdown(create_pushdown_tables, pg_conn):

    for query in queries:
        # first, compare the query results
        assert_query_results_on_tables(
            query, pg_conn, ["Users_f", "Events_f"], ["Users", "Events"]
        )

        # then make sure we are actually pushing down the expressions
        results = run_query("EXPLAIN (VERBOSE) " + query, pg_conn)
        if not ("Custom Scan (Query Pushdown)" in str(results)):
            assert False, "subquery/cte pushdown query failed:" + query
