import pytest
from utils_pytest import *


queries = [
    "SELECT id, COUNT(*) OVER (PARTITION BY id), rank() OVER (PARTITION BY id) FROM Users_f ORDER BY 1 DESC, 2 DESC, 3 DESC LIMIT 5;",
    "SELECT id, avg(avg(float_value)) OVER (PARTITION BY id, MIN(metric2)) FROM Users_f GROUP BY 1 ORDER BY 2 DESC NULLS LAST, 1 DESC;",
    "SELECT id, max(metric1) OVER (PARTITION BY id, MIN(metric2)) FROM (SELECT DISTINCT us.id, us.metric2, metric1 FROM Users_f as us, Events_f WHERE us.id = Events_f.id AND event_type IN (1,2) ORDER BY id, metric2) s GROUP BY 1, metric1 ORDER BY 2 DESC, 1;",
    "SELECT us.id, SUM(us.metric1) OVER (PARTITION BY us.id) FROM Users_f us JOIN Events_f ev ON (us.id = ev.id) GROUP BY 1, metric1 ORDER BY 1, 2 LIMIT 5;",
    # the following is failing due to PG and DuckDB handling join_rte's slightly differently
    # "SELECT id, metric1, SUM(j.metric1) OVER (PARTITION BY j.id) FROM (Users_f us JOIN Events_f ev USING (id)) j GROUP BY id, metric1 ORDER BY 3 DESC, 2 DESC, 1 DESC LIMIT 5;",
    "SELECT id, count (id) OVER (PARTITION BY id) FROM Users_f GROUP BY id HAVING avg(metric1) >= (SELECT min(metric1) FROM Users_f) ORDER BY 1 DESC,2 DESC LIMIT 1;",
    "SELECT DISTINCT ON (Events_f.id, rnk) Events_f.id, rank() OVER my_win AS rnk FROM Events_f, Users_f WHERE Users_f.id = Events_f.id WINDOW my_win AS (PARTITION BY Events_f.id, Users_f.metric1 ORDER BY Events_f.timestamp DESC) ORDER BY rnk DESC, 1 DESC LIMIT 10;",
    "SELECT DISTINCT ON (Events_f.id, rnk) Events_f.id, rank() OVER my_win AS rnk FROM Events_f, Users_f users_alias WHERE users_alias.id = Events_f.id WINDOW my_win AS (PARTITION BY Events_f.id, users_alias.metric1 ORDER BY Events_f.timestamp DESC) ORDER BY rnk DESC, 1 DESC LIMIT 10;",
    "SELECT DISTINCT ON (Events_f.id, rnk) Events_f.id, rank() OVER my_win AS rnk FROM Events_f, Users_f users_alias WHERE users_alias.id = Events_f.id WINDOW my_win AS (PARTITION BY Events_f.metric2, users_alias.metric1 ORDER BY Events_f.timestamp DESC) ORDER BY rnk DESC, 1 DESC LIMIT 10;",
    "SELECT id, rank() OVER my_win as rnk, avg(metric2) as avg_val_2 FROM Events_f GROUP BY id, date_trunc('day', timestamp) WINDOW my_win AS (PARTITION BY id ORDER BY avg(event_type) DESC) ORDER BY 3 DESC, 2 DESC, 1 DESC;",
    "SELECT COUNT(*) OVER (PARTITION BY id, id + 1), rank() OVER (PARTITION BY id) as cnt1, COUNT(*) OVER (PARTITION BY id, (metric1 - metric2)) as cnt2, date_trunc('min', lag(timestamp) OVER (PARTITION BY id ORDER BY timestamp)) as date, rank() OVER my_win as rnnk, avg(CASE WHEN id > 4 THEN metric1 ELSE metric2 END) FILTER (WHERE id > 2) OVER my_win_2 as filtered_count, sum(id * (5.0 / (metric1 + metric2 + 0.1)) * float_value) FILTER (WHERE metric1::text LIKE '%1%') OVER my_win_4 as cnt_with_filter_2 FROM Users_f WINDOW my_win AS (PARTITION BY id, (metric1)::int ORDER BY timestamp DESC), my_win_2 AS (PARTITION BY id, (metric1)::int ORDER BY timestamp DESC), my_win_3 AS (PARTITION BY id, date_trunc('min', timestamp)), my_win_4 AS (my_win_3 ORDER BY metric2, float_value) ORDER BY cnt_with_filter_2 DESC NULLS LAST, filtered_count DESC NULLS LAST, date DESC NULLS LAST, rnnk DESC, cnt2 DESC, cnt1 DESC, id DESC LIMIT 5;",
    "SELECT id, rank() OVER my_win as my_rank, avg(avg(event_type)) OVER my_win_2 as avg, max(timestamp) as mx_time FROM Events_f GROUP BY id, metric2 WINDOW my_win AS (PARTITION BY id, max(event_type) ORDER BY count(*) DESC), my_win_2 AS (PARTITION BY id, avg(id) ORDER BY count(*) DESC) ORDER BY avg DESC, mx_time DESC, my_rank DESC, id DESC;",
    "SELECT id, rank() OVER (PARTITION BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), dense_rank() OVER (PARTITION BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), CUME_DIST() OVER (PARTITION BY id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), PERCENT_RANK() OVER (PARTITION BY id ORDER BY avg(metric1) RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM Users_f GROUP BY 1 ORDER BY 4 DESC,3 DESC,2 DESC ,1 DESC;",
    "SELECT id, metric1, array_agg(metric1) OVER (PARTITION BY id ORDER BY metric1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), array_agg(metric1) OVER (PARTITION BY id ORDER BY metric1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM Users_f WHERE id > -20 AND id < 600 ORDER BY id, metric1, 3, 4;",
    "SELECT id, metric1, array_agg(metric1) OVER range_window, array_agg(metric1) OVER range_window_exclude FROM Users_f WHERE id > 2 AND id < 106 WINDOW range_window as (PARTITION BY id ORDER BY metric1 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING), range_window_exclude as (PARTITION BY id ORDER BY metric1 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) ORDER BY id, metric1, 3, 4;",
    "SELECT id, metric1, array_agg(metric1) OVER some_window_func, array_agg(metric1) OVER some_window_func_exclude FROM Users_f WHERE id > 2 and id < 86 WINDOW some_window_func as (PARTITION BY id ORDER BY metric1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), some_window_func_exclude as (PARTITION BY id ORDER BY metric1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE NO OTHERS) ORDER BY id, metric1, 3, 4;",
    "SELECT metric2, rank() OVER (PARTITION BY metric2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), dense_rank() OVER (PARTITION BY metric2 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), CUME_DIST() OVER (PARTITION BY metric2 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), PERCENT_RANK() OVER (PARTITION BY metric2 ORDER BY avg(metric1) RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM Users_f GROUP BY 1 ORDER BY 4 DESC,3 DESC,2 DESC ,1 DESC;",
    "SELECT metric2, metric1, array_agg(metric1) OVER (PARTITION BY metric2 ORDER BY metric1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), array_agg(metric1) OVER (PARTITION BY metric2 ORDER BY metric1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM Users_f WHERE metric2 > 0 AND metric2 < 96 ORDER BY metric2, metric1, 3, 4;",
    "SELECT metric2, metric1, array_agg(metric1) OVER range_window, array_agg(metric1) OVER range_window_exclude FROM Users_f WHERE metric2 > 0 AND metric2 < 99 WINDOW range_window as (PARTITION BY metric2 ORDER BY metric1 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING), range_window_exclude as (PARTITION BY metric2 ORDER BY metric1 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) ORDER BY metric2, metric1, 3, 4;",
    "SELECT metric2, metric1, array_agg(metric1) OVER some_window_func, array_agg(metric1) OVER some_window_func_exclude FROM Users_f WHERE metric2 > 1 and metric2 < 98 WINDOW some_window_func as (PARTITION BY metric2 ORDER BY metric1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), some_window_func_exclude as (PARTITION BY metric2 ORDER BY metric1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) ORDER BY metric2, metric1, 3, 4;",
    "SELECT id, sum(event_type) OVER my_win , event_type FROM Events_f GROUP BY id, event_type HAVING count(*) > -2 WINDOW my_win AS (PARTITION BY id, max(event_type) ORDER BY count(*) DESC) ORDER BY 2 DESC, 3 DESC, 1 DESC LIMIT 5;",
    "SELECT metric2, avg(float_value), dense_rank() OVER (PARTITION BY avg(float_value) ORDER BY avg(metric2)) FROM Users_f GROUP BY 1 ORDER BY 1 LIMIT 3;",
    "SELECT DISTINCT id, SUM(metric2) OVER (PARTITION BY id) FROM Users_f GROUP BY id, metric1, metric2 HAVING count(*) > -200 ORDER BY 2 DESC, 1 LIMIT 10;",
    "SELECT DISTINCT ON (id) id, SUM(metric2) OVER (PARTITION BY id) FROM Users_f GROUP BY id, metric1, metric2 HAVING count(*) > -20 ORDER BY 1, 2 DESC LIMIT 10;",
    "SELECT id, SUM(metric2) OVER (PARTITION BY id) FROM Users_f GROUP BY id, metric1, metric2 HAVING count(*) > -2000 ORDER BY (SUM(metric1) OVER (PARTITION BY id)) , 2 DESC, 1 LIMIT 10;",
    "SELECT id, AVG(avg(metric1)) OVER (PARTITION BY id, max(id), MIN(metric2)), AVG(avg(id)) OVER (PARTITION BY id, min(id), AVG(metric1)) FROM Users_f GROUP BY 1 ORDER BY 3 DESC, 2 DESC, 1 DESC;",
    "SELECT metric2, AVG(avg(metric1)) OVER (PARTITION BY metric2, max(metric2), MIN(metric2)), AVG(avg(metric2)) OVER (PARTITION BY metric2, min(metric2), AVG(metric1)) FROM Users_f GROUP BY 1 ORDER BY 3 DESC, 2 DESC, 1 DESC LIMIT 25;",
    "SELECT id, sum(avg(id)) OVER () FROM Users_f GROUP BY id ORDER BY 1 LIMIT 10;",
    "SELECT id, 1 + sum(metric1), 1 + AVG(metric2) OVER (partition by id) FROM Users_f GROUP BY id, metric2 ORDER BY 2 DESC, 1 LIMIT 5;",
    "SELECT id, avg(metric1), RANK() OVER (partition by id order by metric2) FROM Users_f GROUP BY id, metric2 ORDER BY id, metric2 DESC;",
    "SELECT id, avg(metric1), RANK() OVER (partition by id order by 1 / (1 + avg(metric1))) FROM Users_f GROUP BY id, metric2 ORDER BY id, avg(metric1) DESC;",
    "WITH cte as ( SELECT users_alias.id id, Events_f.metric2, count(*) c FROM Events_f JOIN Users_f users_alias ON users_alias.id != Events_f.id GROUP BY 1, 2 ) SELECT DISTINCT cte.metric2, cte.c, sum(cte.metric2) OVER (PARTITION BY cte.c) FROM cte JOIN Events_f et ON et.metric2 != cte.metric2 and et.metric2 != cte.c ORDER BY 1;",
    "SELECT DISTINCT rnk FROM ( SELECT Events_f.metric2, sum(users_alias.metric1) OVER (PARTITION BY users_alias.id) AS rnk FROM Events_f JOIN Users_f users_alias ON users_alias.id = Events_f.id ) sq GROUP BY 1 ORDER BY 1;",
    "SELECT id, metric2, rank() OVER my_win AS my_rank, avg(avg(event_type)) OVER my_win_2 AS avg, max(timestamp) AS mx_time, ntile(4) OVER my_win_3 AS ntile_rank_4, ntile(10) OVER my_win_3 AS ntile_rank_10, lag(event_type) OVER my_win_3 AS prev_event_type, lag(event_type, 2) OVER my_win_3 AS prev_event_type_2, lag(event_type, 3, 0) OVER my_win_3 AS prev_event_type_3_default, lead(event_type) OVER my_win_3 AS next_event_type, lead(event_type, 2) OVER my_win_3 AS next_event_type_2, lead(event_type, 3, 0) OVER my_win_3 AS next_event_type_3_default, first_value(event_type) OVER my_win_3 AS first_event_type, last_value(event_type) OVER my_win_3 AS last_event_type, nth_value(event_type, 2) OVER my_win_3 AS second_event_type, nth_value(event_type, 3) OVER my_win_3 AS third_event_type FROM Events_f GROUP BY id, metric2, event_type, timestamp WINDOW my_win AS (PARTITION BY id, max(event_type) ORDER BY count(*) DESC), my_win_2 AS (PARTITION BY id, avg(id) ORDER BY count(*) DESC), my_win_3 AS (PARTITION BY id ORDER BY timestamp) ORDER BY avg DESC, mx_time DESC, my_rank DESC, id DESC;",
]


def test_window_function_pushdown(create_pushdown_tables, pg_conn):

    for query in queries:
        # first, compare the query results
        assert_query_results_on_tables(
            query, pg_conn, ["Users_f", "Events_f"], ["Users", "Events"]
        )

        # some query have only OVER, PARTITION BY is not always required
        if "PARTITION BY" in query:
            assert_remote_query_contains_expression(
                query, "PARTITION BY", pg_conn
            ), query
        elif "OVER" in query:
            assert_remote_query_contains_expression(query, "OVER", pg_conn), query
        else:
            assert False
