import pytest
from utils_pytest import *

queries = [
    # Testing a FULL JOIN with a join result alias and specifying column aliases
    "SELECT DISTINCT j.sum_metric FROM (Users_f FULL JOIN Events_f ON Users_f.id = Events_f.id) j(sum_metric, event_type);",
    "SELECT DISTINCT j.id FROM (Users_f JOIN Events_f USING (id)) j(id);",
    "SELECT id, SUM(j.id) OVER (PARTITION BY j.id) FROM (Users_f JOIN Events_f USING (id)) AS j;",
    "SELECT DISTINCT j.* FROM (Users_f JOIN Events_f USING (id)) AS j(id, event_type);",
    "SELECT DISTINCT j.* FROM (Users_f JOIN Events_f USING (id)) AS j(id, event_type);",
    # Testing a JOIN with aliasing the join itself and using the alias in the SELECT clause
    "SELECT DISTINCT * FROM (Users_f JOIN Events_f USING (id)) AS j;",
    "SELECT DISTINCT sum(j.id) FROM (Users_f JOIN Events_f USING (id)) AS j GROUP BY id;",
    # Using an alias for a table in a simple INNER JOIN
    "SELECT DISTINCT U.* FROM Users_f U JOIN Events_f E ON U.id = E.id;",
    # Using column aliases in a SELECT statement with a JOIN
    "SELECT DISTINCT U.id AS user_id, E.id AS event_id FROM Users_f U JOIN Events_f E ON U.id = E.id;",
    # JOIN using USING to eliminate the need to qualify join column
    "SELECT DISTINCT * FROM Users_f U JOIN Events_f E USING (id);",
    # LEFT JOIN with table and column aliases
    "SELECT DISTINCT U.id AS user_id, E.id AS event_id FROM Users_f U LEFT JOIN Events_f E ON U.id = E.id;",
    # RIGHT JOIN using aliases and filtering in WHERE
    "SELECT DISTINCT (U.id + E.id) FROM Users_f U RIGHT JOIN Events_f E ON U.id = E.id WHERE E.event_type = 1;",
    # FULL OUTER JOIN using an alias for the joined tables
    "SELECT DISTINCT * FROM Users_f U FULL OUTER JOIN Events_f E ON U.id = E.id;",
    # CROSS JOIN with aliasing the resulting table
    "SELECT DISTINCT * FROM Users_f CROSS JOIN Events_f AS E;",
    # Subquery in JOIN with alias
    "SELECT DISTINCT U.id, E.metric2 FROM Users_f U JOIN (SELECT id, metric2 FROM Events_f) E ON U.id = E.id;",
    # Multiple table aliases in complex JOIN
    "SELECT DISTINCT U.id, E.id FROM Users_f U JOIN Events_f E ON U.id = E.id JOIN Events_f E2 ON E.id = E2.id;",
    # NATURAL JOIN with an alias for clarity in SELECT
    "SELECT DISTINCT U.*, E.* FROM Users_f U NATURAL JOIN Users_f E;",
    # Using alias in JOIN ON condition with multiple conditions
    "SELECT DISTINCT U.id AS uid, E.id AS eid FROM Users_f U JOIN Events_f E ON (U.id = E.id AND U.metric1 != E.metric2);",
    # Using a LEFT JOIN with a table alias and specifying columns in the join condition
    "SELECT DISTINCT j.* FROM (Users_f LEFT JOIN Events_f ON Users_f.id = Events_f.id AND Users_f.metric1 = Events_f.metric2) j(id, metric);",
    # Using a RIGHT JOIN with join condition aliases and outputting specific columns with aliases
    "SELECT DISTINCT ON (j.user_id) j.event_info FROM (Users_f RIGHT JOIN Events_f ON Users_f.id = Events_f.id) j(user_id, event_info);",
    # Utilizing CROSS JOIN with aliasing result columns explicitly
    "SELECT DISTINCT j.all_data FROM (Users_f CROSS JOIN Events_f) j(all_data);",
    # Testing NATURAL JOIN with aliasing the entire join block
    "SELECT DISTINCT j.details FROM (Users_f NATURAL JOIN Users_f u2) j(details);",
    # Combining a JOIN with a subquery and aliasing the whole expression for complex data extraction
    "SELECT j.complex_metric FROM (SELECT DISTINCT Users_f.id, Events_f.metric2 FROM Users_f JOIN Events_f ON Users_f.metric2 = Events_f.metric2) j(complex_metric);",
    # Using multiple JOINs with aliases and outputting with specific aliases for complex query structures
    "SELECT DISTINCT j.* FROM (Users_f JOIN Events_f ON Users_f.id = Events_f.id JOIN Events_f Metrics ON Events_f.metric2 = Metrics.metric2) j(id, event_type);",
    # Implementing an INNER JOIN with aliases for both the join condition and selected columns
    "SELECT DISTINCT j.unique_metric FROM (Users_f INNER JOIN Events_f ON Users_f.metric1 = Events_f.metric2) j(unique_metric);",
    # Testing FULL OUTER JOIN using column aliasing in join conditions and output
    "SELECT DISTINCT j.full_info FROM (Users_f FULL OUTER JOIN Events_f ON Users_f.float_value = Events_f.float_value) j(full_info);",
    # Testing a FULL JOIN with a join result alias and column aliases inside a subquery with an outer query performing a simple SELECT
    "SELECT DISTINCT outer_query.sum_metric FROM (SELECT j.sum_metric FROM (Users_f FULL JOIN Events_f ON Users_f.id = Events_f.id) j(sum_metric, event_type)) AS outer_query;",
    # Testing JOIN with aliasing the join itself and using the alias in an outer SELECT query
    "SELECT DISTINCT outer_query.* FROM (SELECT j.* FROM (Users_f JOIN Events_f USING (id)) AS j(id, event_type)) AS outer_query;",
    # Using a LEFT JOIN with table aliases inside a subquery and performing a COUNT in an outer query
    "SELECT DISTINCT COUNT(DISTINCT outer_query.event_type), AVG(outer_query.event_type) FROM (SELECT * FROM (Users_f LEFT JOIN Events_f ON Users_f.id = Events_f.id AND Users_f.metric1 = Events_f.metric2) j(id, metric)) AS outer_query;",
    # Using a RIGHT JOIN within a subquery and selecting from it in an outer query
    "SELECT DISTINCT outer_query.user_id, outer_query.event_info FROM (SELECT j.user_id, j.event_info FROM (Users_f RIGHT JOIN Events_f ON Users_f.id = Events_f.id) j(user_id, event_info)) AS outer_query;",
    # Utilizing CROSS JOIN within a subquery and selecting data in an outer query
    "SELECT DISTINCT outer_query.all_data FROM (SELECT j.all_data FROM (Users_f CROSS JOIN Events_f) j(all_data)) AS outer_query;",
    # Testing NATURAL JOIN within a subquery, and selecting the results in an outer query
    "SELECT DISTINCT outer_query.details FROM (SELECT j.details FROM (Users_f NATURAL JOIN Users_f u2) j(details)) AS outer_query;",
    # Combining a JOIN within a subquery with another join condition and selecting in an outer query
    "SELECT DISTINCT outer_query.complex_metric FROM (SELECT j.complex_metric FROM (SELECT Users_f.id, Events_f.metric2 FROM Users_f JOIN Events_f ON Users_f.metric2 = Events_f.metric2) j(complex_metric)) AS outer_query;",
    # Multiple nested subqueries with JOINs, combining results in an outer query
    "SELECT DISTINCT outer_query.* FROM (SELECT j.* FROM (Users_f JOIN Events_f ON Users_f.id = Events_f.id JOIN Events_f Metrics ON Events_f.metric2 = Metrics.metric2) j(id, event_type)) AS outer_query;",
    # INNER JOIN within a subquery with result aliasing, selected in an outer query
    "SELECT DISTINCT outer_query.unique_metric FROM (SELECT j.unique_metric FROM (Users_f INNER JOIN Events_f ON Users_f.metric1 = Events_f.metric2) j(unique_metric)) AS outer_query;",
    # FULL OUTER JOIN using column aliasing in join conditions within a subquery, selected in an outer query
    "SELECT DISTINCT outer_query.full_info FROM (SELECT j.full_info FROM (Users_f FULL OUTER JOIN Events_f ON Users_f.float_value = Events_f.float_value) j(full_info)) AS outer_query;",
]


# due to an incompatibility with DuckDB, we cannot pushdown
# the queries, but instead we rely on ForeignScan
def test_join_syntax(create_pushdown_tables, pg_conn):

    for query in queries:

        # We can only compare the results of the queries
        # but we cannot easily check the query plan, because
        # of https://github.com/duckdb/duckdb/discussions/12184
        assert_query_results_on_tables(
            query, pg_conn, ["Users_f", "Events_f"], ["Users", "Events"]
        )
