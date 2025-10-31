# pg_lake_benchmark

Similar experience with [duckdb's tpch extension](https://duckdb.org/docs/extensions/tpch.html).
This extension lets you run various benchmarks on pg lake tables. Currently, you are able to run following benchmarks:
- [clickbench](https://github.com/ClickHouse/ClickBench)
- [tpch](https://duckdb.org/docs/stable/extensions/tpch.html)
- [tpcds](https://duckdb.org/docs/stable/extensions/tpcds.html)

## How to run clickbench benchmark?
1. Run `lake_clickbench.create()` to create the bench tables. By default we perform bench on iceberg tables but you can choose to do it on pg_lake or heap tables via `lake_clickbench.create(table_type => 'pg_lake')` or `lake_clickbench.create(table_type => 'heap')`.
2. Run `lake_clickbench.load()` to load data into the bench tables. (might take looong, loads ~15GB s3 parquet file). 
3. There are total of 43 queries in the benchmark. You can see them via `lake_clickbench.show_query(query_id int)`.
4. Run a query via `lake_clickbench.run_query(query_id int)`. This will perform and discard the query result. DO NOT forget to enable `\timing` to see the total query time. To simply run all queries, run `SELECT lake_clickbench.run_query(id) FROM lake_clickbench.queries;`.

## How to run tpch benchmark?
1. Run `lake_tpch.gen(location => 's3://...')` to create and populate the bench tables with generated data. By default we perform bench on iceberg tables but you can choose to do it on pg_lake or heap tables via `lake_tpch.gen(location => 's3://...', table_type => 'pg_lake')` or `lake_tpch.gen(location => 's3://...', table_type => 'heap')`. The default scale_factor is 1.0. You can set it as in `lake_tpch.gen(location => 's3://...', scale_factor => 0.01)`. For large scales, data generation might undergo OOM, so you can set the iteraration to a higher value to prevent memory issues as in `lake_tpch.gen(location => 's3://...', iteration_count => 10)`.
2. There are total of 22 queries in the benchmark. You can see them via `lake_tpch.queries()`.
3. Run a query via `lake_tpch.run_query(query_id int)`. This will perform and discard the query result. DO NOT forget to enable `\timing` to see the total query time. To simply run all queries, run `SELECT lake_tpch.run_query(query_nr) FROM lake_tpch.queries();`.

> [!WARNING] We have the same [limitation]('https://duckdb.org/docs/stable/extensions/tpch.html#limitations') as duckdb's tpch extension, that we run queries with fixed parameters.

## How to run tpcds benchmark?
1. Run `lake_tpcds.gen(location => 's3://...')` to create and populate the bench tables with generated data. By default we perform bench on iceberg tables but you can choose to do it on pg_lake or heap tables via `lake_tpcds.gen(location => 's3://...', table_type => 'pg_lake')` or `lake_tpcds.gen(location => 's3://...', table_type => 'heap')`. The default scale_factor is 1.0. You can set it as in `lake_tpcds.gen(location => 's3://...', scale_factor => 0.01)`.
2. There are total of 99 queries in the benchmark. You can see them via `lake_tpcds.queries()`.
3. Run a query via `lake_tpcds.run_query(query_id int)`. This will perform and discard the query result. DO NOT forget to enable `\timing` to see the total query time. To simply run all queries, run `SELECT lake_tpcds.run_query(query_nr) FROM lake_tpcds.queries();`.

> [!WARNING] We have the same [limitation]('https://duckdb.org/docs/stable/extensions/tpcds.html#limitations') as duckdb's tpch extension, that we run queries with fixed parameters.
