import pytest
import subprocess

from utils_pytest import *


def test_dbt_materialized_table(installcheck, s3, superuser_conn, extension):
    if installcheck:
        return

    # check if materialized table can be recreated multiple times
    dbt_run_helper(["my_first_dbt_model"], repeat=3)

    # check if the materialized table exists in iceberg catalog
    query = "SELECT COUNT(*) = 1 FROM lake_iceberg.tables WHERE table_name = 'my_first_dbt_model'"
    result = run_query(query, superuser_conn)
    assert result[0][0] == True

    # check table rows
    query = "SELECT id FROM my_first_dbt_model ORDER BY id"
    result = run_query(query, superuser_conn)
    assert result == [[1], [2]]

    # drop tables
    run_command("DROP TABLE my_first_dbt_model", superuser_conn)
    superuser_conn.commit()


def test_dbt_materialized_incremental(installcheck, s3, superuser_conn, extension):
    if installcheck:
        return

    # check if incremental table can be recreated multiple times
    dbt_run_helper(["my_first_dbt_model", "my_second_dbt_model"], repeat=3)

    # check if the incremental table exists in iceberg catalog
    query = "SELECT COUNT(*) = 1 FROM lake_iceberg.tables WHERE table_name = 'my_second_dbt_model'"
    result = run_query(query, superuser_conn)
    assert result[0][0] == True

    # check table rows
    result = run_query("SELECT id FROM my_second_dbt_model ORDER BY id", superuser_conn)
    assert result == [[1], [2]]

    # insert into materialized table. for dbt to see the new rows, we commit the transaction
    run_command(
        "INSERT INTO my_first_dbt_model VALUES (3, now()), (4, now())", superuser_conn
    )
    superuser_conn.commit()

    # ingest new rows into incremental table
    dbt_run_helper(["my_second_dbt_model"])

    # check table rows
    result = run_query("SELECT id FROM my_second_dbt_model ORDER BY id", superuser_conn)
    assert result == [[1], [2], [3], [4]]

    # drop tables
    run_command("DROP TABLE my_first_dbt_model, my_second_dbt_model", superuser_conn)
    superuser_conn.commit()


def dbt_run_helper(models, repeat=1):
    try:
        for _ in range(repeat):
            subprocess.run(
                [
                    "dbt",
                    "run",
                    "--project-dir",
                    "tests/dbt_pglake_tests",
                    "--profiles-dir",
                    "tests/dbt_pglake_tests",
                    "--log-path",
                    "tests/dbt_pglake_tests/logs",
                    "--select",
                    " ".join(models),
                ],
                check=True,
            )
    except subprocess.CalledProcessError as e:
        print("dbt run command failed with return code:", e.returncode)
        raise e
