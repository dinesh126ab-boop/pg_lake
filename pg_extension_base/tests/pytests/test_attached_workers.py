import pytest
import psycopg2
import time
from utils_pytest import *
import server_params


def test_simple(superuser_conn, pg_extension_base):
    # Test simple query
    result = run_query(
        "SELECT * FROM extension_base.run_attached($$SELECT 1$$)", superuser_conn
    )
    assert result[0]["command_tag"] == "SELECT 1"


def test_simple_in_db(superuser_conn, pg_extension_base):

    # first, make sure the role is superuser
    result = run_query(
        f"SELECT rolsuper FROM pg_roles where rolname = current_user;", superuser_conn
    )
    assert result[0][0] == True

    # Test simple query on the db/user
    result = run_query(
        f"SELECT * FROM extension_base.run_attached('SELECT 1', '{server_params.PG_DATABASE}')",
        superuser_conn,
    )
    assert result[0]["command_tag"] == "SELECT 1"


def test_non_transactional(superuser_conn, pg_extension_base):
    # Test simple query
    result = run_query(
        "SELECT * FROM extension_base.run_attached($$VACUUM$$)", superuser_conn
    )
    assert result[0]["command_tag"] == "VACUUM"


def test_write(superuser_conn, pg_extension_base):
    run_command("CREATE TABLE test_write (x int, y int)", superuser_conn)
    superuser_conn.commit()

    # Start doing something in the current transaction
    result = run_query("SELECT count(*) FROM test_write", superuser_conn)
    assert result[0]["count"] == 0

    # Test write in subtransaction
    run_command(
        "SELECT * FROM extension_base.run_attached($$INSERT INTO test_write VALUES (1,2)$$)",
        superuser_conn,
    )

    # Confirm that we can immediately see the write
    result = run_query("SELECT count(*) FROM test_write", superuser_conn)
    assert result[0]["count"] == 1

    run_command("DROP TABLE test_write", superuser_conn)
    superuser_conn.rollback()


def test_error_in_worker(superuser_conn, pg_extension_base):
    # Test error handling
    error = run_command(
        "SELECT * FROM extension_base.run_attached($$SELECT 1/0$$)",
        superuser_conn,
        raise_error=False,
    )
    assert "division" in error

    superuser_conn.rollback()


def test_plpgsql_error(superuser_conn, pg_extension_base):
    error = run_command(
        """
        DO $$
        BEGIN
            PERFORM FROM extension_base.run_attached('SELECT 1/0');
        END$$ LANGUAGE plpgsql;
    """,
        superuser_conn,
        raise_error=False,
    )
    assert "division" in error

    superuser_conn.rollback()


def test_parent_cancellation(superuser_conn, pg_extension_base):
    error = run_command(
        """SELECT 8765
						   FROM extension_base.run_attached($$
						       select pg_cancel_backend(pid), pg_sleep(10) from pg_stat_activity where query like 'SELECT 8765%'
						   $$);
	""",
        superuser_conn,
        raise_error=False,
    )
    assert "canceling" in error

    superuser_conn.rollback()

    result = run_query(
        """
		select count(*) from pg_stat_activity where backend_type = 'pg_extension_base attached worker'
	""",
        superuser_conn,
    )
    assert result[0]["count"] == 0

    superuser_conn.rollback()


def test_nologin_role(superuser_conn, pg_extension_base):
    run_command("CREATE ROLE no_login nologin superuser ", superuser_conn)
    current_user = run_query("SELECT user", superuser_conn)[0][0]
    run_command(f"GRANT no_login TO {current_user}", superuser_conn)
    superuser_conn.commit()

    error = run_command(
        """
    set role no_login;
	select * FROM extension_base.run_attached($$SELECT 1$$);
	""",
        superuser_conn,
        raise_error=False,
    )

    # if we are pg16, we expect this to error; if we are not, then we expect this to succeed and return the results
    if superuser_conn.server_version < 170000:
        assert "NOLOGIN" in str(error)
    else:
        assert error is None

    superuser_conn.rollback()

    # cleanup
    run_command("DROP ROLE no_login", superuser_conn)
    superuser_conn.commit()
