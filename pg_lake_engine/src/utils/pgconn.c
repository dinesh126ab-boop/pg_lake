/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "pg_lake/util/pgconn.h"
#include "storage/latch.h"
#include "utils/wait_event.h"

static PGresult *WaitForLastPGResult(PGconn *conn);
static PGresult *WaitForPGResult(PGconn *conn);

/*
 * Run a query on the connection, return a result, properly handling
 * interrupts
 */
PGresult *
RunPGQuery(PGconn *conn, char *query)
{
	int			sentQuery = PQsendQuery(conn, query);

	if (sentQuery == 0)
		ereport(ERROR, (errmsg("lost connection to remote server")));

	return WaitForLastPGResult(conn);
}


/*
 * Handle processing all results and returns the last one.
 */
static PGresult *
WaitForLastPGResult(PGconn *conn)
{
	PGresult   *volatile last_res = NULL;

	/* In what follows, do not leak any PGresults on an error. */
	PG_TRY();
	{
		for (;;)
		{
			PGresult   *res = WaitForPGResult(conn);

			if (res == NULL)
				break;			/* query is complete */

			PQclear(last_res);
			last_res = res;
		}
	}
	PG_CATCH();
	{
		PQclear(last_res);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return last_res;
}


/*
 * This function handles the necessary bits to properly handle interrupts, etc.
 */

static PGresult *
WaitForPGResult(PGconn *conn)
{
	while (PQisBusy(conn))
	{
		int			wc;

		/* Sleep until there's something to do */
		wc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_SOCKET_READABLE |
							   WL_EXIT_ON_PM_DEATH,
							   PQsocket(conn),
							   -1L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (wc & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(conn))
			{
				PQfinish(conn);
				ereport(ERROR, (errmsg("lost connection to query engine")));
			}
		}
	}

	return PQgetResult(conn);
}
