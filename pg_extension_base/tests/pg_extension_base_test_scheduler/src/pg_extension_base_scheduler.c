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

/*-------------------------------------------------------------------------
 *
 * PG extension base extension
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "pg_extension_base/base_workers.h"
#include "utils/guc.h"

#define GUC_STANDARD 0

PG_MODULE_MAGIC;

/* function declarations */
void		_PG_init(void);

/* UDF implementations */
PG_FUNCTION_INFO_V1(pg_extension_base_test_scheduler_main_worker);

/* settings */
static bool EnablePgExtensionBaseScheduler;


/*
 * _PG_init is the entry-point for pg_extension_base_scheduler.
 */
void
_PG_init(void)
{
	DefineCustomBoolVariable(
							 "pg_extension_base_scheduler.enable",
							 gettext_noop("Enables the PG Extension Base Scheduler (default on)"),
							 NULL,
							 &EnablePgExtensionBaseScheduler,
							 true,
							 PGC_POSTMASTER,
							 GUC_STANDARD,
							 NULL, NULL, NULL);
}


/*
 * pg_extension_base_test_scheduler_main_worker is the main entry-point for the base worker
 * that schedules periodic or otherwise persistent jobs.
 */
Datum
pg_extension_base_test_scheduler_main_worker(PG_FUNCTION_ARGS)
{
	int32		workerId = PG_GETARG_INT32(0);

	while (true)
	{
		elog(LOG, "pg_extension_base_test_scheduler_main_worker %d in database %d", workerId, MyDatabaseId);
		LightSleep(30000);
	}
	PG_RETURN_VOID();
}
