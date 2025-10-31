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

/*
 * pg extension base test extension.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pg_extension_base_test_common.h"

PG_MODULE_MAGIC;

/* function declarations */
void		_PG_init(void);

/* UDF declarations */
PG_FUNCTION_INFO_V1(pg_extension_base_test_ext3_initialize_extension);

/*
 * _PG_init is the entry-point for pg_extension_base_test_ext3 which is called
 * by pg_extension_base.
 */
void
_PG_init(void)
{
	/*
	 * CREATE EXTENSION will error unless we were loaded via
	 * shared_preload_libraries.
	 */
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg("pg_extension_base_test_ext3 can only be loaded via shared_preload_libraries"),
						errhint("Add pg_extension_base to shared_preload_libraries configuration "
								"variable in postgresql.conf")));
	}

	if (!IsCommonAvailable)
	{
		ereport(ERROR, (errmsg("pg_extension_base_test_ext3 depends on pg_extension_base_test_common")));
	}
}

/*
 * pg_extension_base_test_ext3_initialize_extension is called when a
 * CREATE EXTENSION pg_extension_base_test_ext3 happens.
 *
 * We use this primarily to confirm to trigger the
 * process_shared_preload_libraries_in_progress check in _PG_init.
 */
Datum
pg_extension_base_test_ext3_initialize_extension(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}
