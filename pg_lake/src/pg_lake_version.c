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
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(cdw_version);
PG_FUNCTION_INFO_V1(pg_lake_version);

Datum
pg_lake_version(PG_FUNCTION_ARGS)
{
	/* PG_LAKE_GIT_VERSION is passed in as a compiler flag during builds */
	PG_RETURN_TEXT_P(cstring_to_text(PG_LAKE_GIT_VERSION));
}


Datum
cdw_version(PG_FUNCTION_ARGS)
{
	elog(NOTICE, "cdw_version is deprecated, use pg_lake_version instead");

	return pg_lake_version(fcinfo);
}
