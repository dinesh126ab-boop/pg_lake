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

#include "utils/date.h"

PG_FUNCTION_INFO_V1(pg_lake_to_date);


/*
 * pg_lake_to_date implements pg_catalog.to_date, which converts a number of days since
 * epoch into a date.
 *
 * to_date is meant to deal with the common representation of dates in Parquet (days
 * since epoch stored in int32). We use double precision for convenience and to match
 * match the signature of to_timestamp(double precision), which exists in both PG and
 * DuckDB.
 */
Datum
pg_lake_to_date(PG_FUNCTION_ARGS)
{
	double		daysSinceEpoch = PG_GETARG_FLOAT8(0);

	/*
	 * Postgres stores dates as as days since 2000-01-01, and there were 10957
	 * days between 1970-01-01 and 2000-01-01.
	 */
	DateADT		date = (DateADT) daysSinceEpoch - 10957;

	PG_RETURN_DATEADT(date);
}
