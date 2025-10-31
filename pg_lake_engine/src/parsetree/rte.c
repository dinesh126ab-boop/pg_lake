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

#include "pg_lake/parsetree/rte.h"
#include "nodes/parsenodes.h"


/*
 * IsFunctionRTE returns whether the given RTE is a function call
 * with the given function ID.
 */
bool
IsFunctionRTE(RangeTblEntry *rte, Oid functionId)
{
	if (rte->rtekind != RTE_FUNCTION)
		return false;

	if (list_length(rte->functions) != 1)
		return false;

	RangeTblFunction *tableFunction = linitial(rte->functions);
	FuncExpr   *functionExpr = (FuncExpr *) tableFunction->funcexpr;

	if (functionExpr->funcid != functionId)
		return false;

	return true;
}
