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

#include "pg_lake/planner/pushdown_utils.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "utils/fmgroids.h"


static bool HasSortGroupClauseListWithUnnest(Query *query, List *clauseList);
static bool HasUnnestWalker(Node *node, void *context);


/*
 * HasGroupByWithUnnest determines whether the given query has unnest
 * in the GROUP BY clause which is allowed by PostgreSQL, but not
 * by DuckDB.
 */
bool
HasGroupByWithUnnest(Query *query)
{
	return HasSortGroupClauseListWithUnnest(query, query->groupClause);
}


/*
 * HasWindowFunctionWithUnnest determines whether the given query has unnest
 * in a window function.
 */
bool
HasWindowFunctionWithUnnest(Query *query)
{
	ListCell   *windowClauseCell = NULL;

	foreach(windowClauseCell, query->windowClause)
	{
		WindowClause *windowClause = lfirst(windowClauseCell);

		if (HasSortGroupClauseListWithUnnest(query, windowClause->partitionClause))
			return true;

		if (HasSortGroupClauseListWithUnnest(query, windowClause->orderClause))
			return true;
	}

	return false;
}


/*
 * HasSortGroupClauseListWithUnnest checks whether a given list of
 * SortGroupClause contains unnest.
 */
static bool
HasSortGroupClauseListWithUnnest(Query *query, List *clauseList)
{
	ListCell   *groupClauseCell = NULL;

	foreach(groupClauseCell, clauseList)
	{
		SortGroupClause *groupClause = (SortGroupClause *) lfirst(groupClauseCell);

		TargetEntry *targetEntry = get_sortgroupref_tle(groupClause->tleSortGroupRef,
														query->targetList);

		if (HasUnnest((Node *) targetEntry->expr, query->rtable))
			return true;
	}

	return false;
}


/*
 * HasUnnest returns whether an expression contains an unnest call.
 */
bool
HasUnnest(Node *node, void *context)
{
	return HasUnnestWalker(node, context);
}


/*
 * HasUnnestWalker recursively checks whether an expression contains an unnest call.
 */
static bool
HasUnnestWalker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	/* unnest in sublinks are allowed */
	if (IsA(node, Query))
	{
		return false;
	}

	if (IsA(node, FuncExpr))
	{
		FuncExpr   *funcExpr = (FuncExpr *) node;

		if (funcExpr->funcid == F_UNNEST_ANYARRAY)
			return true;
	}

#if PG_VERSION_NUM >= 180000
	if (IsA(node, Var))
	{
		List	   *rtable = (List *) context;

		Var		   *v = (Var *) node;

		if (v->varlevelsup != 0)
			return false;		/* unnest in sublinks are allowed */

		RangeTblEntry *rte = rt_fetch(v->varno, rtable);

		if (rte->rtekind == RTE_GROUP)
		{
			List	   *groupExprs = rte->groupexprs;

			ListCell   *groupExprCell = NULL;

			foreach(groupExprCell, groupExprs)
			{
				Expr	   *groupExpr = (Expr *) lfirst(groupExprCell);

				if (HasUnnest((Node *) groupExpr, rtable))
					return true;
			}
		}
	}
#endif

	return expression_tree_walker(node, HasUnnestWalker, context);
}
