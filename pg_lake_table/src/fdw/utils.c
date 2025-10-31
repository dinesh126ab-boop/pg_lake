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
 * transform_query_to_duckdb.c
 *		  Apply any transformations to the query before sending it to duckdb.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "pg_lake/parsetree/options.h"
#include "foreign/foreign.h"
#include "nodes/parsenodes.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_func.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include "pg_lake/fdw/utils.h"
#include "pg_lake/util/rel_utils.h"


bool
IsAnyLakeForeignTable(RangeTblEntry *rte)
{
	if (rte->rtekind != RTE_RELATION ||
		rte->relkind != RELKIND_FOREIGN_TABLE)
	{
		return false;
	}

	return IsAnyLakeForeignTableById(rte->relid);
}


/*
* GetForeignTablePath - get the path option for the foreign table.
*/
char *
GetForeignTablePath(Oid foreignTableId)
{
	ForeignTable *fTable = GetForeignTable(foreignTableId);
	ListCell   *cell;

	foreach(cell, fTable->options)
	{
		DefElem    *defel = (DefElem *) lfirst(cell);

		if (strcmp(defel->defname, "path") == 0)
		{
			return defGetString(defel);
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("path option not found for foreign table %u", foreignTableId)));
}


/*
* GetForeignTableFormat - get the underlying file format for the foreign table.
*/
CopyDataFormat
GetForeignTableFormat(Oid foreignTableId)
{
	PgLakeTableType tableType = GetPgLakeTableType(foreignTableId);

	if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE)
	{
		/* iceberg tables are always parquet */
		return DATA_FORMAT_PARQUET;
	}

	ForeignTable *fTable = GetForeignTable(foreignTableId);
	ListCell   *cell;

	foreach(cell, fTable->options)
	{
		DefElem    *defel = (DefElem *) lfirst(cell);

		if (strcmp(defel->defname, "format") == 0)
		{
			return NameToCopyDataFormat(defGetString(defel));
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("format option not found for foreign table %u", foreignTableId)));
}


/*
 * IsWritablePgLakeTable determines whether the given
 * relation ID belongs to a writable pg_lake table.
 */
bool
IsWritablePgLakeTable(Oid relationId)
{
	if (!IsPgLakeForeignTableById(relationId))
	{
		return false;
	}

	ForeignTable *table = GetForeignTable(relationId);
	DefElem    *writableOption = GetOption(table->options, "writable");

	return writableOption != NULL && defGetBoolean(writableOption);
}
