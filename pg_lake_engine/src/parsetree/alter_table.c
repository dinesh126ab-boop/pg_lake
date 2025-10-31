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
#include "miscadmin.h"

#include "commands/tablecmds.h"
#include "pg_lake/parsetree/alter_table.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"


/*
 * This utility constructs and returns a grant or revoke statement for our
 * create privilege for the given userid.
 */
AlterTableStmt *
GetAlterTableOwnerStmt(Oid relationId, Oid ownerId)
{
	/* Get role name */
	bool		missingOk = false;
	char	   *rolename = GetUserNameFromId(ownerId, missingOk);

	/* Build RoleSpec */
	RoleSpec   *newowner = makeNode(RoleSpec);

	newowner->roletype = ROLESPEC_CSTRING;
	newowner->rolename = pstrdup(rolename);
	newowner->location = -1;

	/* Build RangeVar for table name */
	char	   *relname = get_rel_name(relationId);
	char	   *relnamespace = get_namespace_name(get_rel_namespace(relationId));

	RangeVar   *relationName = makeRangeVar(relnamespace, relname, -1);

	/* Alter owner */
	AlterTableCmd *cmd = makeNode(AlterTableCmd);

	cmd->subtype = AT_ChangeOwner;
	cmd->newowner = newowner;

	/* Create AlterTableStmt */
	AlterTableStmt *stmt = makeNode(AlterTableStmt);

	stmt->relation = relationName;
	stmt->objtype = OBJECT_TABLE;
	stmt->cmds = list_make1(cmd);
	stmt->missing_ok = false;

	return stmt;
}
