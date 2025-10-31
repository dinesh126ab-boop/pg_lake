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
 * Functions for handling permissions.
 */
#include "postgres.h"
#include "miscadmin.h"

#include "pg_lake/permissions/roles.h"
#include "utils/acl.h"

#define lake_read_ROLE_NAME "lake_read"
#define lake_write_ROLE_NAME "lake_write"


/*
 * PgLakeReadRoleId returns the OID of the global read role,
 * if it exists.
 */
Oid
PgLakeReadRoleId(void)
{
	bool		missingOK = true;

	return get_role_oid(lake_read_ROLE_NAME, missingOK);
}


/*
 * PgLakeWriteRoleId returns the OID of the global write role,
 * if it exists.
 */
Oid
PgLakeWriteRoleId(void)
{
	bool		missingOK = true;

	return get_role_oid(lake_write_ROLE_NAME, missingOK);
}


/*
 * CheckURLReadAccess returns an error if the current user does not
 * have permission to read from URLs.
 */
void
CheckURLReadAccess(void)
{
	Oid			userId = GetUserId();
	Oid			readRoleId = PgLakeReadRoleId();

	/*
	 * If the role does not exist, only superuser can read from URLs.
	 * Otherwise, the user needs to have the read access role.
	 */
	if (!superuser() &&
		(readRoleId == InvalidOid || !has_privs_of_role(userId, readRoleId)))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied to read from URL"),
						errhint("grant %s to %s to enable reading from URLs",
								GetUserNameFromId(readRoleId, false),
								GetUserNameFromId(userId, false))));
	}
}


/*
 * CheckURLWriteAccess returns an error if the current user does not
 * have permission to write to URLs.
 */
void
CheckURLWriteAccess(void)
{
	Oid			userId = GetUserId();
	Oid			writeRoleId = PgLakeWriteRoleId();

	/*
	 * If the role does not exist, only superuser can write to URLs.
	 * Otherwise, the user needs to have the write access role.
	 */
	if (!superuser() &&
		(writeRoleId == InvalidOid || !has_privs_of_role(userId, writeRoleId)))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied to write to URL"),
						errhint("grant %s to %s to enable writing to URLs",
								GetUserNameFromId(writeRoleId, false),
								GetUserNameFromId(userId, false))));
	}
}
