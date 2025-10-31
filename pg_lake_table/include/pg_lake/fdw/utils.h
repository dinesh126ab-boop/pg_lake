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
 * utils.h
 *		  Utils for fdw
 *
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LAKE_FDW_UTILS_H
#define PG_LAKE_FDW_UTILS_H

#include "postgres.h"

#include "nodes/parsenodes.h"
#include "pg_lake/copy/copy_format.h"

extern PGDLLEXPORT bool IsAnyLakeForeignTable(RangeTblEntry *rte);
extern PGDLLEXPORT char *GetForeignTablePath(Oid foreignTableId);
extern PGDLLEXPORT CopyDataFormat GetForeignTableFormat(Oid foreignTableId);
extern PGDLLEXPORT bool IsWritablePgLakeTable(Oid relationId);
extern PGDLLEXPORT void ErrorIfTypeUnsupportedForIcebergTables(Oid typeOid, int32 typmod, char *columnName);
extern PGDLLEXPORT void ErrorIfTypeUnsupportedNumericForIcebergTables(int32 typmod, char *columnName);

#endif							/* PG_LAKE_FDW_UTILS_H */
