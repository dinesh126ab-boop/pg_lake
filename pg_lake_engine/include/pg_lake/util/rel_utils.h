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

#pragma once

#include "nodes/primnodes.h"

/* distinguish tables with "server pg_lake" vs "server pg_lake_iceberg" */
typedef enum PgLakeTableType
{
	PG_LAKE_INVALID_TABLE_TYPE,
	PG_LAKE_TABLE_TYPE,
	PG_LAKE_ICEBERG_TABLE_TYPE
}			PgLakeTableType;


typedef enum IcebergCatalogType
{
	NOT_ICEBERG_TABLE = 0,

	/* catalog='postgres' */
	POSTGRES_CATALOG = 1,

	/*
	 * catalog='rest', read_only=True Always treat like external iceberg
	 * table, read the metadata location from the external catalog and never
	 * modify.
	 */
	REST_CATALOG_READ_ONLY = 2,

	/*
	 * catalog='rest', read_only=False Treat like internal iceberg table, use
	 * all the catalog tables like lake_table.files.
	 */
	REST_CATALOG_READ_WRITE = 3,

	/*
	 * Similar to REST_CATALOG_READ_ONLY, but using an object store compatible
	 * API instead of a REST catalog server.
	 */
	OBJECT_STORE_READ_ONLY = 4,

	/*
	 * Similar to REST_CATALOG_READ_WRITE, but using an object store
	 * compatible API instead of a REST catalog server.
	 */
	OBJECT_STORE_READ_WRITE = 5
} IcebergCatalogType;

struct PgLakeTableProperties;

#define PG_LAKE_SERVER_NAME "pg_lake"
#define PG_LAKE_ICEBERG_SERVER_NAME "pg_lake_iceberg"

extern PGDLLEXPORT bool IsAnyLakeForeignTableById(Oid foreignTableId);
extern PGDLLEXPORT char *GetQualifiedRelationName(Oid relationId);
extern PGDLLEXPORT const char *PgLakeTableTypeToName(PgLakeTableType tableType);
extern PGDLLEXPORT PgLakeTableType GetPgLakeTableType(Oid foreignTableId);
extern PGDLLEXPORT char *GetPgLakeForeignServerName(Oid foreignTableId);
extern PGDLLEXPORT PgLakeTableType GetPgLakeTableTypeViaServerName(char *serverName);
extern PGDLLEXPORT bool IsPgLakeForeignTableById(Oid foreignTableId);
extern PGDLLEXPORT bool IsPgLakeIcebergForeignTableById(Oid foreignTableId);
extern PGDLLEXPORT bool IsPgLakeServerName(const char *serverName);
extern PGDLLEXPORT bool IsAnyWritableLakeTable(Oid foreignTableId);
extern PGDLLEXPORT bool IsPgLakeIcebergServerName(const char *serverName);
extern PGDLLEXPORT char *GetWritableTableLocation(Oid relationId, char **queryArguments);
extern PGDLLEXPORT void EnsureTableOwner(Oid relationId);
extern PGDLLEXPORT struct PgLakeTableProperties GetPgLakeTableProperties(Oid relationId);
extern PGDLLEXPORT bool IsInternalOrExternalIcebergTable(struct PgLakeTableProperties properties);

/* range var help */
extern PGDLLEXPORT List *MakeNameListFromRangeVar(const RangeVar *rel);
