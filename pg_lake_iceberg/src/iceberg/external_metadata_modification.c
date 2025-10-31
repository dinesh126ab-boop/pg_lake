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
#include "fmgr.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/acl.h"

#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"

PG_FUNCTION_INFO_V1(external_catalog_modification);


/*
* external_catalog_modification is an INSTEAD OF trigger that currently
* aims to prevent external catalog modifications to the iceberg catalog,
* namely pg_catalog.iceberg_tables view.
*
* Note that external tools such as spark or pyiceberg knows only about
* pg_catalog.iceberg_tables. The underlying pg_lake_iceberg.tables is not
* known to the external world.
*
* In the future, we might want to use this trigger to allow external tools
* to modify the iceberg catalog, and reflect those changes in the pg_lake
* catalog tables such as lake_table.data_files.
*/
Datum
external_catalog_modification(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	HeapTuple	rettuple;

	/* make sure it's called as a trigger at all */
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
				 errmsg("must be called as a trigger")));
	}

	/* make sure called as INSTEAD OF as we do it for a VIEW */
	if (!TRIGGER_FIRED_INSTEAD(trigdata->tg_event))
	{
		ereport(ERROR,
				(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
				 errmsg("must be called as an INSTEAD OF trigger")));
	}

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		rettuple = trigdata->tg_newtuple;
	else
		rettuple = trigdata->tg_trigtuple;

	bool		isnull = false;
	Datum		catalogNameDatum = heap_getattr(rettuple, 1, trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "catalog_name cannot be NULL");

	Datum		namespaceDatum = heap_getattr(rettuple, 2, trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "table_namespace cannot be NULL");
	Datum		tableNameDatum = heap_getattr(rettuple, 3, trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "table_name cannot be NULL");

	bool		metadataLocationIsNull = false;
	Datum		metadataLocationDatum = heap_getattr(rettuple, 4, trigdata->tg_relation->rd_att, &metadataLocationIsNull);
	bool		prevMetadataLocationIsNull = false;
	Datum		prevMetadataLocationDatum = heap_getattr(rettuple, 5, trigdata->tg_relation->rd_att, &prevMetadataLocationIsNull);

	char	   *catalogName = TextDatumGetCString(catalogNameDatum);
	char	   *namespaceName = TextDatumGetCString(namespaceDatum);
	char	   *tableName = TextDatumGetCString(tableNameDatum);
	char	   *metadataLocation =
		metadataLocationIsNull ? NULL : TextDatumGetCString(metadataLocationDatum);
	char	   *prevMetadataLocation =
		prevMetadataLocationIsNull ? NULL : TextDatumGetCString(prevMetadataLocationDatum);

	char	   *databaseName = get_database_name(MyDatabaseId);

	if (strcmp(catalogName, databaseName) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("writes to the %s catalog are currently only supported via pg_lake_iceberg tables",
						databaseName)));
	}

	/*
	 * Postgres only allows INSTEAD OF triggers on views. We are using this
	 * trigger to prevent external tools from modifying the iceberg catalog.
	 * But given that we use INSTEAD OF trigger on a view, we still need to
	 * handle the INSERT, UPDATE, DELETE operations on the base table.
	 */
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		UpdateExternalCatalogMetadataLocation(catalogName, namespaceName, tableName, metadataLocation, prevMetadataLocation);
	}
	else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
	{
		InsertExternalIcebergCatalogTable(catalogName, namespaceName, tableName, metadataLocation);
	}
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		DeleteExternalIcebergCatalogTable(catalogName, namespaceName, tableName);
	}
	else
	{
		/* no other command is supported on view triggers */
		pg_unreachable();
	}

	return PointerGetDatum(rettuple);
}
