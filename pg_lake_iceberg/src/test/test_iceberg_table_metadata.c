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
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/pgduck/write_data.h"

#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(reserialize_iceberg_table_metadata);



/*
* reserialize_iceberg_table_metadata is a test function that reads the metadata
* of an Iceberg table from a file and then reserializes it to a JSONB object.
*/
Datum
reserialize_iceberg_table_metadata(PG_FUNCTION_ARGS)
{
	char	   *metadataUri = text_to_cstring(PG_GETARG_TEXT_P(0));

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataUri);
	char	   *serializedMetadataText = WriteIcebergTableMetadataToJson(metadata);

	PG_RETURN_TEXT_P(cstring_to_text(serializedMetadataText));
}
