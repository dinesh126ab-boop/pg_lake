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

#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "pg_lake/json/json_reader.h"

#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"


PG_FUNCTION_INFO_V1(pg_lake_copy_file);


/*
* pg_lake_copy_file is a test function that reads the content of a local json
* file and copies it to an S3 bucket.
*/
Datum
pg_lake_copy_file(PG_FUNCTION_ARGS)
{
	char	   *localFilePath = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *s3Uri = text_to_cstring(PG_GETARG_TEXT_P(1));

	CopyLocalFileToS3(localFilePath, s3Uri);

	PG_RETURN_VOID();
}
