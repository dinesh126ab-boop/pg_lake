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
#include "libpq-fe.h"
#include "miscadmin.h"

#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "pg_lake/util/rel_utils.h"
#include "utils/builtins.h"

static char *CopyLocalFileToS3Command(char *localFileUri, char *s3Uri);


/*
* CopyLocalFileToS3WithCleanupOnAbort similar to CopyLocalFileToS3 but
* it inserts the file path to the IN_PROGRESS_FILES_TABLE before copying
* the file to S3.
*/
void
CopyLocalFileToS3WithCleanupOnAbort(char *localFilePath, char *s3Uri)
{
	InsertInProgressFileRecord(s3Uri);

	CopyLocalFileToS3(localFilePath, s3Uri);
}

/*
* CopyLocalManifestFileToS3WithCleanupOnAbort similar to CopyLocalFileToS3WithCleanupOnAbort
* but we defer the deletion of the IN_PROGRESS_FILES_TABLE record for manifests until snapshot
* creation time.
*/
void
CopyLocalManifestFileToS3WithCleanupOnAbort(char *localFilePath, char *s3Uri)
{
	bool		isPrefix = false;
	bool		deferDeletion = true;

	InsertInProgressFileRecordExtended(s3Uri, isPrefix, deferDeletion);

	CopyLocalFileToS3(localFilePath, s3Uri);
}

/*
* CopyLocalFileToS3 copies the content of a local
* file to an S3 bucket.
*/
void
CopyLocalFileToS3(char *localFilePath, char *s3Uri)
{
	ExecuteCommandInPGDuck(CopyLocalFileToS3Command(localFilePath, s3Uri));
}


/*
* CopyLocalFileToS3Command returns the SQL command to copy
* the content of a local JSON file to an S3 bucket.
*/
static char *
CopyLocalFileToS3Command(char *localFileUri, char *s3Uri)
{
	StringInfoData command;

	initStringInfo(&command);

	appendStringInfo(&command, "SELECT * FROM pg_lake_copy_file(%s,%s);",
					 quote_literal_cstr(localFileUri), quote_literal_cstr(s3Uri));

	return command.data;
}
