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

#include "pg_lake/iceberg/utils.h"

/*
 * Convert the IcebergManifestContentType to a string.
 */
const char *
IcebergManifestContentTypeToName(IcebergManifestContentType contentType)
{
	switch (contentType)
	{
		case ICEBERG_MANIFEST_FILE_CONTENT_DATA:
			/* this would normally be 'DATA' but we follow DuckDB  */
			return "EXISTING";
		case ICEBERG_MANIFEST_FILE_CONTENT_DELETES:
			return "DELETES";
		default:
			return "UNKNOWN";
	}
}

/*
 * Convert the IcebergManifestEntryStatus to a string.
 */
const char *
IcebergManifestEntryStatusToName(IcebergManifestEntryStatus status)
{
	switch (status)
	{
		case ICEBERG_MANIFEST_ENTRY_STATUS_EXISTING:
			return "EXISTING";
		case ICEBERG_MANIFEST_ENTRY_STATUS_ADDED:
			return "ADDED";
		case ICEBERG_MANIFEST_ENTRY_STATUS_DELETED:
			return "DELETED";
		default:
			return "UNKNOWN";
	}
}

/*
 * Convert the IcebergDataFileContentType to a string.
 */
const char *
IcebergDataFileContentTypeToName(IcebergDataFileContentType contentType)
{
	switch (contentType)
	{
		case ICEBERG_DATA_FILE_CONTENT_DATA:
			return "DATA";
		case ICEBERG_DATA_FILE_CONTENT_POSITION_DELETES:
			return "POSITION_DELETES";
		case ICEBERG_DATA_FILE_CONTENT_EQUALITY_DELETES:
			return "EQUALITY_DELETES";
		default:
			return "UNKNOWN";
	}
}
