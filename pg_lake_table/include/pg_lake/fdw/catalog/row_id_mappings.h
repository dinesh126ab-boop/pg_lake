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

#include "postgres.h"

#include "pg_lake/data_file/data_files.h"

/*
 * Row Id mapping is a technique for handling the tracking of rowids in an
 * abstract table (for a single relation) and turning this into distinct file
 * number and row number information so we can track where a specific row in a
 * relation actually lives.
 *
 * This is used for both logical replication as well as being a basis for
 * indexes, as it provides an infrastructure to detach the logical tuple
 * identified by a PK from its actual on-disk storage/representation.  This
 * also provides a way to synchronize changes with file compaction without
 * affecting logical replication's replay tables.
 *
 * Row mappings are stored in the lake_table.row_id_mapping
 * catalog.
 *
 * Some properties/invariants, some of which are enforced by the database:
 *
 * - rowids are unique per table; this allows us to track a specific rowid to
 *   a file number and row number within this file. As a result, row ID ranges
 *   for a given table must never overlap.
 *
 * - not all tables need be row-mapped, but iceberg tables involved as logical
 *   replication targets will.
 *
 * - the sum or the row ID range sizes for a file must match the row_count for
 *   that file in lake_table.files
 *
 * - position deletes do not affect row IDs, the ranges will be adjusted
 *   during compaction.
 *
 * - all files in a table with row IDs must have at least one corresponding
 *   row ID range
 */

#define ROW_ID_MAPPINGS_TABLE_NAME  "row_id_mappings"
#define ROW_ID_MAPPINGS_QUALIFIED_TABLE_NAME PG_LAKE_TABLE_SCHEMA ".row_id_mappings"


void		InsertSingleRowMapping(Oid relationId,
								   int64 fileNumber,
								   int64 rowIdStart,
								   int64 rowIdEnd,
								   int64 rowNumberStart);
void		DeleteRowMappingsForTable(Oid relationId);
