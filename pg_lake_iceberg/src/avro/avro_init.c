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
#include "utils/palloc.h"
#include "utils/memutils.h"

#include "avro.h"

#include "pg_lake/avro/avro_init.h"


static void *AvroPostgresAllocator(void *ud, void *ptr, size_t osize, size_t nsize);

/*
 * AvroInit sets up initialization steps for Avro reader and writer.
 * Currently, it only sets up global allocator as Postgres's.
 */
void
AvroInit(void)
{
	avro_set_allocator(AvroPostgresAllocator, NULL);
}

/*
 * AvroPostgresAllocator can be used by libavro to allocate memory via
 * Postgres memory contexts.
 */
static void *
AvroPostgresAllocator(void *ud, void *ptr, size_t osize, size_t nsize)
{
	if (nsize == 0)
	{
		pfree(ptr);
		return NULL;
	}
	else if (ptr != NULL)
	{
		return repalloc(ptr, nsize);
	}
	else
	{
		return palloc(nsize);
	}
}
