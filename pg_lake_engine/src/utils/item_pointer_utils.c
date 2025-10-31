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

#include "pg_lake/util/item_pointer_utils.h"
#include "storage/itemptr.h"


/*
 * UInt64ToItemPointer converts a 64-bit integer to an ItemPointer,
 * of which at most 48 bits are used.
 */
ItemPointer
UInt64ToItemPointer(uint64 ctidInt)
{
	ItemPointer result = (ItemPointer) palloc(sizeof(ItemPointerData));
	BlockNumber blockNumber = (uint32) (ctidInt >> 16);
	OffsetNumber offsetNumber = (uint16) (ctidInt & 0xFFFF);

	ItemPointerSet(result, blockNumber, offsetNumber);

	return result;
}


/*
 * ItemPointerToUInt64 converts an ItemPointer to a 64-bit integer,
 * of which at most 48 bits are used.
 */
uint64
ItemPointerToUInt64(ItemPointer ctid)
{
	uint64		blockNumber = ItemPointerGetBlockNumberNoCheck(ctid);
	uint64		offsetNumber = ItemPointerGetOffsetNumberNoCheck(ctid);

	return (blockNumber << 16) | offsetNumber;
}
