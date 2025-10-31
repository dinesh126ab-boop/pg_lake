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

/*
 * Required extension entry-point.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"


PG_MODULE_MAGIC;

/* function declarations */
void		_PG_init(void);


/*
 * _PG_init is the entry-point for the library.
 */
void
_PG_init(void)
{
	if (IsBinaryUpgrade)
		return;
}
