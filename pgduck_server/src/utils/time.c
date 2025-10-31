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

#include <locale.h>
#include <time.h>

#include "utils/pgduck_log_utils.h"


/*
 * iso8601_timestamp writes an ISO 8601 formatted timestamp to buf.
 *
 * The buffer should be at least 21 characters long.
 */
void
iso8601_timestamp(char *buf)
{
	time_t		now;

	time(&now);
	strftime(buf, 21, "%FT%TZ", gmtime(&now));
}
