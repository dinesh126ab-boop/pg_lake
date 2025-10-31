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

#include "pg_lake/util/url_encode.h"

#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(url_encode_path);


/*
* url_encode_path is a test function that gets a string and returns a URL-encoded
* version of it.
*/
Datum
url_encode_path(PG_FUNCTION_ARGS)
{
	char	   *uri = text_to_cstring(PG_GETARG_TEXT_P(0));

	PG_RETURN_TEXT_P(cstring_to_text(URLEncodePath(uri)));
}
