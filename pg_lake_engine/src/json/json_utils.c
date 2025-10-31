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

#include <inttypes.h>

#include "utils/json.h"
#include "utils/jsonb.h"

#include "pg_lake/json/json_utils.h"


const char *
EscapeJson(const char *val)
{
	if (val == NULL)
		return "\"\"";

	StringInfo	str = makeStringInfo();

	escape_json(str, val);

	return str->data;
}

/*
 * UnEscapeJson unescapes a JSON string. There is no unescape_json function in
 * PostgreSQL, so we have to implement it here.
 */
const char *
UnEscapeJson(const char *val)
{
	if (val == NULL)
		return NULL;

	StringInfo	unescapedVal = makeStringInfo();

	char	   *p = pstrdup(val);

	if (p[0] == '"')
	{
		p++;
	}

	if (p[strlen(p) - 1] == '"')
	{
		p[strlen(p) - 1] = '\0';
	}

	while (*p)
	{
		if (*p == '\\')
		{
			p++;
			switch (*p)
			{
				case 'b':
					appendStringInfoChar(unescapedVal, '\b');
					break;
				case 'f':
					appendStringInfoChar(unescapedVal, '\f');
					break;
				case 'n':
					appendStringInfoChar(unescapedVal, '\n');
					break;
				case 'r':
					appendStringInfoChar(unescapedVal, '\r');
					break;
				case 't':
					appendStringInfoChar(unescapedVal, '\t');
					break;
				case '"':
					appendStringInfoChar(unescapedVal, '"');
					break;
				case '\\':
					appendStringInfoChar(unescapedVal, '\\');
					break;
				case 'u':
					{
						char		unicodeStr[5];

						unicodeStr[0] = p[1];
						unicodeStr[1] = p[2];
						unicodeStr[2] = p[3];
						unicodeStr[3] = p[4];
						unicodeStr[4] = '\0';

						char		c = (char) strtol(unicodeStr, NULL, 16);

						appendStringInfoChar(unescapedVal, c);

						p += 4;
					}
					break;
				default:
					appendStringInfoChar(unescapedVal, *p);
					break;
			}
		}
		else
		{
			appendStringInfoChar(unescapedVal, *p);
		}

		p++;
	}

	return unescapedVal->data;
}

void
appendJsonStringWithEscapedValue(StringInfo str, const char *key, const char *escapedValue)
{
	appendStringInfo(str, "%s:%s", EscapeJson(key), escapedValue);
}

void
appendJsonString(StringInfo str, const char *key, const char *value)
{
	appendStringInfo(str, "%s:%s", EscapeJson(key), EscapeJson(value));
}

void
appendJsonKey(StringInfo str, const char *key)
{
	appendStringInfo(str, "%s:", EscapeJson(key));
}

void
appendJsonValue(StringInfo str, const char *value)
{
	appendStringInfo(str, "%s", EscapeJson(value));
}

void
appendJsonInt32(StringInfo str, const char *key, int32_t value)
{
	appendStringInfo(str, "%s:%d", EscapeJson(key), value);
}

void
appendJsonInt64(StringInfo str, const char *key, int64_t value)
{
	appendStringInfo(str, "%s:%" PRId64, EscapeJson(key), value);
}

void
appendJsonBool(StringInfo str, const char *key, bool value)
{
	appendStringInfo(str, "%s:%s", EscapeJson(key), value ? "true" : "false");
}
