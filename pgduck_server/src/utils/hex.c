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
 * Hexadecimal encoding functions.
 *
 * Portions Copyright (c) 2010-2023, PostgreSQL Global Development Group
 * See: src/backend/utils/adt/encode.c
 */
#include "postgres_fe.h"

#include "utils/hex.h"
#include "utils/pgduck_log_utils.h"

static const char hextbl[] = "0123456789abcdef";

static const int8 hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

uint64
hex_encode(const char *src, size_t len, char *dst)
{
	const char *end = src + len;

	while (src < end)
	{
		*dst++ = hextbl[(*src >> 4) & 0xF];
		*dst++ = hextbl[*src & 0xF];
		src++;
	}
	return (uint64) len * 2;
}

static inline bool
get_hex(const char *cp, char *out)
{
	unsigned char c = (unsigned char) *cp;
	int			res = -1;

	if (c < 127)
		res = hexlookup[c];

	*out = (char) res;

	return (res >= 0);
}


uint64
hex_decode(const char *src, size_t len, char *dst)
{
	const char *s,
			   *srcend;
	char		v1,
				v2,
			   *p;

	srcend = src + len;
	s = src;
	p = dst;
	while (s < srcend)
	{
		if (*s == ' ' || *s == '\n' || *s == '\t' || *s == '\r')
		{
			s++;
			continue;
		}
		if (!get_hex(s, &v1))
		{
			PGDUCK_SERVER_ERROR("invalid hexadecimal digit: \"%.*s\"", 1, s);
			return -1;
		}
		s++;
		if (s >= srcend)
		{
			PGDUCK_SERVER_ERROR("invalid hexadecimal data: odd number of digits");
			return -1;
		}
		if (!get_hex(s, &v2))
		{
			PGDUCK_SERVER_ERROR("invalid hexadecimal digit: \"%.*s\"", 1, s);
			return -1;
		}
		s++;
		*p++ = (v1 << 4) | v2;
	}

	return p - dst;
}
