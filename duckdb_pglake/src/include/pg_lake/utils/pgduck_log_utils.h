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
 * Slightly simplified version of pgduck_log_utils in pgduck_server
 */
#ifndef PG_DUCK_LOG_UTILS_H
#define PG_DUCK_LOG_UTILS_H

#include <time.h>

/* PostgreSQL error levels, used for consistency */
#define DEBUG5 10
#define DEBUG4 11
#define DEBUG3 12
#define DEBUG2 13
#define DEBUG1 14
#define LOG 15
#define INFO 17
#define NOTICE 18
#define WARNING 19
#define WARNING_CLIENT_ONLY 20
#define ERROR 21
#define FATAL 22
#define PANIC 23

/* we map error levels into these strings via level ## _STRING below */
#define ERROR_STRING "ERROR"
#define WARNING_STRING "WARNING"
#define LOG_STRING "LOG"
#define DEBUG1_STRING "DEBUG"

/*
 * Print the message to stderr, prefixed by
 * verbose: <timestamp> <level string> <file name>:<line number>:<function name>
 * regular: <timestamp> <level string>
 */
#define PGDUCK_SERVER_LOG_INTERNAL(level, fmt, ...) \
	{ \
	char currentTime[21]; \
	iso8601_timestamp(currentTime); \
	if (PgLakePgcompatIsOutputVerbose) \
		fprintf(stderr, "%s %s %s:%d:%s " fmt "\n", \
				currentTime, level ## _STRING, __FILE__, __LINE__, __func__, ##__VA_ARGS__);  \
	else if (level != DEBUG1) \
		fprintf(stderr, "%s %s " fmt "\n", \
				currentTime, level ## _STRING, ##__VA_ARGS__); \
	}

#define PGDUCK_SERVER_DEBUG(fmt, ...) \
    PGDUCK_SERVER_LOG_INTERNAL(DEBUG1, fmt __VA_OPT__(,) __VA_ARGS__)

#define PGDUCK_SERVER_LOG(fmt, ...) \
    PGDUCK_SERVER_LOG_INTERNAL(LOG, fmt __VA_OPT__(,) __VA_ARGS__)

#define PGDUCK_SERVER_WARN(fmt, ...) \
    PGDUCK_SERVER_LOG_INTERNAL(WARNING, fmt __VA_OPT__(,) __VA_ARGS__)

#define PGDUCK_SERVER_ERROR(fmt, ...) \
    PGDUCK_SERVER_LOG_INTERNAL(ERROR, fmt __VA_OPT__(,) __VA_ARGS__)


extern bool PgLakePgcompatIsOutputVerbose;

/*
 * iso8601_timestamp writes an ISO 8601 formatted timestamp to buf.
 *
 * The buffer should be at least 21 characters long.
 */
static inline void
iso8601_timestamp(char *buf)
{
	time_t		now;

	time(&now);
	strftime(buf, 21, "%FT%TZ", gmtime(&now));
}

#endif							/* // PG_DUCK_LOG_UTILS_H */
