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
 * Utility functions that are based on Postgres logging infrastructure.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#ifndef PG_LOG_UTILS_H
#define PG_LOG_UTILS_H

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

extern const char *GetErrorCodeStr(int code, bool *found);

#endif							/* // PG_DUCK_LOG_UTILS_H */
