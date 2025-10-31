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

#include "utils/jsonb.h"

extern PGDLLEXPORT void appendJsonStringWithEscapedValue(StringInfo str, const char *key, const char *value);
extern PGDLLEXPORT void appendJsonString(StringInfo str, const char *key, const char *value);
extern PGDLLEXPORT void appendJsonKey(StringInfo str, const char *key);
extern PGDLLEXPORT void appendJsonValue(StringInfo str, const char *value);
extern PGDLLEXPORT void appendJsonInt32(StringInfo str, const char *key, int32_t value);
extern PGDLLEXPORT void appendJsonInt64(StringInfo str, const char *key, int64_t value);
extern PGDLLEXPORT void appendJsonBool(StringInfo str, const char *key, bool value);
extern PGDLLEXPORT const char *EscapeJson(const char *val);
extern PGDLLEXPORT const char *UnEscapeJson(const char *val);
