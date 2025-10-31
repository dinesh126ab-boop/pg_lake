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


#define ADV_LOCKTAG_CLASS_IN_PROGRESS_FILE_OPERATION_ID 1001


#define SET_LOCKTAG_IN_PROGRESS_OPERATION_ID(tag, operationId) \
	SET_LOCKTAG_ADVISORY(tag, \
						 MyDatabaseId, \
						 (uint32) ((operationId) >> 32), \
						 (uint32) operationId, \
						 ADV_LOCKTAG_CLASS_IN_PROGRESS_FILE_OPERATION_ID)
