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

namespace duckdb {

/*
 * PgLakeQueryListener can be used to safely get the query
 * string from a ClientContext. GetCurrentQuery is only safe to call
 * while a query is active.
 */
struct PgLakeQueryListener : public ClientContextState {
	int64_t connectionId;

	bool isQueryActive;
	string queryString;
	timestamp_t queryStart;

	void QueryBegin(ClientContext &context) override {
		isQueryActive = true;
		queryString = context.GetCurrentQuery();
		queryStart = Timestamp::GetCurrentTimestamp();
	}

	void QueryEnd(ClientContext &context) override {
		isQueryActive = false;
		queryString = "";
	}
};


} // namespace duckdb
