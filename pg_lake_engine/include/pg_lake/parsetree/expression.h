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

#include "nodes/nodes.h"
#include "datatype/timestamp.h"

extern PGDLLEXPORT Node *MakeUpperCaseExpr(Node *arg);
extern PGDLLEXPORT Node *MakeLowerCaseExpr(Node *arg);
extern PGDLLEXPORT Node *MakeDatePartExpr(char *part, Node *timeArg);
extern PGDLLEXPORT Node *MakeDateTruncExpr(char *part, Node *timeArg);
extern PGDLLEXPORT Node *MakeDivisionExpr(Node *arg, int divisor);
extern PGDLLEXPORT Node *MakeModInt32Expr(Node *arg, int32 modulo);
extern PGDLLEXPORT Node *MakeModInt64Expr(Node *arg, int64 modulo);
extern PGDLLEXPORT Node *MakeLpadExpr(Node *arg, int length, char *string);
extern PGDLLEXPORT Node *MakeRpadExpr(Node *arg, int length, char *string);
extern PGDLLEXPORT Node *MakeCastExpr(Node *arg, Oid targetType);
extern PGDLLEXPORT Node *MakeCaseExpr(Node *testArg, Node *ifTrueArg, Node *elseArg);
extern PGDLLEXPORT Node *MakeOpExpr(Node *left, char *schemaName, char *operatorName, Node *right);
extern PGDLLEXPORT Node *MakeAddIntervalExpr(Node *node, Interval *interval);
