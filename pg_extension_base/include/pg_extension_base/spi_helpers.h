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

#ifndef SPI_UTILITIES_H
#define SPI_UTILITIES_H

#include "executor/spi.h"

/* SPI macros for reading results */
#define DEDATUMIZE_TEXTOID(Value) TextDatumGetCString(Value)
#define DEDATUMIZE_NAMEOID(Value) DatumGetName(Value)
#define DEDATUMIZE_BOOLOID(Value) DatumGetBool(Value)
#define DEDATUMIZE_INT2OID(Value) DatumGetInt16(Value)
#define DEDATUMIZE_INT2ARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_INT4OID(Value) DatumGetInt32(Value)
#define DEDATUMIZE_INT4ARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_INT8OID(Value) DatumGetInt64(Value)
#define DEDATUMIZE_INT8ARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_FLOAT4OID(Value) DatumGetFloat4(Value)
#define DEDATUMIZE_FLOAT4ARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_FLOAT8OID(Value) DatumGetFloat8(Value)
#define DEDATUMIZE_FLOAT8ARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_JSONBOID(Value) DatumGetJsonbP(Value)
#define DEDATUMIZE_BYTEAOID(Value) DatumGetByteaP(Value)
#define DEDATUMIZE_BYTEAARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE(TypeId,Value) DATUMIZE_##TypeId(Value)

#define GET_SPI_DATUM(RowIndex,ColumnNumber,IsNull) \
	SPI_getbinval(SPI_tuptable->vals[RowIndex], SPI_tuptable->tupdesc, ColumnNumber, IsNull)

/* TODO: avoid dedatumize when isNull is true? */
#define GET_SPI_VALUE(TypeId,RowIndex,ColumnNumber,IsNull) \
	DEDATUMIZE_##TypeId(GET_SPI_DATUM(RowIndex,ColumnNumber,IsNull))

#endif
