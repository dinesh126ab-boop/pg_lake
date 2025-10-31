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

#ifndef NUMERIC_UTIL_H
#define NUMERIC_UTIL_H

/*
 * make_numeric_typmod() - from numeric.c in postgres
 *
 *  Pack numeric precision and scale values into a typmod.  The upper 16 bits
 *  are used for the precision (though actually not all these bits are needed,
 *  since the maximum allowed precision is 1000).  The lower 16 bits are for
 *  the scale, but since the scale is constrained to the range [-1000, 1000],
 *  we use just the lower 11 of those 16 bits, and leave the remaining 5 bits
 *  unset, for possible future use.
 *
 *  For purely historical reasons VARHDRSZ is then added to the result, thus
 *  the unused space in the upper 16 bits is not all as freely available as it
 *  might seem.  (We can't let the result overflow to a negative int32, as
 *  other parts of the system would interpret that as not-a-valid-typmod.)
 */
static inline int32
make_numeric_typmod(int precision, int scale)
{
	return ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ;
}


/*
 * numeric_typmod_precision() -
 *
 *  Extract the precision from a numeric typmod --- see make_numeric_typmod().
 */
static inline int
numeric_typmod_precision(int32 typmod)
{
	return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}

/*
 * numeric_typmod_scale() -
 *
 *  Extract the scale from a numeric typmod --- see make_numeric_typmod().
 *
 *  Note that the scale may be negative, so we must do sign extension when
 *  unpacking it.  We do this using the bit hack (x^1024)-1024, which sign
 *  extends an 11-bit two's complement number x.
 */
static inline int
numeric_typmod_scale(int32 typmod)
{
	return (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
}

#endif
