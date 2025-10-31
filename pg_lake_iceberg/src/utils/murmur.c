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
 * MurmurHash3 implementation for Iceberg bucket partitioning.
 * This is a C implementation of the MurmurHash3 algorithm,
 * which is inspired by Apache Iceberg's Java implementation.
 *
 * https://github.com/apache/iceberg/blob/9fb80b7167e58a2daef80f4a1a09f223b870c030/api/src/main/java/org/apache/iceberg/util/BucketUtil.java
 *
 * https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/Murmur3_32HashFunction.java
 */


#include "postgres.h"

#include "pg_lake/iceberg/hash_utils.h"

#include <stdint.h>
#include <stddef.h>


static inline uint32_t
rotl32(uint32_t x, uint8_t r)
{
	return (x << r) | (x >> (32u - r));
}


static inline uint32_t
fmix32(uint32_t h)
{
	const uint32_t fm1 = 0x85ebca6bu;
	const uint32_t fm2 = 0xc2b2ae35u;

	h ^= h >> 16;
	h *= fm1;
	h ^= h >> 13;
	h *= fm2;
	h ^= h >> 16;
	return h;
}


static inline uint32_t
mixk1(uint32_t k1)
{
	const uint32_t c1 = 0xcc9e2d51u;
	const uint32_t c2 = 0x1b873593u;

	k1 *= c1;
	k1 = rotl32(k1, 15);
	k1 *= c2;

	return k1;
}


static inline uint32_t
mixh1(uint32_t seed, uint32_t k1)
{
	uint32_t	h1 = seed ^ k1;

	h1 = rotl32(h1, 13);
	h1 = h1 * 5u + 0xe6546b64u;

	return h1;
}


/*
 * MurmurHash3_32_Bytes computes a 32-bit MurmurHash3 hash of the given bytes key.
 * Seed is mandated to be 0 by the Apache Iceberg specification.
 */
int32_t
MurmurHash3_32_Bytes(const void *key, size_t len)
{
	const uint8_t *data = (const uint8_t *) key;
	const size_t nblock = len / 4;

	uint32_t	h1 = 0;			/* seed 0 */

	/* ---- body: 4-byte blocks ------------------------------------------ */
	for (size_t i = 0; i < nblock; i++)
	{
		/* read little-endian without assuming alignment */
		uint32_t	k1 =
			(uint32_t) data[4 * i + 0]
			| (uint32_t) data[4 * i + 1] << 8
			| (uint32_t) data[4 * i + 2] << 16
			| (uint32_t) data[4 * i + 3] << 24;

		k1 = mixk1(k1);
		h1 = mixh1(h1, k1);
	}

	/* ---- tail ---------------------------------------------------------- */
	const uint8_t *tail = data + nblock * 4;
	uint32_t	k1 = 0;

	switch (len & 3)
	{
		case 3:
			k1 ^= (uint32_t) tail[2] << 16;
			/* fallthrough */
		case 2:
			k1 ^= (uint32_t) tail[1] << 8;
			/* fallthrough */
		case 1:
			k1 ^= (uint32_t) tail[0];
			k1 = mixk1(k1);
			h1 ^= k1;
	}

	/* ---- finalization -------------------------------------------------- */
	h1 ^= (uint32_t) len;
	h1 = fmix32(h1);

	return (int32_t) h1;
}


/*
 * MurmurHash3_32_Int computes a 32-bit MurmurHash3 hash of the given
 * integer key. Seed is mandated to be 0 by the Apache Iceberg specification.
 */
int32_t
MurmurHash3_32_Int(int32_t key)
{
	uint32_t	seed = 0;

	uint32_t	k1 = mixk1((uint32_t) key);

	uint32_t	h1 = mixh1(seed, k1);

	h1 ^= 4u;
	h1 = fmix32(h1);

	return (int32_t) h1;
}


/*
 * MurmurHash3_32_Long computes a 32-bit MurmurHash3 hash of the given
 * 64-bit integer key. Seed is mandated to be 0 by the Apache Iceberg specification.
 */
int32_t
MurmurHash3_32_Long(int64_t key)
{
	/* Split the 64‑bit word into two 32‑bit little‑endian blocks */
	uint32_t	low = (uint32_t) (key & 0xFFFFFFFFu);
	uint32_t	high = (uint32_t) ((key >> 32) & 0xFFFFFFFFu);

#if BIG_ENDIAN_HOST
	/* Convert to little‑endian byte order on BE machines */
	k1 = bswap32(low);
	k2 = bswap32(high);
#endif

	uint32_t	seed = 0;

	int			k1 = mixk1(low);
	int			h1 = mixh1(seed, k1);

	k1 = mixk1(high);
	h1 = mixh1(h1, k1);

	h1 ^= 8u;
	h1 = fmix32(h1);

	return (int32_t) h1;
}
