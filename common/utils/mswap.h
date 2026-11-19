/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _MSWAP_H_
#define _MSWAP_H_

#include "monetdb_config.h"

static inline uint16_t
swap_int16(uint16_t value)
{
#ifdef _MSC_VER
	return _byteswap_ushort(value);
#else
	return
		((value & (UINT16_C(0xFF) << 8)) >> 8) |
		((value & (UINT16_C(0xFF) << 0)) << 8);
#endif
}

static inline uint32_t
swap_int32(uint32_t value)
{
#ifdef _MSC_VER
	return _byteswap_ulong(value);
#else
	return
		((value & (UINT32_C(0xFF) << 24)) >> 24) |
		((value & (UINT32_C(0xFF) << 16)) >>  8) |
		((value & (UINT32_C(0xFF) <<  8)) <<  8) |
		((value & (UINT32_C(0xFF) <<  0)) << 24);
#endif
}

static inline uint64_t
swap_int64(uint64_t value)
{
#ifdef _MSC_VER
	return _byteswap_uint64(value);
#else
	return
		((value & (UINT64_C(0xFF) << 56)) >> 56) |
		((value & (UINT64_C(0xFF) << 48)) >> 40) |
		((value & (UINT64_C(0xFF) << 40)) >> 24) |
		((value & (UINT64_C(0xFF) << 32)) >>  8) |
		((value & (UINT64_C(0xFF) << 24)) <<  8) |
		((value & (UINT64_C(0xFF) << 16)) << 24) |
		((value & (UINT64_C(0xFF) <<  8)) << 40) |
		((value & (UINT64_C(0xFF) <<  0)) << 56);
#endif
}

#ifdef HAVE_INT128_T
static inline uint128_t
swap_int128(uint128_t value)
{
	return
		((value & ((uint128_t) 0xFF << 120)) >> 120) |
		((value & ((uint128_t) 0xFF << 112)) >> 104) |
		((value & ((uint128_t) 0xFF << 104)) >>  88) |
		((value & ((uint128_t) 0xFF <<  96)) >>  72) |
		((value & ((uint128_t) 0xFF <<  88)) >>  56) |
		((value & ((uint128_t) 0xFF <<  80)) >>  40) |
		((value & ((uint128_t) 0xFF <<  72)) >>  24) |
		((value & ((uint128_t) 0xFF <<  64)) >>   8) |
		((value & ((uint128_t) 0xFF <<  56)) <<   8) |
		((value & ((uint128_t) 0xFF <<  48)) <<  24) |
		((value & ((uint128_t) 0xFF <<  40)) <<  40) |
		((value & ((uint128_t) 0xFF <<  32)) <<  56) |
		((value & ((uint128_t) 0xFF <<  24)) <<  72) |
		((value & ((uint128_t) 0xFF <<  16)) <<  88) |
		((value & ((uint128_t) 0xFF <<   8)) << 104) |
		((value & ((uint128_t) 0xFF <<   0)) << 120);
}
#endif

#endif
