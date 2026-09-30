/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef COPYBINARY_SUPPORT_H
#define COPYBINARY_SUPPORT_H

#include <stdint.h>
#include "copybinary.h"

// According to Godbolt, these code sequences are recognized by
// GCC at least back to 6.2 and Clang at least back to 6.0.0.
// I didn't check earlier ones.
// They properly use byte swapping instructions.
// MSVC doesn't recognize it but that's no problem because we will
// not ever use it for big endian platforms.

// First some macros that can be used a expressions:
//    uint16_t swapped = copy_binary_byteswap16(value);


#ifdef _MSC_VER

static inline uint16_t
copy_binary_byteswap16(uint16_t value)
{
	return _byteswap_ushort(value);
}

static inline uint32_t
copy_binary_byteswap32(uint32_t value)
{
	return _byteswap_ulong(value);
}

static inline uint64_t
copy_binary_byteswap64(uint64_t value)
{
	return _byteswap_uint64(value);
}

#else

static inline uint16_t
copy_binary_byteswap16(uint16_t value)
{
	return
		((value & (UINT16_C(0xFF) << 8)) >>  8) |
		((value & (UINT16_C(0xFF) << 0)) <<  8)
		;
}

static inline uint32_t
copy_binary_byteswap32(uint32_t value)
{
	return
		((value & (UINT32_C(0xFF) << 24)) >> 24) |
		((value & (UINT32_C(0xFF) << 16)) >>  8) |
		((value & (UINT32_C(0xFF) <<  8)) <<  8) |
		((value & (UINT32_C(0xFF) <<  0)) << 24)
		;
}

static inline uint64_t
copy_binary_byteswap64(uint64_t value)
{
	return
		((value & (UINT64_C(0xFF) << 56)) >> 56) |
		((value & (UINT64_C(0xFF) << 48)) >> 40) |
		((value & (UINT64_C(0xFF) << 40)) >> 24) |
		((value & (UINT64_C(0xFF) << 32)) >>  8) |
		((value & (UINT64_C(0xFF) << 24)) <<  8) |
		((value & (UINT64_C(0xFF) << 16)) << 24) |
		((value & (UINT64_C(0xFF) <<  8)) << 40) |
		((value & (UINT64_C(0xFF) <<  0)) << 56)
		;
}

#endif

#ifdef HAVE_HGE
static inline uint128_t
copy_binary_byteswap128(uint128_t value)
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

static inline copy_binary_date
copy_binary_byteswap_date(copy_binary_date value)
{
	return (copy_binary_date) {
		.day = value.day,
		.month = value.month,
		.year = copy_binary_byteswap16(value.year),
	};
}

static inline copy_binary_time
copy_binary_byteswap_time(copy_binary_time value)
{
	return (copy_binary_time) {
		.ms = copy_binary_byteswap32(value.ms),
		.seconds = value.seconds,
		.minutes = value.minutes,
		.hours = value.hours,
		.padding = value.padding,
	};
}

static inline copy_binary_timestamp
copy_binary_byteswap_timestamp(copy_binary_timestamp value)
{
	return (copy_binary_timestamp) {
		.time = copy_binary_byteswap_time(value.time),
		.date = copy_binary_byteswap_date(value.date),
	};
}




// These macros are used to convert a value in-place.
// This makes it possible to also convert timestamp structs.

static inline void
copy_binary_convert16(void *p)
{
	uint16_t *pp = (uint16_t*)p;
	*pp = copy_binary_byteswap16(*pp);
}

static inline void
copy_binary_convert32(void *p)
{
	uint32_t *pp = (uint32_t*)p;
	*pp = copy_binary_byteswap32(*pp);
}

static inline void
copy_binary_convert64(void *p)
{
	uint64_t *pp = (uint64_t*)p;
	*pp = copy_binary_byteswap64(*pp);
}

#ifdef HAVE_HGE
static inline void
copy_binary_convert128(void *p)
{
	uint128_t *pp = (uint128_t*)p;
	*pp = copy_binary_byteswap128(*pp);
}
#endif

static inline void
copy_binary_convert_date(void *p)
{
	copy_binary_date *pp = (copy_binary_date*)p;
	copy_binary_convert16(&pp->year);
}


static inline void
copy_binary_convert_time(void *p)
{
	copy_binary_time *pp = (copy_binary_time*)p;
	copy_binary_convert32(&pp->ms);
}

static inline void
copy_binary_convert_timestamp(void *p)
{
	copy_binary_timestamp *pp = (copy_binary_timestamp*)p;
	copy_binary_convert_date(&pp->date);
	copy_binary_convert_time(&pp->time);
}

#endif
