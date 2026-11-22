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
#include "mswap.h"

// According to Godbolt, these code sequences are recognized by
// GCC at least back to 6.2 and Clang at least back to 6.0.0.
// I didn't check earlier ones.
// They properly use byte swapping instructions.
// MSVC doesn't recognize it but that's no problem because we will
// not ever use it for big endian platforms.

// First some macros that can be used a expressions:
//    uint16_t swapped = copy_binary_byteswap16(value);


#define copy_binary_byteswap16(v)	swap_int16(v)
#define copy_binary_byteswap32(v)	swap_int32(v)
#define copy_binary_byteswap64(v)	swap_int64(v)
#ifdef HAVE_HGE
#define copy_binary_byteswap128(v)	swap_int128(v)
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
