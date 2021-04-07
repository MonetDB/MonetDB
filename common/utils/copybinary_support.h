#ifndef COPYBINARY_SUPPORT_H
#define COPYBINARY_SUPPORT_H

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
copy_binary_byteswap16(uint16_t value) {
	return _byteswap_ushort(value);
}

static inline uint32_t
copy_binary_byteswap32(uint32_t value) {
	return _byteswap_ulong(value);
}

static inline uint64_t
copy_binary_byteswap64(uint64_t value) {
	return _byteswap_uint64(value);
}

#else

static inline uint16_t
copy_binary_byteswap16(uint16_t value) {
	return
		((value & 0xFF00u) >>  8u) |
		((value & 0x00FFu) <<  8u)
		;
}

static inline uint32_t
copy_binary_byteswap32(uint32_t value) {
	return
		((value & 0xFF000000u) >> 24u) |
		((value & 0x00FF0000u) >>  8u) |
		((value & 0x0000FF00u) <<  8u) |
		((value & 0x000000FFu) << 24u)
		;
}

static inline uint64_t
copy_binary_byteswap64(uint64_t value) {
	return
		((value & 0xFF00000000000000u) >> 56u) |
		((value & 0x00FF000000000000u) >> 40u) |
		((value & 0x0000FF0000000000u) >> 24u) |
		((value & 0x000000FF00000000u) >>  8u) |
		((value & 0x00000000FF000000u) <<  8u) |
		((value & 0x0000000000FF0000u) << 24u) |
		((value & 0x000000000000FF00u) << 40u) |
		((value & 0x00000000000000FFu) << 56u)
		;
}

#endif

#ifdef HAVE_HGE
static inline
uhge copy_binary_byteswap128(uhge value) {
	uint64_t lo = (uint64_t) value;
	uint64_t hi = (uint64_t) (value >> 64);
	uhge swapped_lo = (uhge)copy_binary_byteswap64(lo);
	uhge swapped_hi = (uhge)copy_binary_byteswap64(hi);
	return swapped_hi | (swapped_lo << 64);
}
#endif

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
	uhge *pp = (uhge*)p;
	*pp = copy_binary_byteswap128(*pp);
}
#endif

static inline void
copy_binary_convert_date(void *p)
{
	copy_binary_date *pp = (copy_binary_date*)p;
	copy_binary_convert32(&pp->year);
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
