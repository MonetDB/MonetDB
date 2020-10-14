#ifndef COPYBINARY_H
#define COPYBINARY_H

#include "monetdb_config.h"

typedef struct {
	uint8_t day;
	uint8_t month;
	int16_t year;
} copy_binary_date; // natural size: 32, natural alignment: 16

typedef struct {
	uint32_t ms;
	uint8_t seconds;
	uint8_t minutes;
	uint8_t hours;
	uint8_t padding; // implied in C, explicit elsewhere
} copy_binary_time;		 // natural size: 64, natural alignment: 32

typedef struct {
	copy_binary_time time;
	copy_binary_date date;
} copy_binary_timestamp; // natural size: 96, natural alignment: 32


// According to Godbolt, these code sequences are recognized by
// GCC at least back to 6.2 and Clang at least back to 6.0.0.
// I didn't check earlier ones.
// They properly use byte swapping instructions.
// MSVC doesn't recognize it but that's no problem because we will
// not ever use it for big endian platforms.


#define COPY_BINARY_BYTESWAP16(value) ( \
      (((*(uint16_t*)&value) & 0xFF00u) >>  8u) | \
      (((*(uint16_t*)&value) & 0x00FFu) <<  8u) \
    )


#define COPY_BINARY_BYTESWAP32(value) ( \
      (((*(uint32_t*)&value) & 0xFF000000u) >> 24u) | \
      (((*(uint32_t*)&value) & 0x00FF0000u) >>  8u) | \
      (((*(uint32_t*)&value) & 0x0000FF00u) <<  8u) | \
      (((*(uint32_t*)&value) & 0x000000FFu) << 24u) \
    )


#define COPY_BINARY_BYTESWAP64(value) ( \
      (((*(uint64_t*)&value) & 0xFF00000000000000u) >> 56u) | \
      (((*(uint64_t*)&value) & 0x00FF000000000000u) >> 40u) | \
      (((*(uint64_t*)&value) & 0x0000FF0000000000u) >> 24u) | \
      (((*(uint64_t*)&value) & 0x000000FF00000000u) >>  8u) | \
      (((*(uint64_t*)&value) & 0x00000000FF000000u) <<  8u) | \
      (((*(uint64_t*)&value) & 0x0000000000FF0000u) << 24u) | \
      (((*(uint64_t*)&value) & 0x000000000000FF00u) << 40u) | \
      (((*(uint64_t*)&value) & 0x00000000000000FFu) << 56u) \
    )

#define COPY_BINARY_BYTESWAP128(value) ( \
    ( (uint128_t)COPY_BINARY_BYTESWAP64(   ((uint64_t*)&value)[0]   )  << 64 ) \
    | ( (uint128_t)COPY_BINARY_BYTESWAP64(   ((uint64_t*)&value)[1]   )   ) \
)


#ifdef WORDS_BIGENDIAN
	#define COPY_BINARY_CONVERT16(lhs) \
		do { (lhs) = COPY_BINARY_BYTESWAP16(lhs); } while (0)
	#define COPY_BINARY_CONVERT32(lhs) \
		do { (lhs) = COPY_BINARY_BYTESWAP32(lhs); } while (0)
	#define COPY_BINARY_CONVERT64(lhs) \
		do { (lhs) = COPY_BINARY_BYTESWAP64(lhs); } while (0)
	#define COPY_BINARY_CONVERT128(lhs) \
		do { (lhs) = COPY_BINARY_BYTESWAP128(lhs); } while (0)
#else
	// still refer to the fields to provoke error message when used incorrectly
	#define COPY_BINARY_CONVERT16(lhs) \
		do { (void)(lhs); } while (0)
	#define COPY_BINARY_CONVERT32(lhs) \
		do { (void)(lhs); } while (0)
	#define COPY_BINARY_CONVERT64(lhs) \
		do { (void)(lhs); } while (0)
	#define COPY_BINARY_CONVERT128(lhs) \
		do { (void)(lhs); } while (0)
#endif

#define COPY_BINARY_CONVERT_DATE_ENDIAN(d) \
	do { \
		COPY_BINARY_CONVERT16((d).year); \
	} while (0)

#define COPY_BINARY_CONVERT_TIME_ENDIAN(t) \
	do { \
		COPY_BINARY_CONVERT32((t).ms); \
	} while (0)

#define COPY_BINARY_CONVERT_TIMESTAMP_ENDIAN(ts) \
    do { \
        COPY_BINARY_CONVERT_DATE_ENDIAN((ts).date); \
        COPY_BINARY_CONVERT_TIME_ENDIAN((ts).time); \
    } while (0)

#endif
