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


// These code sequences are recognized by gcc and clang
#define COPY_BINARY_BYTESWAP16(u) ( \
		  ((uint8_t)(u) << 8) \
		| ((uint8_t)(u>>8)) \
	)
#define COPY_BINARY_BYTESWAP32(u) ( \
		  ((uint8_t)(u) << 24) \
		| ((uint8_t)(u>>8) << 16) \
		| ((uint8_t)(u>>16) << 8) \
		| ((uint8_t)(u>>24)) \
	)
#define COPY_BINARY_BYTESWAP64(u) ( \
		  ((uint8_t)(u>>0) << 56) \
		| ((uint8_t)(u>>8) << 48) \
		| ((uint8_t)(u>>16) << 40) \
		| ((uint8_t)(u>>24) << 32) \
		| ((uint8_t)(u>>32) << 24) \
		| ((uint8_t)(u>>40) << 16) \
		| ((uint8_t)(u>>48) << 8) \
		| ((uint8_t)(u>>56)) \
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
