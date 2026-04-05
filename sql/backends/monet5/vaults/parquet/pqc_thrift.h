/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _PQC_THRIFT_H_
#define _PQC_THRIFT_H_

/**
 * Thrift compact protocol
 * Take special note of the T_STOP type which is used specifically to mark
 * the end of a sequence of fields.
 */
typedef enum {
  T_STOP           = 0x00,
  T_BOOLEAN_TRUE   = 0x01,
  T_BOOLEAN_FALSE  = 0x02,
  T_BYTE           = 0x03,
  T_I16            = 0x04,
  T_I32            = 0x05,
  T_I64            = 0x06,
  T_DOUBLE         = 0x07,
  T_BINARY         = 0x08,
  T_LIST           = 0x09,
  T_SET            = 0x0A,
  T_MAP            = 0x0B,
  T_STRUCT         = 0x0C
} ThriftCompactType;

#include <sys/types.h>

/* these will be private within the library */

/* get windows defa */
#define pqc_export extern

pqc_export uint32_t get_uint32(unsigned char *c);
pqc_export uint32_t get_le_uint32(unsigned char *c);

pqc_export int pqc_get_zint32( char *in, uint32_t *v); /* returns number of read bytes, or error -1 */
pqc_export int pqc_get_zint64( char *in, uint64_t *v); /* returns number of read bytes, or error -1 */

pqc_export int pqc_get_int32( char *in, uint32_t *v); /* returns number of read bytes, or error -1 */
pqc_export int pqc_get_int64( char *in, uint64_t *v); /* returns number of read bytes, or error -1 */

pqc_export int pqc_put_int32( char *in, uint32_t v); /* returns number of read bytes, or error -1 */
pqc_export int pqc_put_int64( char *in, uint64_t v); /* returns number of read bytes, or error -1 */

pqc_export int pqc_get_field( char *in, int *fieldid, int *type);
pqc_export int pqc_get_list( char *in, int *size, int *type);
pqc_export int pqc_get_string( char *in, char **s, int *len); /* need way to buffer 0 */

#ifdef _MSC_VER
/* use intrinsic functions on Windows */
#define short_int_SWAP(s)	((int16_t) _byteswap_ushort((uint16_t) (s)))
/* on Windows, long is the same size as int */
#define normal_int_SWAP(i)	((int) _byteswap_ulong((unsigned long) (i)))
#define long_int_SWAP(l)	((int64_t) _byteswap_uint64((unsigned __int64) (l)))
#else
#define short_int_SWAP(s)				\
	((int16_t) (((0x00ff & (uint16_t) (s)) << 8) |	\
		  ((0xff00 & (uint16_t) (s)) >> 8)))

#define normal_int_SWAP(i)						\
	((int) (((((unsigned) 0xff <<  0) & (unsigned) (i)) << 24) |	\
		((((unsigned) 0xff <<  8) & (unsigned) (i)) <<  8) |	\
		((((unsigned) 0xff << 16) & (unsigned) (i)) >>  8) |	\
		((((unsigned) 0xff << 24) & (unsigned) (i)) >> 24)))

#define long_int_SWAP(l)						\
	((int64_t) (((((uint64_t) 0xff <<  0) & (uint64_t) (l)) << 56) | \
		((((uint64_t) 0xff <<  8) & (uint64_t) (l)) << 40) |	\
		((((uint64_t) 0xff << 16) & (uint64_t) (l)) << 24) |	\
		((((uint64_t) 0xff << 24) & (uint64_t) (l)) <<  8) |	\
		((((uint64_t) 0xff << 32) & (uint64_t) (l)) >>  8) |	\
		((((uint64_t) 0xff << 40) & (uint64_t) (l)) >> 24) |	\
		((((uint64_t) 0xff << 48) & (uint64_t) (l)) >> 40) |	\
		((((uint64_t) 0xff << 56) & (uint64_t) (l)) >> 56)))
#endif

#ifdef HAVE_HGE
#define huge_int_SWAP(h)					\
	((hge) (((((uhge) 0xff <<   0) & (uhge) (h)) << 120) |	\
		((((uhge) 0xff <<   8) & (uhge) (h)) << 104) |	\
		((((uhge) 0xff <<  16) & (uhge) (h)) <<  88) |	\
		((((uhge) 0xff <<  24) & (uhge) (h)) <<  72) |	\
		((((uhge) 0xff <<  32) & (uhge) (h)) <<  56) |	\
		((((uhge) 0xff <<  40) & (uhge) (h)) <<  40) |	\
		((((uhge) 0xff <<  48) & (uhge) (h)) <<  24) |	\
		((((uhge) 0xff <<  56) & (uhge) (h)) <<   8) |	\
		((((uhge) 0xff <<  64) & (uhge) (h)) >>   8) |	\
		((((uhge) 0xff <<  72) & (uhge) (h)) >>  24) |	\
		((((uhge) 0xff <<  80) & (uhge) (h)) >>  40) |	\
		((((uhge) 0xff <<  88) & (uhge) (h)) >>  56) |	\
		((((uhge) 0xff <<  96) & (uhge) (h)) >>  72) |	\
		((((uhge) 0xff << 104) & (uhge) (h)) >>  88) |	\
		((((uhge) 0xff << 112) & (uhge) (h)) >> 104) |	\
		((((uhge) 0xff << 120) & (uhge) (h)) >> 120)))
#endif

#ifdef WORDS_BIGENDIAN
#define pqc_sht(s) short_int_SWAP(s)
#define pqc_int(s) normal_int_SWAP(s)
#define pqc_lng(s) long_int_SWAP(s)
#define pqc_hge(s) huge_int_SWAP(s)
#else
#define pqc_sht(s) (s)
#define pqc_int(s) (s)
#define pqc_lng(s) (s)
#define pqc_hge(s) (s)
#endif

#endif /*_PQC_THRIFT_H_*/
