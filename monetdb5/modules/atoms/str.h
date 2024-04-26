/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef __string_H__
#define __string_H__
#include "gdk.h"
#include "mal.h"
#include "mal_exception.h"
#include <ctype.h>

/* Get the last char in (X2), and #bytes it takes, but do not decrease
 * the pos in (X2).  See gdk_atoms.c for UTF-8 encoding */
#define UTF8_LASTCHAR(X1, SZ, X2, SZ2)				\
	do {											\
		if (((X2)[SZ2-1] & 0x80) == 0) {			\
			(X1) = (X2)[SZ2-1];					\
			(SZ) = 1;								\
		} else if (((X2)[SZ2-2] & 0xE0) == 0xC0) {	\
			(X1)  = ((X2)[SZ2-2] & 0x1F) << 6;		\
			(X1) |= ((X2)[SZ2-1] & 0x3F);			\
			(SZ) = 2;								\
		} else if (((X2)[SZ2-3] & 0xF0) == 0xE0) {	\
			(X1)  = ((X2)[SZ2-3] & 0x0F) << 12;	\
			(X1) |= ((X2)[SZ2-2] & 0x3F) << 6;		\
			(X1) |= ((X2)[SZ2-1] & 0x3F);			\
			(SZ) = 3;								\
		} else if (((X2)[SZ2-4] & 0xF8) == 0xF0) {	\
			(X1)  = ((X2)[SZ2-4] & 0x07) << 18;	\
			(X1) |= ((X2)[SZ2-3] & 0x3F) << 12;	\
			(X1) |= ((X2)[SZ2-2] & 0x3F) << 6;		\
			(X1) |= ((X2)[SZ2-1] & 0x3F);			\
			(SZ) = 4;								\
		} else {									\
			(X1) = int_nil;						\
			(SZ) = 0;								\
		}											\
	} while (0)

/* Get the first char in (X2), and #bytes it takes, but do not
 * increase the pos in (X2) */
#define UTF8_NEXTCHAR(X1, SZ, X2)				\
	do {										\
		if (((X2)[0] & 0x80) == 0) {			\
			(X1) = (X2)[0];					\
			(SZ) = 1;							\
		} else if (((X2)[0] & 0xE0) == 0xC0) {	\
			(X1)  = ((X2)[0] & 0x1F) << 6;		\
			(X1) |= ((X2)[1] & 0x3F);			\
			(SZ) = 2;							\
		} else if (((X2)[0] & 0xF0) == 0xE0) {	\
			(X1)  = ((X2)[0] & 0x0F) << 12;	\
			(X1) |= ((X2)[1] & 0x3F) << 6;		\
			(X1) |= ((X2)[2] & 0x3F);			\
			(SZ) = 3;							\
		} else if (((X2)[0] & 0xF8) == 0xF0) {	\
			(X1)  = ((X2)[0] & 0x07) << 18;	\
			(X1) |= ((X2)[1] & 0x3F) << 12;	\
			(X1) |= ((X2)[2] & 0x3F) << 6;		\
			(X1) |= ((X2)[3] & 0x3F);			\
			(SZ) = 4;							\
		} else {								\
			(X1) = int_nil;					\
			(SZ) = 0;							\
		}										\
	} while (0)

#define UTF8_GETCHAR(X1, X2)											\
	do {																\
		if ((*(X2) & 0x80) == 0) {										\
			(X1) = *(X2)++;											\
		} else if ((*(X2) & 0xE0) == 0xC0) {							\
			(X1)  = (*(X2)++ & 0x1F) << 6;								\
			(X1) |= (*(X2)++ & 0x3F);									\
		} else if ((*(X2) & 0xF0) == 0xE0) {							\
			(X1)  = (*(X2)++ & 0x0F) << 12;							\
			(X1) |= (*(X2)++ & 0x3F) << 6;								\
			(X1) |= (*(X2)++ & 0x3F);									\
		} else if ((*(X2) & 0xF8) == 0xF0) {							\
			(X1)  = (*(X2)++ & 0x07) << 18;							\
			(X1) |= (*(X2)++ & 0x3F) << 12;							\
			(X1) |= (*(X2)++ & 0x3F) << 6;								\
			(X1) |= (*(X2)++ & 0x3F);									\
			if ((X1) > 0x10FFFF || ((X1) & 0x1FF800) == 0xD800)		\
				goto illegal;											\
		} else {														\
			(X1) = int_nil;											\
		}																\
	} while (0)

#define UTF8_PUTCHAR(X1, X2)											\
	do {																\
		if ((X1) < 0 || (X1) > 0x10FFFF || ((X1) & 0x1FF800) == 0xD800) { \
			goto illegal;												\
		} else if ((X1) <= 0x7F) {										\
			*(X2)++ = (X1);											\
		} else if ((X1) <= 0x7FF) {									\
			*(X2)++ = 0xC0 | ((X1) >> 6);								\
			*(X2)++ = 0x80 | ((X1) & 0x3F);							\
		} else if ((X1) <= 0xFFFF) {									\
			*(X2)++ = 0xE0 | ((X1) >> 12);								\
			*(X2)++ = 0x80 | (((X1) >> 6) & 0x3F);						\
			*(X2)++ = 0x80 | ((X1) & 0x3F);							\
		} else {														\
			*(X2)++ = 0xF0 | ((X1) >> 18);								\
			*(X2)++ = 0x80 | (((X1) >> 12) & 0x3F);					\
			*(X2)++ = 0x80 | (((X1) >> 6) & 0x3F);						\
			*(X2)++ = 0x80 | ((X1) & 0x3F);							\
		}																\
	} while (0)

#define UTF8_CHARLEN(UC) \
	((UC) <= 0x7F ? 1 : (UC) <= 0x7FF ? 2 : (UC) <= 0xFFFF ? 3 : 4)
//  (1 + ((UC) > 0x7F) + ((UC) > 0x7FF) + ((UC) > 0xFFFF))

/* The batstr module functions use a single buffer to avoid malloc/free overhead.
   Note the buffer should be always large enough to hold null strings, so less testing will be required */
#define INITIAL_STR_BUFFER_LENGTH (MAX(strlen(str_nil) + 1, 1024))

/* The batstr module functions use a single buffer to avoid malloc/free overhead.
   Note the buffer should be always large enough to hold null strings, so less testing will be required */
#define CHECK_STR_BUFFER_LENGTH(BUFFER, BUFFER_LEN, NEXT_LEN, OP) \
	do { \
		if ((NEXT_LEN) > *BUFFER_LEN) { \
			size_t newlen = (((NEXT_LEN) + 1023) & ~1023); /* align to a multiple of 1024 bytes */ \
			str newbuf = GDKmalloc(newlen); \
			if (!newbuf) \
				throw(MAL, OP, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			GDKfree(*BUFFER); \
			*BUFFER = newbuf; \
			*BUFFER_LEN = newlen; \
		} \
	} while (0)

mal_export bool batstr_func_has_candidates(const char *func);
mal_export int UTF8_strwidth(const char *s);
mal_export int UTF8_strlen(const char *s);
mal_export int str_strlen(const char *s);

/* For str returning functions, the result is passed as the input parameter buf. The returned str indicates
   if the function succeeded (ie malloc failure or invalid unicode character). str_wchr_at function also
   follows this pattern. */

/* Warning, the following functions don't test for NULL values, that's resposibility from the caller */

extern str str_from_wchr(str *buf, size_t *buflen, int c)
		__attribute__((__visibility__("hidden")));
extern str str_wchr_at(int *res, const char *s, int at)
		__attribute__((__visibility__("hidden")));

extern int str_is_prefix(const char *s, const char *prefix, int plen)
		__attribute__((__visibility__("hidden")));
extern int str_is_iprefix(const char *s, const char *prefix, int plen)
		__attribute__((__visibility__("hidden")));
extern int str_is_suffix(const char *s, const char *suffix, int sul)
		__attribute__((__visibility__("hidden")));
extern int str_is_isuffix(const char *s, const char *suffix, int sul)
		__attribute__((__visibility__("hidden")));
extern int str_contains(const char *h, const char *n, int nlen)
		__attribute__((__visibility__("hidden")));
extern int str_icontains(const char *h, const char *n, int nlen)
		__attribute__((__visibility__("hidden")));

extern str str_tail(str *buf, size_t *buflen, const char *s, int off)
		__attribute__((__visibility__("hidden")));
extern str str_Sub_String(str *buf, size_t *buflen, const char *s, int off,
						  int l)
		__attribute__((__visibility__("hidden")));
extern str str_substring_tail(str *buf, size_t *buflen, const char *s,
							  int start)
		__attribute__((__visibility__("hidden")));
extern str str_sub_string(str *buf, size_t *buflen, const char *s, int start,
						  int l)
		__attribute__((__visibility__("hidden")));
extern str str_suffix(str *buf, size_t *buflen, const char *s, int l)
		__attribute__((__visibility__("hidden")));
extern str str_repeat(str *buf, size_t *buflen, const char *s, int c)
		__attribute__((__visibility__("hidden")));

extern str str_case_hash_lock(bool upper)
		__attribute__((__visibility__("hidden")));
extern void str_case_hash_unlock(bool upper)
		__attribute__((__visibility__("hidden")));
/* Make sure the UTF8_toLowerFrom hash is locked! */
extern str str_lower(str *buf, size_t *buflen, const char *s)
		__attribute__((__visibility__("hidden")));
/* Make sure the UTF8_toUpperFrom hash is locked! */
extern str str_upper(str *buf, size_t *buflen, const char *s)
		__attribute__((__visibility__("hidden")));

extern str str_strip(str *buf, size_t *buflen, const char *s)
		__attribute__((__visibility__("hidden")));
extern str str_ltrim(str *buf, size_t *buflen, const char *s)
		__attribute__((__visibility__("hidden")));
extern str str_rtrim(str *buf, size_t *buflen, const char *s)
		__attribute__((__visibility__("hidden")));
extern str str_strip2(str *buf, size_t *buflen, const char *s, const char *s2)
		__attribute__((__visibility__("hidden")));
extern str str_ltrim2(str *buf, size_t *buflen, const char *s, const char *s2)
		__attribute__((__visibility__("hidden")));
extern str str_rtrim2(str *buf, size_t *buflen, const char *s, const char *s2)
		__attribute__((__visibility__("hidden")));
extern str str_lpad(str *buf, size_t *buflen, const char *s, int len)
		__attribute__((__visibility__("hidden")));
extern str str_rpad(str *buf, size_t *buflen, const char *s, int len)
		__attribute__((__visibility__("hidden")));
extern str str_lpad3(str *buf, size_t *buflen, const char *s, int len,
					 const char *s2)
		__attribute__((__visibility__("hidden")));
extern str str_rpad3(str *buf, size_t *buflen, const char *s, int len,
					 const char *s2)
		__attribute__((__visibility__("hidden")));

extern int str_search(const char *s, const char *needle, int needle_len)
		__attribute__((__visibility__("hidden")));
extern int str_isearch(const char *s, const char *needle, int needle_len)
		__attribute__((__visibility__("hidden")));
extern int str_reverse_str_search(const char *s, const char *needle,
								  int needle_len)
		__attribute__((__visibility__("hidden")));
extern int str_reverse_str_isearch(const char *s, const char *needle,
								   int needle_len)
		__attribute__((__visibility__("hidden")));
extern int str_locate2(const char *needle, const char *haystack, int start)
		__attribute__((__visibility__("hidden")));

extern str str_splitpart(str *buf, size_t *buflen, const char *s,
						 const char *s2, int f)
		__attribute__((__visibility__("hidden")));
extern str str_insert(str *buf, size_t *buflen, const char *s, int strt, int l,
					  const char *s2)
		__attribute__((__visibility__("hidden")));
extern str str_substitute(str *buf, size_t *buflen, const char *s,
						  const char *src, const char *dst, bit repeat)
		__attribute__((__visibility__("hidden")));

#endif /* __string_H__ */
