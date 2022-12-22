/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef __string_H__
#define __string_H__
#include "gdk.h"
#include "mal.h"
#include "mal_exception.h"
#include <ctype.h>

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

#ifndef NDEBUG
static void
UTF8_assert(const char *restrict s)
{
	int c;

	if (s == NULL)
		return;
	if (*s == '\200' && s[1] == '\0')
		return;					/* str_nil */
	while ((c = *s++) != '\0') {
		if ((c & 0x80) == 0)
			continue;
		if ((*s++ & 0xC0) != 0x80)
			assert(0);
		if ((c & 0xE0) == 0xC0)
			continue;
		if ((*s++ & 0xC0) != 0x80)
			assert(0);
		if ((c & 0xF0) == 0xE0)
			continue;
		if ((*s++ & 0xC0) != 0x80)
			assert(0);
		if ((c & 0xF8) == 0xF0)
			continue;
		assert(0);
	}
}
#else
#define UTF8_assert(s)		((void) 0)
#endif

static inline int
UTF8_strlen(const char *restrict s) /* This function assumes, s is never nil */
{
	size_t pos = 0;

	UTF8_assert(s);
	assert(!strNil(s));

	while (*s) {
		/* just count leading bytes of encoded code points; only works
		 * for correctly encoded UTF-8 */
		pos += (*s++ & 0xC0) != 0x80;
	}
	assert(pos < INT_MAX);
	return (int) pos;
}

static inline int
str_strlen(const char *restrict s)  /* This function assumes, s is never nil */
{
	size_t pos = strlen(s);
	assert(pos < INT_MAX);
	return (int) pos;
}

static inline int
UTF8_strwidth(const char *restrict s)
{
	int len = 0;
	int c;
	int n;

	if (strNil(s))
		return int_nil;
	c = 0;
	n = 0;
	while (*s != 0) {
		if ((*s & 0x80) == 0) {
			assert(n == 0);
			len++;
			n = 0;
		} else if ((*s & 0xC0) == 0x80) {
			c = (c << 6) | (*s & 0x3F);
			if (--n == 0) {
				/* last byte of a multi-byte character */
				len++;
				/* this list was created by combining
				 * the code points marked as
				 * Emoji_Presentation in
				 * /usr/share/unicode/emoji/emoji-data.txt
				 * and code points marked either F or
				 * W in EastAsianWidth.txt; this list
				 * is up-to-date with Unicode 9.0 */
				if ((0x1100 <= c && c <= 0x115F) ||
				    (0x231A <= c && c <= 0x231B) ||
				    (0x2329 <= c && c <= 0x232A) ||
				    (0x23E9 <= c && c <= 0x23EC) ||
				    c == 0x23F0 ||
				    c == 0x23F3 ||
				    (0x25FD <= c && c <= 0x25FE) ||
				    (0x2614 <= c && c <= 0x2615) ||
				    (0x2648 <= c && c <= 0x2653) ||
				    c == 0x267F ||
				    c == 0x2693 ||
				    c == 0x26A1 ||
				    (0x26AA <= c && c <= 0x26AB) ||
				    (0x26BD <= c && c <= 0x26BE) ||
				    (0x26C4 <= c && c <= 0x26C5) ||
				    c == 0x26CE ||
				    c == 0x26D4 ||
				    c == 0x26EA ||
				    (0x26F2 <= c && c <= 0x26F3) ||
				    c == 0x26F5 ||
				    c == 0x26FA ||
				    c == 0x26FD ||
				    c == 0x2705 ||
				    (0x270A <= c && c <= 0x270B) ||
				    c == 0x2728 ||
				    c == 0x274C ||
				    c == 0x274E ||
				    (0x2753 <= c && c <= 0x2755) ||
				    c == 0x2757 ||
				    (0x2795 <= c && c <= 0x2797) ||
				    c == 0x27B0 ||
				    c == 0x27BF ||
				    (0x2B1B <= c && c <= 0x2B1C) ||
				    c == 0x2B50 ||
				    c == 0x2B55 ||
				    (0x2E80 <= c && c <= 0x2E99) ||
				    (0x2E9B <= c && c <= 0x2EF3) ||
				    (0x2F00 <= c && c <= 0x2FD5) ||
				    (0x2FF0 <= c && c <= 0x2FFB) ||
				    (0x3000 <= c && c <= 0x303E) ||
				    (0x3041 <= c && c <= 0x3096) ||
				    (0x3099 <= c && c <= 0x30FF) ||
				    (0x3105 <= c && c <= 0x312D) ||
				    (0x3131 <= c && c <= 0x318E) ||
				    (0x3190 <= c && c <= 0x31BA) ||
				    (0x31C0 <= c && c <= 0x31E3) ||
				    (0x31F0 <= c && c <= 0x321E) ||
				    (0x3220 <= c && c <= 0x3247) ||
				    (0x3250 <= c && c <= 0x32FE) ||
				    (0x3300 <= c && c <= 0x4DBF) ||
				    (0x4E00 <= c && c <= 0xA48C) ||
				    (0xA490 <= c && c <= 0xA4C6) ||
				    (0xA960 <= c && c <= 0xA97C) ||
				    (0xAC00 <= c && c <= 0xD7A3) ||
				    (0xF900 <= c && c <= 0xFAFF) ||
				    (0xFE10 <= c && c <= 0xFE19) ||
				    (0xFE30 <= c && c <= 0xFE52) ||
				    (0xFE54 <= c && c <= 0xFE66) ||
				    (0xFE68 <= c && c <= 0xFE6B) ||
				    (0xFF01 <= c && c <= 0xFF60) ||
				    (0xFFE0 <= c && c <= 0xFFE6) ||
				    c == 0x16FE0 ||
				    (0x17000 <= c && c <= 0x187EC) ||
				    (0x18800 <= c && c <= 0x18AF2) ||
				    (0x1B000 <= c && c <= 0x1B001) ||
				    c == 0x1F004 ||
				    c == 0x1F0CF ||
				    c == 0x1F18E ||
				    (0x1F191 <= c && c <= 0x1F19A) ||
				    /* removed 0x1F1E6..0x1F1FF */
				    (0x1F200 <= c && c <= 0x1F202) ||
				    (0x1F210 <= c && c <= 0x1F23B) ||
				    (0x1F240 <= c && c <= 0x1F248) ||
				    (0x1F250 <= c && c <= 0x1F251) ||
				    (0x1F300 <= c && c <= 0x1F320) ||
				    (0x1F32D <= c && c <= 0x1F335) ||
				    (0x1F337 <= c && c <= 0x1F37C) ||
				    (0x1F37E <= c && c <= 0x1F393) ||
				    (0x1F3A0 <= c && c <= 0x1F3CA) ||
				    (0x1F3CF <= c && c <= 0x1F3D3) ||
				    (0x1F3E0 <= c && c <= 0x1F3F0) ||
				    c == 0x1F3F4 ||
				    (0x1F3F8 <= c && c <= 0x1F43E) ||
				    c == 0x1F440 ||
				    (0x1F442 <= c && c <= 0x1F4FC) ||
				    (0x1F4FF <= c && c <= 0x1F53D) ||
				    (0x1F54B <= c && c <= 0x1F54E) ||
				    (0x1F550 <= c && c <= 0x1F567) ||
				    c == 0x1F57A ||
				    (0x1F595 <= c && c <= 0x1F596) ||
				    c == 0x1F5A4 ||
				    (0x1F5FB <= c && c <= 0x1F64F) ||
				    (0x1F680 <= c && c <= 0x1F6C5) ||
				    c == 0x1F6CC ||
				    (0x1F6D0 <= c && c <= 0x1F6D2) ||
				    (0x1F6EB <= c && c <= 0x1F6EC) ||
				    (0x1F6F4 <= c && c <= 0x1F6F6) ||
				    (0x1F910 <= c && c <= 0x1F91E) ||
				    (0x1F920 <= c && c <= 0x1F927) ||
				    c == 0x1F930 ||
				    (0x1F933 <= c && c <= 0x1F93E) ||
				    (0x1F940 <= c && c <= 0x1F94B) ||
				    (0x1F950 <= c && c <= 0x1F95E) ||
				    (0x1F980 <= c && c <= 0x1F991) ||
				    c == 0x1F9C0 ||
				    (0x20000 <= c && c <= 0x2FFFD) ||
				    (0x30000 <= c && c <= 0x3FFFD))
					len++;
			}
		} else if ((*s & 0xE0) == 0xC0) {
			assert(n == 0);
			n = 1;
			c = *s & 0x1F;
		} else if ((*s & 0xF0) == 0xE0) {
			assert(n == 0);
			n = 2;
			c = *s & 0x0F;
		} else if ((*s & 0xF8) == 0xF0) {
			assert(n == 0);
			n = 3;
			c = *s & 0x07;
		} else if ((*s & 0xFC) == 0xF8) {
			assert(n == 0);
			n = 4;
			c = *s & 0x03;
		} else {
			assert(0);
			n = 0;
		}
		s++;
	}
	return len;
}

mal_export bool batstr_func_has_candidates(const char *func);

/* For str returning functions, the result is passed as the input parameter buf. The returned str indicates
   if the function succeeded (ie malloc failure or invalid unicode character). str_wchr_at function also
   follows this pattern. */

/* Warning, the following functions don't test for NULL values, that's resposibility from the caller */

extern str str_from_wchr(str *buf, size_t *buflen, int c)
__attribute__((__visibility__("hidden")));
extern str str_wchr_at(int *res, const char *s, int at)
__attribute__((__visibility__("hidden")));

extern bit str_is_prefix(const char *s, const char *prefix)
__attribute__((__visibility__("hidden")));
extern bit str_is_suffix(const char *s, const char *suffix)
__attribute__((__visibility__("hidden")));

extern str str_tail(str *buf, size_t *buflen, const char *s, int off)
__attribute__((__visibility__("hidden")));
extern str str_Sub_String(str *buf, size_t *buflen, const char *s, int off, int l)
__attribute__((__visibility__("hidden")));
extern str str_substring_tail(str *buf, size_t *buflen, const char *s, int start)
__attribute__((__visibility__("hidden")));
extern str str_sub_string(str *buf, size_t *buflen, const char *s, int start, int l)
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
extern str str_lpad3(str *buf, size_t *buflen, const char *s, int len, const char *s2)
__attribute__((__visibility__("hidden")));
extern str str_rpad3(str *buf, size_t *buflen, const char *s, int len, const char *s2)
__attribute__((__visibility__("hidden")));

extern int str_search(const char *s, const char *s2)
__attribute__((__visibility__("hidden")));
extern int str_reverse_str_search(const char *s, const char *s2)
__attribute__((__visibility__("hidden")));
extern int str_locate2(const char *needle, const char *haystack, int start)
__attribute__((__visibility__("hidden")));

extern str str_splitpart(str *buf, size_t *buflen, const char *s, const char *s2, int f)
__attribute__((__visibility__("hidden")));
extern str str_insert(str *buf, size_t *buflen, const char *s, int strt, int l, const char *s2)
__attribute__((__visibility__("hidden")));
extern str str_substitute(str *buf, size_t *buflen, const char *s, const char *src, const char *dst, bit repeat)
__attribute__((__visibility__("hidden")));

#endif /* __string_H__ */
