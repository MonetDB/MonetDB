/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

mal_export bool batstr_func_has_candidates(const char *func);

/* For str returning functions, the result is passed as the input parameter buf. The returned str indicates
   if the function succeeded (ie malloc failure or invalid unicode character). str_wchr_at function also
   follows this pattern. */

/* Warning, the following functions don't test for NULL values, that's resposibility from the caller */

extern str str_from_wchr(str *buf, size_t *buflen, int c)
__attribute__((__visibility__("hidden")));
extern str str_wchr_at(int *res, str s, int at)
__attribute__((__visibility__("hidden")));

extern bit str_is_prefix(str s, str prefix)
__attribute__((__visibility__("hidden")));
extern bit str_is_suffix(str s, str suffix)
__attribute__((__visibility__("hidden")));

extern str str_tail(str *buf, size_t *buflen, str s, int off)
__attribute__((__visibility__("hidden")));
extern str str_Sub_String(str *buf, size_t *buflen, str s, int off, int l)
__attribute__((__visibility__("hidden")));
extern str str_substring_tail(str *buf, size_t *buflen, str s, int start)
__attribute__((__visibility__("hidden")));
extern str str_sub_string(str *buf, size_t *buflen, str s, int start, int l)
__attribute__((__visibility__("hidden")));
extern str str_suffix(str *buf, size_t *buflen, str s, int l)
__attribute__((__visibility__("hidden")));
extern str str_repeat(str *buf, size_t *buflen, str s, int c)
__attribute__((__visibility__("hidden")));

extern str str_lower(str *buf, size_t *buflen, str s)
__attribute__((__visibility__("hidden")));
extern str str_upper(str *buf, size_t *buflen, str s)
__attribute__((__visibility__("hidden")));

extern str str_strip(str *buf, size_t *buflen, str s)
__attribute__((__visibility__("hidden")));
extern str str_ltrim(str *buf, size_t *buflen, str s)
__attribute__((__visibility__("hidden")));
extern str str_rtrim(str *buf, size_t *buflen, str s)
__attribute__((__visibility__("hidden")));
extern str str_strip2(str *buf, size_t *buflen, str s, str s2)
__attribute__((__visibility__("hidden")));
extern str str_ltrim2(str *buf, size_t *buflen, str s, str s2)
__attribute__((__visibility__("hidden")));
extern str str_rtrim2(str *buf, size_t *buflen, str s, str s2)
__attribute__((__visibility__("hidden")));
extern str str_lpad(str *buf, size_t *buflen, str s, int len)
__attribute__((__visibility__("hidden")));
extern str str_rpad(str *buf, size_t *buflen, str s, int len)
__attribute__((__visibility__("hidden")));
extern str str_lpad3(str *buf, size_t *buflen, str s, int len, str s2)
__attribute__((__visibility__("hidden")));
extern str str_rpad3(str *buf, size_t *buflen, str s, int len, str s2)
__attribute__((__visibility__("hidden")));

extern int str_search(str s, str s2)
__attribute__((__visibility__("hidden")));
extern int str_reverse_str_search(str s, str s2)
__attribute__((__visibility__("hidden")));
extern int str_locate2(str needle, str haystack, int start)
__attribute__((__visibility__("hidden")));

extern str str_splitpart(str *buf, size_t *buflen, str s, str s2, int f)
__attribute__((__visibility__("hidden")));
extern str str_insert(str *buf, size_t *buflen, str s, int strt, int l, str s2)
__attribute__((__visibility__("hidden")));
extern str str_substitute(str *buf, size_t *buflen, str s, str src, str dst, bit repeat)
__attribute__((__visibility__("hidden")));

#endif /* __string_H__ */
