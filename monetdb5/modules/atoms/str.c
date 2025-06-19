/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 *  N.J. Nes, M.L. Kersten
 * The String Module
 * Strings can be created in many ways. Already in the built-in
 * operations each atom can be cast to a string using the str(atom)
 * mil command.  The string module gives the possibility of
 * construction string as a substring of the a given string (s). There
 * are two such construction functions.  The first is the substring
 * from some position (offset) until the end of the string. The second
 * start again on the given offset position but only copies count
 * number of bytes. The functions fail when the position and count
 * fall out of bounds. A negative position indicates that the position
 * is computed from the end of the source string.
 *
 * The strings can be compared using the "=" and "!=" operators.
 *
 * The operator "+" concatenates a string and an atom. The atom will
 * be converted to a string using the atom to string c function. The
 * string and the result of the conversion are concatenated to form a
 * new string. This string is returned.
 *
 * The length function returns the length of the string. The length is
 * the number of characters in the string. The maximum string length
 * handled by the kernel is 32-bits long.
 *
 * chrAt() returns the character at position index in the string
 * s. The function will fail when the index is out of range. The range
 * is from 0 to length(s)-1.
 *
 * The startsWith and endsWith functions test if the string s starts
 * with or ends with the given prefix or suffix.
 *
 * The toLower and toUpper functions cast the string to lower or upper
 * case characters.
 *
 * The search(str,chr) function searches for the first occurrence of a
 * character from the beginning of the string. The search(chr,str)
 * searches for the last occurrence (or first from the end of the
 * string). The last search function locates the position of first
 * occurrence of the string s2 in string s. All search functions
 * return -1 when the search failed.  Otherwise the position is
 * returned.
 *
 * All string functions fail when an incorrect string (NULL pointer)
 * is given.  In the current implementation, a fail is signaled by
 * returning nil, since this facilitates the use of the string module
 * in bulk operations.
 *
 * All functions in the module have now been converted to
 * Unicode. Internally, we use UTF-8 to store strings as Unicode in
 * zero-terminated byte-sequences.
 */
#include "monetdb_config.h"
#include "str.h"
#include <string.h>
#include "mal_interpreter.h"
#include "mutf8.h"
#include "bigram.h"

#define UTF8_assert(s)		assert(checkUTF8(s))

/* return the number of codepoints in `s' before `end'. */
static inline int
UTF8_strpos(const char *s, const char *end)
{
	int pos = 0;

	UTF8_assert(s);

	if (s > end) {
		return -1;
	}
	while (s < end) {
		/* just count leading bytes of encoded code points; only works
		 * for correctly encoded UTF-8 */
		pos += (*s++ & 0xC0) != 0x80;
	}
	return pos;
}

/* return a pointer to the byte that starts the pos'th (0-based)
 * codepoint in s */
static inline char *
UTF8_strtail(const char *s, int pos)
{
	UTF8_assert(s);
	while (*s) {
		if ((*s & 0xC0) != 0x80) {
			if (pos <= 0)
				break;
			pos--;
		}
		s++;
	}
	return (char *) s;
}

/* copy n Unicode codepoints from s to dst, return pointer to new end */
static inline str
UTF8_strncpy(char *restrict dst, const char *restrict s, int n)
{
	UTF8_assert(s);
	while (*s && n) {
		if ((*s & 0xF8) == 0xF0) {
			/* 4 byte UTF-8 sequence */
			*dst++ = *s++;
			*dst++ = *s++;
			*dst++ = *s++;
			*dst++ = *s++;
		} else if ((*s & 0xF0) == 0xE0) {
			/* 3 byte UTF-8 sequence */
			*dst++ = *s++;
			*dst++ = *s++;
			*dst++ = *s++;
		} else if ((*s & 0xE0) == 0xC0) {
			/* 2 byte UTF-8 sequence */
			*dst++ = *s++;
			*dst++ = *s++;
		} else {
			/* 1 byte UTF-8 "sequence" */
			*dst++ = *s++;
		}
		n--;
	}
	*dst = '\0';
	return dst;
}

/* return number of Unicode codepoints in s; s is not nil */
int
UTF8_strlen(const char *s)
{								/* This function assumes, s is never nil */
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

/* return (int) strlen(s); s is not nil */
int
str_strlen(const char *s)
{								/* This function assumes s is never nil */
	UTF8_assert(s);
	assert(!strNil(s));

	return (int) strlen(s);
}

/* return the display width of s */
int
UTF8_strwidth(const char *S)
{
	if (strNil(S))
		return int_nil;

	const uint8_t *s = (const uint8_t *) S;
	int len = 0;

	for (uint32_t state = 0, codepoint = 0; *s; s++) {
		switch (decode(&state, &codepoint, (uint8_t) *s)) {
		case UTF8_ACCEPT: {
			int n = charwidth(codepoint);
			if (n >= 0)
				len += n;
			else
				len++;			/* assume width 1 if unprintable */
			break;
		}
		default:
			break;
		case UTF8_REJECT:
			assert(0);
		}
	}
	return len;
}

/*
 * Here you find the wrappers around the version 4 library code
 * It also contains the direct implementation of the string
 * matching support routines.
 */
#include "mal_exception.h"

/*
 * The SQL like function return a boolean
 */
static bool
STRlike(const char *s, const char *pat, const char *esc)
{
	const char *t, *p;

	t = s;
	for (p = pat; *p && *t; p++) {
		if (esc && *p == *esc) {
			p++;
			if (*p != *t)
				return false;
			t++;
		} else if (*p == '_')
			t++;
		else if (*p == '%') {
			p++;
			while (*p == '%')
				p++;
			if (*p == 0)
				return true;	/* tail is acceptable */
			for (; *p && *t; t++)
				if (STRlike(t, p, esc))
					return true;
			if (*p == 0 && *t == 0)
				return true;
			return false;
		} else if (*p == *t)
			t++;
		else
			return false;
	}
	if (*p == '%' && *(p + 1) == 0)
		return true;
	return *t == 0 && *p == 0;
}

static str
STRlikewrap3(Client ctx, bit *ret, const char *const *s, const char *const *pat, const char *const *esc)
{
	(void) ctx;
	if (strNil(*s) || strNil(*pat) || strNil(*esc))
		*ret = bit_nil;
	else
		*ret = (bit) STRlike(*s, *pat, *esc);
	return MAL_SUCCEED;
}

static str
STRlikewrap(Client ctx, bit *ret, const char *const *s, const char *const *pat)
{
	(void) ctx;
	if (strNil(*s) || strNil(*pat))
		*ret = bit_nil;
	else
		*ret = (bit) STRlike(*s, *pat, NULL);
	return MAL_SUCCEED;
}

static str
STRtostr(Client ctx, str *res, const char *const *src)
{
	(void) ctx;
	if (*src == 0)
		*res = MA_STRDUP(ctx->alloc, str_nil);
	else
		*res = MA_STRDUP(ctx->alloc, *src);
	if (*res == NULL)
		throw(MAL, "str.str", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
STRLength(Client ctx, int *res, const char *const *arg1)
{
	(void) ctx;
	const char *s = *arg1;

	*res = strNil(s) ? int_nil : UTF8_strlen(s);
	return MAL_SUCCEED;
}

static str
STRBytes(Client ctx, int *res, const char *const *arg1)
{
	(void) ctx;
	const char *s = *arg1;

	*res = strNil(s) ? int_nil : str_strlen(s);
	return MAL_SUCCEED;
}

str
str_tail(str *buf, size_t *buflen, const char *s, int off)
{
	if (off < 0) {
		off += UTF8_strlen(s);
		if (off < 0)
			off = 0;
	}
	const char *tail = UTF8_strtail(s, off);
	size_t nextlen = strlen(tail) + 1;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, nextlen, "str.tail");
	strcpy(*buf, tail);
	return MAL_SUCCEED;
}

static str
STRTail(Client ctx, str *res, const char *const *arg1, const int *offset)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int off = *offset;

	if (strNil(s) || is_int_nil(off)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.tail", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_tail(&buf, &buflen, s, off)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.tail", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

/* copy the substring s[off:off+l] into *buf, replacing *buf with a
 * freshly allocated buffer if the substring doesn't fit; off is 0
 * based, and both off and l count in Unicode codepoints (i.e. not
 * bytes); if off < 0, off counts from the end of the string */
str
str_Sub_String(str *buf, size_t *buflen, const char *s, int off, int l)
{
	size_t len;

	if (off < 0) {
		off += UTF8_strlen(s);
		if (off < 0) {
			l += off;
			off = 0;
		}
	}
	/* here, off >= 0 */
	if (l < 0) {
		strcpy(*buf, "");
		return MAL_SUCCEED;
	}
	s = UTF8_strtail(s, off);
	len = (size_t) (UTF8_strtail(s, l) - s + 1);
	CHECK_STR_BUFFER_LENGTH(buf, buflen, len, "str.substring");
	strcpy_len(*buf, s, len);
	return MAL_SUCCEED;
}

static str
STRSubString(Client ctx, str *res, const char *const *arg1, const int *offset, const int *length)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int off = *offset, len = *length;

	if (strNil(s) || is_int_nil(off) || is_int_nil(len)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_Sub_String(&buf, &buflen, s, off, len)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.substring",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_from_wchr(str *buf, size_t *buflen, int c)
{
	CHECK_STR_BUFFER_LENGTH(buf, buflen, 5, "str.unicode");
	str s = *buf;
	UTF8_PUTCHAR(c, s);
	*s = 0;
	return MAL_SUCCEED;
  illegal:
	throw(MAL, "str.unicode", SQLSTATE(42000) "Illegal Unicode code point");
}

static str
STRFromWChr(Client ctx, str *res, const int *c)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	int cc = *c;

	if (is_int_nil(cc)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = MAX(strlen(str_nil) + 1, 8);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_from_wchr(&buf, &buflen, cc)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.unicode",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

/* return the Unicode code point of arg1 at position at */
str
str_wchr_at(int *res, const char *s, int at)
{
	/* 64bit: should have lng arg */
	if (strNil(s) || is_int_nil(at) || at < 0) {
		*res = int_nil;
		return MAL_SUCCEED;
	}
	s = UTF8_strtail(s, at);
	if (s == NULL || *s == 0) {
		*res = int_nil;
		return MAL_SUCCEED;
	}
	uint32_t state = 0, codepoint;
	while (*s) {
		if (decode(&state, &codepoint, (uint8_t) *s) == UTF8_ACCEPT) {
			*res = codepoint;
			return MAL_SUCCEED;
		}
		s++;
	}
	throw(MAL, "str.unicodeAt", SQLSTATE(42000) "Illegal Unicode code point");
}

static str
STRWChrAt(Client ctx, int *res, const char *const *arg1, const int *at)
{
	(void) ctx;
	return str_wchr_at(res, *arg1, *at);
}

static inline str
doStrConvert(allocator *alloc, str *res, const char *arg1, gdk_return (*func)(char **restrict, size_t *restrict, const char *restrict))
{
	str buf = NULL, msg = MAL_SUCCEED;

	if (strNil(arg1)) {
		*res = MA_STRDUP(alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.lower", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((*func)(&buf, &buflen, arg1) != GDK_SUCCEED) {
			GDKfree(buf);
			throw(MAL, "str.lower", GDK_EXCEPTION);
		}
		*res = MA_STRDUP(alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.lower",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static inline str
STRlower(Client ctx, str *res, const char *const *arg1)
{
	(void) ctx;
	return doStrConvert(ctx->alloc, res, *arg1, GDKtolower);
}

static inline str
STRupper(Client ctx, str *res, const char *const *arg1)
{
	(void) ctx;
	return doStrConvert(ctx->alloc, res, *arg1, GDKtoupper);
}

static inline str
STRcasefold(Client ctx, str *res, const char *const *arg1)
{
	(void) ctx;
	return doStrConvert(ctx->alloc, res, *arg1, GDKcasefold);
}

/* returns whether arg1 starts with arg2 */
int
str_is_prefix(const char *s, const char *prefix, int plen)
{
	return strncmp(s, prefix, plen);
}

int
str_is_iprefix(const char *s, const char *prefix, int plen)
{
	return GDKstrncasecmp(s, prefix, SIZE_MAX, plen);
}

int
str_is_suffix(const char *s, const char *suffix, int sul)
{
	int sl = str_strlen(s);

	if (sl < sul)
		return -1;
	else
		return strcmp(s + sl - sul, suffix);
}

/* case insensitive endswith check */
int
str_is_isuffix(const char *s, const char *suffix, int sul)
{
	const char *e = s + strlen(s);
	const char *sf;

	(void) sul;
	/* note that the uppercase and lowercase forms of a character aren't
	 * necessarily the same length in their UTF-8 encodings */
	for (sf = suffix; *sf && e > s; sf++) {
		if ((*sf & 0xC0) != 0x80) {
			while ((*--e & 0xC0) == 0x80)
				;
		}
	}
	while ((*sf & 0xC0) == 0x80)
		sf++;
	return *sf != 0 || GDKstrcasecmp(e, suffix) != 0;
}

int
str_contains(const char *h, const char *n, int nlen)
{
	(void) nlen;
	return strstr(h, n) == NULL;
}

int
str_icontains(const char *h, const char *n, int nlen)
{
	(void) nlen;
	return GDKstrcasestr(h, n) == NULL;
}

static str
STRstartswith(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	bit *r = getArgReference_bit(stk, pci, 0);
	const char *s1 = *getArgReference_str(stk, pci, 1);
	const char *s2 = *getArgReference_str(stk, pci, 2);
	bit icase = pci->argc == 4 && *getArgReference_bit(stk, pci, 3);

	if (strNil(s1) || strNil(s2)) {
		*r = bit_nil;
	} else {
		int s2_len = str_strlen(s2);
		*r = icase ?
			str_is_iprefix(s1, s2, s2_len) == 0 :
			str_is_prefix(s1, s2, s2_len) == 0;
	}
	return MAL_SUCCEED;
}

static str
STRendswith(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	bit *r = getArgReference_bit(stk, pci, 0);
	const char *s1 = *getArgReference_str(stk, pci, 1);
	const char *s2 = *getArgReference_str(stk, pci, 2);
	bit icase = pci->argc == 4 && *getArgReference_bit(stk, pci, 3);

	if (strNil(s1) || strNil(s2)) {
		*r = bit_nil;
	} else {
		int s2_len = str_strlen(s2);
		*r = icase ?
			str_is_isuffix(s1, s2, s2_len) == 0 :
			str_is_suffix(s1, s2, s2_len) == 0;
	}
	return MAL_SUCCEED;
}

/* returns whether haystack contains needle */
static str
STRcontains(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	bit *r = getArgReference_bit(stk, pci, 0);
	const char *s1 = *getArgReference_str(stk, pci, 1);
	const char *s2 = *getArgReference_str(stk, pci, 2);
	bit icase = pci->argc == 4 && *getArgReference_bit(stk, pci, 3);

	if (strNil(s1) || strNil(s2)) {
		*r = bit_nil;
	} else {
		int s2_len = str_strlen(s2);
		*r = icase ?
			str_icontains(s1, s2, s2_len) == 0 :
			str_contains(s1, s2, s2_len) == 0;
	}
	return MAL_SUCCEED;
}

int
str_search(const char *haystack, const char *needle)
{
	needle = strstr(haystack, needle);
	if (needle == NULL)
		return -1;

	return UTF8_strpos(haystack, needle);
}

int
str_isearch(const char *haystack, const char *needle)
{
	needle = GDKstrcasestr(haystack, needle);
	if (needle == NULL)
		return -1;

	return UTF8_strpos(haystack, needle);
}

/* find first occurrence of needle in haystack */
static str
STRstr_search(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	bit *res = getArgReference(stk, pci, 0);
	const char *haystack = *getArgReference_str(stk, pci, 1);
	const char *needle = *getArgReference_str(stk, pci, 2);
	bit icase = pci->argc == 4 && *getArgReference_bit(stk, pci, 3);

	if (strNil(haystack) || strNil(needle)) {
		*res = bit_nil;
	} else {
		*res = icase ?
			str_isearch(haystack, needle) :
			str_search(haystack, needle);
	}
	return MAL_SUCCEED;
}

int
str_reverse_str_search(const char *haystack, const char *needle)
{
	int nulen = UTF8_strlen(needle);
	size_t nlen = strlen(needle);

	for (int pos = str_strlen(haystack) - 1; pos >= 0; pos--) {
		if ((haystack[pos] & 0xC0) != 0x80) {
			if (nulen > 0)
				nulen--;
			else if (strncmp(haystack + pos, needle, nlen) == 0)
				return pos;
		}
	}
	return -1;
}

int
str_reverse_str_isearch(const char *haystack, const char *needle)
{
	int nulen = UTF8_strlen(needle);
	size_t nlen = strlen(needle);

	for (int pos = str_strlen(haystack) - 1; pos >= 0; pos--) {
		if ((haystack[pos] & 0xC0) != 0x80) {
			if (nulen > 0)
				nulen--;
			else if (GDKstrncasecmp(haystack + pos, needle, SIZE_MAX, nlen) == 0)
				return pos;
		}
	}
	return -1;
}

/* find last occurrence of arg2 in arg1 */
static str
STRrevstr_search(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	int *res = getArgReference_int(stk, pci, 0);
	const str haystack = *getArgReference_str(stk, pci, 1);
	const str needle = *getArgReference_str(stk, pci, 2);
	bit icase = pci->argc == 4 && *getArgReference_bit(stk, pci, 3);

	if (strNil(haystack) || strNil(needle)) {
		*res = bit_nil;
	} else {
		*res = icase ?
			str_reverse_str_isearch(haystack, needle) :
			str_reverse_str_search(haystack, needle);
	}
	return MAL_SUCCEED;
}

str
str_splitpart(str *buf, size_t *buflen, const char *s, const char *s2, int f)
{
	size_t len;
	char *p = NULL;

	if (f <= 0)
		throw(MAL, "str.splitpart",
			  SQLSTATE(42000) "field position must be greater than zero");

	len = strlen(s2);
	if (len) {
		while ((p = strstr(s, s2)) != NULL && f > 1) {
			s = p + len;
			f--;
		}
	}

	if (f != 1) {
		strcpy(*buf, "");
		return MAL_SUCCEED;
	}

	if (p == NULL) {
		len = strlen(s);
	} else {
		len = (size_t) (p - s);
	}

	len++;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, len, "str.splitpart");
	strcpy_len(*buf, s, len);
	return MAL_SUCCEED;
}

static str
STRsplitpart(Client ctx, str *res, const char *const *haystack, const char *const *needle, const int *field)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *haystack, *s2 = *needle;
	int f = *field;

	if (strNil(s) || strNil(s2) || is_int_nil(f)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_splitpart(&buf, &buflen, s, s2, f)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.splitpart",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

/* returns number of bytes to remove from left to strip the codepoints in rm */
static size_t
lstrip(const char *s, size_t len, const uint32_t *rm, size_t nrm)
{
	uint32_t state = 0, codepoint;
	size_t skip = 0;

	for (size_t n = 0; n < len;) {
		if (decode(&state, &codepoint, (uint8_t) s[n++]) == UTF8_ACCEPT) {
			size_t i;
			for (i = 0; i < nrm; i++) {
				if (rm[i] == codepoint) {
					break;
				}
			}
			if (i == nrm)
				return skip;
			skip = n;
		}
	}
	return skip;
}

/* returns the resulting length of s after stripping codepoints in rm
 * from the right */
static size_t
rstrip(const char *s, size_t len, const uint32_t *rm, size_t nrm)
{
	uint32_t c;
	size_t i, n;

	while (len > 0) {
		UTF8_LASTCHAR(c, n, s, len);
		assert(n > 0 && n <= len);
		for (i = 0; i < nrm; i++) {
			if (rm[i] == c) {
				len -= n;
				break;
			}
		}
		if (i == nrm)
			break;
	}
	return len;
}

const uint32_t whitespace[] = {
	' ',						/* space */
	'\t',						/* tab (character tabulation) */
	'\n',						/* line feed */
	'\r',						/* carriage return */
	'\f',						/* form feed */
	'\v',						/* vertical tab (line tabulation) */
/* below the code points that have the Unicode Zs (space separator) property */
	0x00A0,						/* no-break space */
	0x1680,						/* ogham space mark */
	0x2000,						/* en quad */
	0x2001,						/* em quad */
	0x2002,						/* en space */
	0x2003,						/* em space */
	0x2004,						/* three-per-em space */
	0x2005,						/* four-per-em space */
	0x2006,						/* six-per-em space */
	0x2007,						/* figure space */
	0x2008,						/* punctuation space */
	0x2009,						/* thin space */
	0x200A,						/* hair space */
	0x202F,						/* narrow no-break space */
	0x205F,						/* medium mathematical space */
	0x3000,						/* ideographic space */
/* below the code points that have the Unicode Zl (line separator) property */
	0x2028,						/* line separator */
/* below the code points that have the Unicode Zp (paragraph separator)
 * property */
	0x2029,						/* paragraph separator */
};

#define NSPACES		(sizeof(whitespace) / sizeof(whitespace[0]))

str
str_strip(str *buf, size_t *buflen, const char *s)
{
	size_t len = strlen(s);
	size_t n = lstrip(s, len, whitespace, NSPACES);
	s += n;
	len -= n;
	n = rstrip(s, len, whitespace, NSPACES);

	n++;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, n, "str.strip");
	strcpy_len(*buf, s, n);
	return MAL_SUCCEED;
}

/* remove all whitespace from either side of arg1 */
static str
STRStrip(Client ctx, str *res, const char *const *arg1)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;

	if (strNil(s)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.strip", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_strip(&buf, &buflen, s)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.strip",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_ltrim(str *buf, size_t *buflen, const char *s)
{
	size_t len = strlen(s);
	size_t n = lstrip(s, len, whitespace, NSPACES);
	size_t nallocate = len - n + 1;

	CHECK_STR_BUFFER_LENGTH(buf, buflen, nallocate, "str.ltrim");
	strcpy_len(*buf, s + n, nallocate);
	return MAL_SUCCEED;
}

/* remove all whitespace from the start (left) of arg1 */
static str
STRLtrim(Client ctx, str *res, const char *const *arg1)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;

	if (strNil(s)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.ltrim", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_ltrim(&buf, &buflen, s)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.ltrim",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_rtrim(str *buf, size_t *buflen, const char *s)
{
	size_t len = strlen(s);
	size_t n = rstrip(s, len, whitespace, NSPACES);

	n++;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, n, "str.rtrim");
	strcpy_len(*buf, s, n);
	return MAL_SUCCEED;
}

/* remove all whitespace from the end (right) of arg1 */
static str
STRRtrim(Client ctx, str *res, const char *const *arg1)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;

	if (strNil(s)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rtrim", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rtrim(&buf, &buflen, s)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.rtrim",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

/* return a list of codepoints in s */
static str
trimchars(str *buf, size_t *buflen, size_t *n, const char *s, size_t len_s,
		  const char *malfunc)
{
	size_t len = 0, nlen = len_s * sizeof(int);
	uint32_t *cbuf;

	assert(s);
	CHECK_STR_BUFFER_LENGTH(buf, buflen, nlen, malfunc);
	cbuf = *(uint32_t **) buf;

	uint32_t state = 0;
	uint32_t codepoint;
	while (*s) {
		if (decode(&state, &codepoint, (uint8_t) *s) == UTF8_ACCEPT) {
			cbuf[len++] = codepoint;
		}
		s++;
	}
	if (state != UTF8_ACCEPT)
		throw(MAL, malfunc, SQLSTATE(42000) "Illegal Unicode code point");
	*n = len;
	return MAL_SUCCEED;
}

str
str_strip2(str *buf, size_t *buflen, const char *s, const char *s2)
{
	str msg = MAL_SUCCEED;
	size_t len, n, n2, n3;

	if ((n2 = strlen(s2)) == 0) {
		len = strlen(s) + 1;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, len, "str.strip2");
		strcpy(*buf, s);
		return MAL_SUCCEED;
	} else {
		if ((msg = trimchars(buf, buflen, &n3, s2, n2, "str.strip2")) != MAL_SUCCEED)
			return msg;
		len = strlen(s);
		n = lstrip(s, len, *(uint32_t **) buf, n3);
		s += n;
		len -= n;
		n = rstrip(s, len, *(uint32_t **) buf, n3);

		n++;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, n, "str.strip2");
		strcpy_len(*buf, s, n);
		return MAL_SUCCEED;
	}
}

/* remove the longest string containing only characters from arg2 from
 * either side of arg1 */
static str
STRStrip2(Client ctx, str *res, const char *const *arg1, const char *const *arg2)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;

	if (strNil(s) || strNil(s2)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH * sizeof(int);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.strip2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_strip2(&buf, &buflen, s, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.strip2",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_ltrim2(str *buf, size_t *buflen, const char *s, const char *s2)
{
	str msg = MAL_SUCCEED;
	size_t len, n, n2, n3, nallocate;

	if ((n2 = strlen(s2)) == 0) {
		len = strlen(s) + 1;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, len, "str.ltrim2");
		strcpy(*buf, s);
		return MAL_SUCCEED;
	} else {
		if ((msg = trimchars(buf, buflen, &n3, s2, n2, "str.ltrim2")) != MAL_SUCCEED)
			return msg;
		len = strlen(s);
		n = lstrip(s, len, *(uint32_t **) buf, n3);
		nallocate = len - n + 1;

		CHECK_STR_BUFFER_LENGTH(buf, buflen, nallocate, "str.ltrim2");
		strcpy_len(*buf, s + n, nallocate);
		return MAL_SUCCEED;
	}
}

/* remove the longest string containing only characters from arg2 from
 * the start (left) of arg1 */
static str
STRLtrim2(Client ctx, str *res, const char *const *arg1, const char *const *arg2)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;

	if (strNil(s) || strNil(s2)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH * sizeof(int);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.ltrim2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_ltrim2(&buf, &buflen, s, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.ltrim2",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_rtrim2(str *buf, size_t *buflen, const char *s, const char *s2)
{
	str msg = MAL_SUCCEED;
	size_t len, n, n2, n3;

	if ((n2 = strlen(s2)) == 0) {
		len = strlen(s) + 1;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, len, "str.rtrim2");
		strcpy(*buf, s);
		return MAL_SUCCEED;
	} else {
		if ((msg = trimchars(buf, buflen, &n3, s2, n2, "str.ltrim2")) != MAL_SUCCEED)
			return msg;
		len = strlen(s);
		n = rstrip(s, len, *(uint32_t **) buf, n3);
		n++;

		CHECK_STR_BUFFER_LENGTH(buf, buflen, n, "str.rtrim2");
		strcpy_len(*buf, s, n);
		return MAL_SUCCEED;
	}
}

/* remove the longest string containing only characters from arg2 from
 * the end (right) of arg1 */
static str
STRRtrim2(Client ctx, str *res, const char *const *arg1, const char *const *arg2)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;

	if (strNil(s) || strNil(s2)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH * sizeof(int);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rtrim2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rtrim2(&buf, &buflen, s, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.rtrim2",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
pad(str *buf, size_t *buflen, const char *s, const char *pad, int len, int left,
	const char *malfunc)
{
	size_t slen, padlen, repeats, residual, i, nlen;
	char *res;

	if (len < 0)
		len = 0;

	slen = (size_t) UTF8_strlen(s);
	if (slen > (size_t) len) {
		/* truncate */
		pad = UTF8_strtail(s, len);
		slen = pad - s + 1;

		CHECK_STR_BUFFER_LENGTH(buf, buflen, slen, malfunc);
		strcpy_len(*buf, s, slen);
		return MAL_SUCCEED;
	}

	padlen = (size_t) UTF8_strlen(pad);
	if (slen == (size_t) len || padlen == 0) {
		/* nothing to do (no padding if there is no pad string) */
		slen = strlen(s) + 1;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, slen, malfunc);
		strcpy(*buf, s);
		return MAL_SUCCEED;
	}

	repeats = ((size_t) len - slen) / padlen;
	residual = ((size_t) len - slen) % padlen;
	if (residual > 0)
		residual = (size_t) (UTF8_strtail(pad, (int) residual) - pad);
	padlen = strlen(pad);
	slen = strlen(s);

	nlen = slen + repeats * padlen + residual + 1;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, nlen, malfunc);
	res = *buf;
	if (left) {
		for (i = 0; i < repeats; i++)
			memcpy(res + i * padlen, pad, padlen);
		if (residual > 0)
			memcpy(res + repeats * padlen, pad, residual);
		if (slen > 0)
			memcpy(res + repeats * padlen + residual, s, slen);
	} else {
		if (slen > 0)
			memcpy(res, s, slen);
		for (i = 0; i < repeats; i++)
			memcpy(res + slen + i * padlen, pad, padlen);
		if (residual > 0)
			memcpy(res + slen + repeats * padlen, pad, residual);
	}
	res[repeats * padlen + residual + slen] = 0;
	return MAL_SUCCEED;
}

str
str_lpad(str *buf, size_t *buflen, const char *s, int len)
{
	return pad(buf, buflen, s, " ", len, 1, "str.lpad");
}

/* Fill up 'arg1' to length 'len' by prepending whitespaces.
 * If 'arg1' is already longer than 'len', then it's truncated on the right
 * (NB: this is the PostgreSQL definition).
 *
 * Example: lpad('hi', 5)
 * Result: '   hi'
 */
static str
STRLpad(Client ctx, str *res, const char *const *arg1, const int *len)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *len;

	if (strNil(s) || is_int_nil(l)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.lpad", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_lpad(&buf, &buflen, s, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.lpad", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_rpad(str *buf, size_t *buflen, const char *s, int len)
{
	return pad(buf, buflen, s, " ", len, 0, "str.lpad");
}

/* Fill up 'arg1' to length 'len' by appending whitespaces.
 * If 'arg1' is already longer than 'len', then it's truncated (on the right)
 * (NB: this is the PostgreSQL definition).
 *
 * Example: rpad('hi', 5)
 * Result: 'hi   '
 */
static str
STRRpad(Client ctx, str *res, const char *const *arg1, const int *len)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *len;

	if (strNil(s) || is_int_nil(l)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rpad", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rpad(&buf, &buflen, s, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.rpad", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_lpad3(str *buf, size_t *buflen, const char *s, int len, const char *s2)
{
	return pad(buf, buflen, s, s2, len, 1, "str.lpad2");
}

/* Fill up 'arg1' to length 'len' by prepending characters from 'arg2'
 * If 'arg1' is already longer than 'len', then it's truncated on the right
 * (NB: this is the PostgreSQL definition).
 *
 * Example: lpad('hi', 5, 'xy')
 * Result: xyxhi
 */
static str
STRLpad3(Client ctx, str *res, const char *const *arg1, const int *len, const char *const *arg2)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;
	int l = *len;

	if (strNil(s) || strNil(s2) || is_int_nil(l)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.lpad2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_lpad3(&buf, &buflen, s, l, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.lpad2",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_rpad3(str *buf, size_t *buflen, const char *s, int len, const char *s2)
{
	return pad(buf, buflen, s, s2, len, 0, "str.rpad2");
}

/* Fill up 'arg1' to length 'len' by appending characters from 'arg2'
 * If 'arg1' is already longer than 'len', then it's truncated (on the right)
 * (NB: this is the PostgreSQL definition).
 *
 * Example: rpad('hi', 5, 'xy')
 * Result: hixyx
 */
static str
STRRpad3(Client ctx, str *res, const char *const *arg1, const int *len, const char *const *arg2)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;
	int l = *len;

	if (strNil(s) || strNil(s2) || is_int_nil(l)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rpad2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rpad3(&buf, &buflen, s, l, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.rpad2",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_substitute(str *buf, size_t *buflen, const char *s, const char *src,
			   const char *dst, bit repeat)
{
	size_t lsrc = strlen(src), ldst = strlen(dst), n, l = strlen(s);
	char *b, *fnd;
	const char *pfnd;

	if (!lsrc || !l) {			/* s/src is an empty string, there's nothing to substitute */
		l++;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, l, "str.substitute");
		strcpy(*buf, s);
		return MAL_SUCCEED;
	}

	n = l + ldst;
	if (repeat && ldst > lsrc)
		n = (ldst * l) / lsrc;	/* max length */

	n++;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, n, "str.substitute");
	b = *buf;
	pfnd = s;
	do {
		fnd = strstr(pfnd, src);
		if (fnd == NULL)
			break;
		n = fnd - pfnd;
		if (n > 0) {
			strcpy_len(b, pfnd, n + 1);
			b += n;
		}
		if (ldst > 0) {
			strcpy_len(b, dst, ldst + 1);
			b += ldst;
		}
		if (*fnd == 0)
			break;
		pfnd = fnd + lsrc;
	} while (repeat);
	strcpy(b, pfnd);
	return MAL_SUCCEED;
}

static str
STRSubstitute(Client ctx, str *res, const char *const *arg1, const char *const *arg2, const char *const *arg3,
			  const bit *g)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2, *s3 = *arg3;

	if (strNil(s) || strNil(s2) || strNil(s3)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substitute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_substitute(&buf, &buflen, s, s2, s3, *g)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.substitute",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRascii(Client ctx, int *ret, const char *const *s)
{
	(void) ctx;
	return str_wchr_at(ret, *s, 0);
}

str
str_substring_tail(str *buf, size_t *buflen, const char *s, int start)
{
	if (start < 1)
		start = 1;
	start--;
	return str_tail(buf, buflen, s, start);
}

static str
STRsubstringTail(Client ctx, str *res, const char *const *arg1, const int *start)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int st = *start;

	if (strNil(s) || is_int_nil(st)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substringTail", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_substring_tail(&buf, &buflen, s, st)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.substringTail",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_sub_string(str *buf, size_t *buflen, const char *s, int start, int l)
{
	if (start < 1)
		start = 1;
	start--;
	return str_Sub_String(buf, buflen, s, start, l);
}

static str
STRsubstring(Client ctx, str *res, const char *const *arg1, const int *start, const int *ll)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int st = *start, l = *ll;

	if (strNil(s) || is_int_nil(st) || is_int_nil(l)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_sub_string(&buf, &buflen, s, st, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.substring",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRprefix(Client ctx, str *res, const char *const *arg1, const int *ll)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *ll;

	if (strNil(s) || is_int_nil(l)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.prefix", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_Sub_String(&buf, &buflen, s, 0, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.prefix",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_suffix(str *buf, size_t *buflen, const char *s, int l)
{
	int start = (int) (strlen(s) - l);
	return str_Sub_String(buf, buflen, s, start, l);
}

static str
STRsuffix(Client ctx, str *res, const char *const *arg1, const int *ll)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *ll;

	if (strNil(s) || is_int_nil(l)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.suffix", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_suffix(&buf, &buflen, s, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.suffix",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

int
str_locate2(const char *needle, const char *haystack, int start)
{
	int off, res;
	const char *s;

	off = start <= 0 ? 1 : start;
	s = UTF8_strtail(haystack, off - 1);
	res = str_search(s, needle);
	return res >= 0 ? res + off : 0;
}

static str
STRlocate3(Client ctx, int *ret, const char *const *needle, const char *const *haystack, const int *start)
{
	(void) ctx;
	const char *s = *needle, *s2 = *haystack;
	int st = *start;

	*ret = (strNil(s) || strNil(s2) || is_int_nil(st)) ?
		int_nil :
		str_locate2(s, s2, st);
	return MAL_SUCCEED;
}

static str
STRlocate(Client ctx, int *ret, const char *const *needle, const char *const *haystack)
{
	(void) ctx;
	const char *s = *needle, *s2 = *haystack;

	*ret = (strNil(s) || strNil(s2)) ? int_nil : str_locate2(s, s2, 1);
	return MAL_SUCCEED;
}

str
str_insert(str *buf, size_t *buflen, const char *s, int strt, int l,
		   const char *s2)
{
	str v;
	int l1 = UTF8_strlen(s);
	size_t nextlen;

	if (l < 0)
		throw(MAL, "str.insert",
			  SQLSTATE(42000)
			  "The number of characters for insert function must be non negative");
	if (strt < 0) {
		if (-strt <= l1)
			strt = l1 + strt;
		else
			strt = 0;
	}
	if (strt > l1)
		strt = l1;

	nextlen = strlen(s) + strlen(s2) + 1;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, nextlen, "str.insert");
	v = *buf;
	if (strt > 0)
		v = UTF8_strncpy(v, s, strt);
	v = stpcpy(v, s2);
	if (strt + l < l1)
		strcpy(v, UTF8_strtail(s, strt + l));
	return MAL_SUCCEED;
}

static str
STRinsert(Client ctx, str *res, const char *const *input, const int *start, const int *nchars,
		  const char *const *input2)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *input, *s2 = *input2;
	int st = *start, n = *nchars;

	if (strNil(s) || is_int_nil(st) || is_int_nil(n) || strNil(s2)) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_insert(&buf, &buflen, s, st, n, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.insert",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRreplace(Client ctx, str *ret, const char *const *s1, const char *const *s2, const char *const *s3)
{
	(void) ctx;
	bit flag = TRUE;
	return STRSubstitute(ctx, ret, s1, s2, s3, &flag);
}

str
str_repeat(str *buf, size_t *buflen, const char *s, int c)
{
	size_t l = strlen(s), nextlen;

	if (l >= INT_MAX)
		throw(MAL, "str.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	nextlen = (size_t) c *l + 1;

	CHECK_STR_BUFFER_LENGTH(buf, buflen, nextlen, "str.repeat");
	str t = *buf;
	*t = 0;
	for (int i = c; i > 0; i--, t += l)
		strcpy(t, s);
	return MAL_SUCCEED;
}

static str
STRrepeat(Client ctx, str *res, const char *const *arg1, const int *c)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int cc = *c;

	if (strNil(s) || is_int_nil(cc) || cc < 0) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_repeat(&buf, &buflen, s, cc)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.repeat",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRspace(Client ctx, str *res, const int *ll)
{
	(void) ctx;
	str buf = NULL, msg = MAL_SUCCEED;
	int l = *ll;

	if (is_int_nil(l) || l < 0) {
		*res = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		const char space[] = " ", *s = space;
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_repeat(&buf, &buflen, s, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = MA_STRDUP(ctx->alloc, buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.space",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRasciify(Client ctx, str *r, const char *const *s)
{
	(void) ctx;
	char *buf = NULL;
	size_t buflen = 0;
	if (GDKasciify(&buf, &buflen, *s) != GDK_SUCCEED)
		throw(MAL, "str.asciify", GDK_EXCEPTION);
	*r = MA_STRDUP(ctx->alloc, buf);
	GDKfree(buf);
	return MAL_SUCCEED;
}

static inline void
BBPreclaim_n(int nargs, ...)
{
	va_list valist;
	va_start(valist, nargs);
	for (int i = 0; i < nargs; i++) {
		BAT *b = va_arg(valist, BAT *);
		BBPreclaim(b);
	}
	va_end(valist);
}

#define VALUE(s, x)  (s##_vars + VarHeapVal(s##_vals, (x), s##i->width))
#define APPEND(b, o) (((oid *) b->theap->base)[b->batCount++] = (o))

#define SCAN_LOOP(STR_CMP)									\
	do {													\
		TIMEOUT_LOOP(lci->ncand, qry_ctx) {					\
			oid lo = canditer_next(lci);					\
			const char *ls = VALUE(l, lo - l_base);			\
			if (!strNil(ls) && (STR_CMP))					\
				APPEND(rl, lo);								\
		}													\
	} while (0)

static str
scan_loop_strselect(BAT *rl, BATiter *li, struct canditer *lci, const char *r,
					int (*str_cmp)(const char *, const char *, int),
					bool anti, const char *fname, QryCtx *qry_ctx)
{
	oid l_base = li->b->hseqbase;
	const char *l_vars = li->vh->base, *l_vals = li->base;
	int r_len = str_strlen(r);

	lng t0 = 0;
	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if (anti)
		SCAN_LOOP(str_cmp(ls, r, r_len) != 0);
	else
		SCAN_LOOP(str_cmp(ls, r, r_len) == 0);

	BATsetcount(rl, BATcount(rl));
	if (BATcount(rl) > 0) {
		BATnegateprops(rl);
		rl->tnonil = true;
		rl->tnil = false;
	}

	TRC_DEBUG(ALGO, "(%s, %s, l=%s #%zu [%s], cl=%s #%zu, time="LLFMT"usecs)\n",
			  fname, "scan_loop_strselect",
			  BATgetId(li->b), li->count, ATOMname(li->b->ttype),
			  lci ? BATgetId(lci->s) : "NULL", lci ? lci->ncand : 0,
			  GDKusec() - t0);

	return MAL_SUCCEED;
}

static str
STRselect(MalStkPtr stk, InstrPtr pci, const str fname,
		  int (*str_cmp)(const char *, const char *, int))
{
	str msg = MAL_SUCCEED;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	BAT *l = NULL, *cl = NULL, *rl = NULL;

	bat *RL = getArgReference_bat(stk, pci, 0);
	bat *L = getArgReference_bat(stk, pci, 1);
	bat *CL = getArgReference_bat(stk, pci, 2);
	const char *r = *getArgReference_str(stk, pci, 3);
	bool icase = pci->argc != 5;
	bool anti = pci->argc == 5 ? *getArgReference_bit(stk, pci, 4) :
		*getArgReference_bit(stk, pci, 5);

	if (!(l = BATdescriptor(*L)))
		throw(MAL, fname, RUNTIME_OBJECT_MISSING);

	if (CL && !is_bat_nil(*CL) && !(cl = BATdescriptor(*CL))) {
		BBPreclaim(l);
		throw(MAL, fname, RUNTIME_OBJECT_MISSING);
	}

	BATiter li = bat_iterator(l);
	struct canditer lci;
	canditer_init(&lci, l, cl);
	size_t l_cnt = lci.ncand;

	rl = COLnew(0, TYPE_oid, l_cnt, TRANSIENT);
	if (!rl) {
		BBPreclaim_n(2, l, cl);
		throw(MAL, fname, MAL_MALLOC_FAIL);
	}

	if (icase) {
		if (str_cmp == str_is_prefix)
			str_cmp = str_is_iprefix;
		else if (str_cmp == str_is_suffix)
			str_cmp = str_is_isuffix;
		else
			str_cmp = str_icontains;
	}

	msg = scan_loop_strselect(rl, &li, &lci, r, str_cmp, anti, fname, qry_ctx);

	bat_iterator_end(&li);

	if (!msg) {
		*RL = rl->batCacheid;
		BBPkeepref(rl);
	} else {
		BBPreclaim(rl);
	}

	BBPreclaim_n(2, l, cl);
	return msg;
}

static str
STRstartswithselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	return STRselect(stk, pci, "str.startswithselect", str_is_prefix);
}

static str
STRendswithselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	return STRselect(stk, pci, "str.endswithselect", str_is_suffix);
}

static str
STRcontainsselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	return STRselect(stk, pci, "str.containsselect", str_contains);
}

static void
do_strrev(char *dst, const char *src, size_t len)
{
	dst[len] = 0;
	if (strNil(src)) {
		assert(len == strlen(str_nil));
		strcpy(dst, str_nil);
		return;
	}
	while (*src) {
		if ((*src & 0xF8) == 0xF0) {
			assert(len >= 4);
			dst[len - 4] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 3] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 2] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 1] = *src++;
			len -= 4;
		} else if ((*src & 0xF0) == 0xE0) {
			assert(len >= 3);
			dst[len - 3] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 2] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 1] = *src++;
			len -= 3;
		} else if ((*src & 0xE0) == 0xC0) {
			assert(len >= 2);
			dst[len - 2] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 1] = *src++;
			len -= 2;
		} else {
			assert(len >= 1);
			assert((*src & 0x80) == 0);
			dst[--len] = *src++;
		}
	}
	assert(len == 0);
}

static BAT *
strbat_reverse(BAT *b)
{
	BAT *bn = NULL;
	BATiter bi;
	BUN p, q;
	const char *src;
	size_t len;
	char *dst;
	size_t dstlen;

	dstlen = 1024;
	dst = GDKmalloc(dstlen);
	if (dst == NULL)
		return NULL;

	assert(b->ttype == TYPE_str);

	bn = COLnew(b->hseqbase, TYPE_str, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		GDKfree(dst);
		return NULL;
	}

	bi = bat_iterator(b);
	BATloop(b, p, q) {
		src = (const char *) BUNtail(bi, p);
		len = strlen(src);
		if (len >= dstlen) {
			char *ndst;
			dstlen = len + 1024;
			ndst = GDKrealloc(dst, dstlen);
			if (ndst == NULL) {
				bat_iterator_end(&bi);
				BBPreclaim(bn);
				GDKfree(dst);
				return NULL;
			}
			dst = ndst;
		}
		do_strrev(dst, src, len);
		if (BUNappend(bn, dst, false) != GDK_SUCCEED) {
			bat_iterator_end(&bi);
			BBPreclaim(bn);
			GDKfree(dst);
			return NULL;
		}
	}

	bat_iterator_end(&bi);
	GDKfree(dst);
	return bn;
}

#define NESTED_LOOP(STR_CMP)											\
	do {																\
		canditer_reset(lci);											\
		TIMEOUT_LOOP(rci->ncand, qry_ctx) {								\
			or = canditer_next(rci);									\
			const char *rs = VALUE(r, or - rbase);						\
			if (strNil(rs))												\
				continue;												\
			canditer_reset(lci);										\
			TIMEOUT_LOOP(lci->ncand, qry_ctx) {							\
				ol = canditer_next(lci);								\
				const char *ls = VALUE(l, ol - lbase);					\
				if (!strNil(ls) && STR_CMP) {							\
						APPEND(rl, ol);									\
						if (rr) APPEND(rr, or);							\
						if (BATcount(rl) == BATcapacity(rl)) {			\
							new_cap = BATgrows(rl);						\
							if (BATextend(rl, new_cap) != GDK_SUCCEED || \
								(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) { \
								throw(MAL, fname, GDK_EXCEPTION);		\
							}											\
						}												\
				}														\
			}															\
		}																\
	} while (0)															\

static str
nested_loop_strjoin(BAT *rl, BAT *rr, BATiter *li, BATiter *ri,
					struct canditer *lci, struct canditer *rci,
					int (*str_cmp)(const char *, const char *, int),
					bool anti, const char *fname, QryCtx *qry_ctx)
{
	size_t new_cap;
	oid lbase = li->b->hseqbase, rbase = ri->b->hseqbase, or, ol;
	const char *l_vars = li->vh->base, *r_vars = ri->vh->base,
		*l_vals = li->base, *r_vals = ri->base;

	lng t0 = 0;
	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if (anti)
		NESTED_LOOP(str_cmp(ls, rs, str_strlen(rs)) != 0);
	else
		NESTED_LOOP(str_cmp(ls, rs, str_strlen(rs)) == 0);

	BATsetcount(rl, BATcount(rl));
	if (rr) BATsetcount(rr, BATcount(rr));

	if (BATcount(rl) > 0) {
		BATnegateprops(rl);
		rl->tnonil = true;
		rl->tnil = false;
		if (rr) {
			BATnegateprops(rr);
			rr->tnonil = true;
			rr->tnil = false;
			rr->tsorted = ri->sorted;
			rr->trevsorted = ri->revsorted;
		}
		// GDKfree(vb_low);
	}

	TRC_DEBUG(ALGO, "(%s, %s, l=%s #%zu [%s], r=%s #%zu [%s], cl=%s #%zu, cr=%s #%zu, time="LLFMT"usecs)\n",
			  fname, "nested_loop_strjoin",
			  BATgetId(li->b), li->count, ATOMname(li->b->ttype),
			  BATgetId(ri->b), ri->count, ATOMname(ri->b->ttype),
			  lci ? BATgetId(lci->s) : "NULL", lci ? lci->ncand : 0,
			  rci ? BATgetId(rci->s) : "NULL", rci ? rci->ncand : 0,
			  GDKusec() - t0);

	return MAL_SUCCEED;
}

static void
ng_destroy(NGrams *ng)
{
	if (ng) {
		GDKfree(ng->idx);
		GDKfree(ng->sigs);
		GDKfree(ng->histogram);
		GDKfree(ng->lists);
		GDKfree(ng->rids);
	}
	GDKfree(ng);
}

static NGrams *
ng_create(size_t cnt, size_t ng_sz)
{
	NGrams *ng = GDKmalloc(sizeof(NGrams));
	if (ng) {
		ng->idx  = GDKmalloc(ng_sz * sizeof(NG_TYPE));
		ng->sigs = GDKmalloc(cnt * sizeof(NG_TYPE));
		ng->histogram = GDKmalloc(ng_sz * sizeof(unsigned));
		ng->lists = GDKmalloc(ng_sz * sizeof(unsigned));
		ng->rids = GDKmalloc(NG_MULTIPLE * cnt * sizeof(oid));
	}
	if (!ng || !ng->idx || !ng->sigs || !ng->histogram || !ng->lists || !ng->rids) {
		ng_destroy(ng);
		return NULL;
	}
	return ng;
}

static str
init_bigram_idx(NGrams *ng, BATiter *bi, struct canditer *bci, QryCtx *qry_ctx)
{
	NG_TYPE *idx = ng->idx;
	NG_TYPE *sigs = ng->sigs;
	unsigned *h = ng->histogram;
	unsigned h_tmp[NG_BITS][NG_BITS] = { 0 };
	unsigned *h_tmp_ptr = (unsigned *) h_tmp;
	unsigned map[BIGRAM_SZ];
	unsigned *lists = ng->lists;
	oid *rids = ng->rids;
	unsigned k = 1;

	oid b_base = bi->b->hseqbase;
	const char *b_vars = bi->vh->base, *b_vals = bi->base;

	canditer_reset(bci);
	TIMEOUT_LOOP(bci->ncand, qry_ctx) {
		oid ob = canditer_next(bci);
		const char *s = VALUE(b, ob - b_base);
		if (!strNil(s))
			for ( ; BIGRAM(s); s++)
				h_tmp[ENC_TOKEN1(s)][ENC_TOKEN2(s)]++;
	}

	for (unsigned i = 0; i < BIGRAM_SZ; i++) {
		map[i] = i;
		idx[i] = lists[i] = 0;
		h[i] = h_tmp_ptr[i];
	}

	GDKqsort(h_tmp_ptr, map, NULL, BIGRAM_SZ,
			 sizeof(unsigned), sizeof(unsigned), TYPE_int, true, false);

	unsigned j = BIGRAM_SZ - 1, sum = 0;
	for ( ; j; j--) {
		if ((sum + h_tmp_ptr[j] ) >= (NG_MULTIPLE * bci->ncand) - 1)
			break;
		sum += h_tmp_ptr[j];
	}
	unsigned larger_cnt = h_tmp_ptr[j];
	for(; h_tmp_ptr[j] == larger_cnt; j++)
		;
	ng->max = h_tmp_ptr[0];
	ng->min = h_tmp_ptr[j];

	for (NG_TYPE i = 0, n = 0; i < BIGRAM_SZ && h_tmp_ptr[i] > 0; i++) {
		idx[map[i]] = (NG_TYPE)1 << n;
		n++;
		n %= NG_BITS;
	}

	canditer_reset(bci);
	TIMEOUT_LOOP(bci->ncand, qry_ctx) {
		oid ob = canditer_next(bci);
		const char *s = VALUE(b, ob - b_base);
		if (!strNil(s) && BIGRAM(s)) {
			NG_TYPE sig = 0;
			for ( ; BIGRAM(s); s++) {
				unsigned enc_bigram = ENC_TOKEN1(s) * NG_BITS + ENC_TOKEN2(s);
				sig |= idx[enc_bigram];
				if (h[enc_bigram] <= ng->min) {
					if (lists[enc_bigram] == 0) {
						lists[enc_bigram] = k;
						k += h[enc_bigram];
						assert(k <= UINT32_MAX);
						h[enc_bigram] = 0;
					}
					int done = (h[enc_bigram] > 0 &&
								rids[lists[enc_bigram] + h[enc_bigram] - 1] == (ob - b_base));
					if (!done) {
						rids[lists[enc_bigram] + h[enc_bigram]] = (ob - b_base);
						h[enc_bigram]++;
					}
				}
			}
			*sigs = sig;
		} else {
			*sigs = NG_TYPENIL;
		}
		sigs++;
	}

	return MAL_SUCCEED;
}

static str
bigram_strjoin(BAT *rl, BAT *rr, BATiter *li, BATiter *ri,
			   struct canditer *lci, struct canditer *rci,
			   int (*str_cmp)(const char *, const char *, int),
			   const char *fname, QryCtx *qry_ctx)
{
	str msg = MAL_SUCCEED;

	NGrams *ng = ng_create(lci->ncand, BIGRAM_SZ);
	if (!ng)
		throw(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	NG_TYPE *idx = ng->idx;
	NG_TYPE *sigs = ng->sigs;
	unsigned *h = ng->histogram;
	unsigned *lists = ng->lists;
	oid *rids = ng->rids;
	size_t new_cap;

	msg = init_bigram_idx(ng, li, lci, qry_ctx);
	if (msg) {
		ng_destroy(ng);
		return msg;
	}

	oid l_base = li->b->hseqbase, r_base = ri->b->hseqbase;
	const char *l_vars = li->vh->base, *r_vars = ri->vh->base,
		*l_vals = li->base, *r_vals = ri->base;

	lng t0 = 0;
	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	canditer_reset(lci);
	TIMEOUT_LOOP(rci->ncand, qry_ctx) {
		oid or = canditer_next(rci);
		const char *rs = VALUE(r, or - r_base), *rs_iter = rs;
		if (strNil(rs))
			continue;
		if (strlen(rs) < 2) {
			canditer_reset(lci);
			TIMEOUT_LOOP(lci->ncand, qry_ctx) {
				oid ol = canditer_next(lci);
				const char *ls = VALUE(l, ol - l_base);
				if (!strNil(ls) && str_cmp(ls, rs, str_strlen(rs)) == 0) {
					APPEND(rl, ol);
					if (rr) APPEND(rr, or);
					if (BATcount(rl) == BATcapacity(rl)) {
						new_cap = BATgrows(rl);
						if (BATextend(rl, new_cap) != GDK_SUCCEED ||
							(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
							ng_destroy(ng);
							throw(MAL, fname, GDK_EXCEPTION);
						}
					}
				}
			}
		} else if (BIGRAM(rs)) {
			NG_TYPE sig = 0;
			unsigned local_min = ng->max, min_enc_bigram = 0;
			for ( ; BIGRAM(rs_iter); rs_iter++) {
				unsigned enc_bigram = ENC_TOKEN1(rs_iter) * NG_BITS + ENC_TOKEN2(rs_iter);
				sig |= idx[enc_bigram];
				if (h[enc_bigram] < local_min) {
					local_min = h[enc_bigram];
					min_enc_bigram = enc_bigram;
				}
			}
			if (local_min <= ng->min) {
				unsigned list = lists[min_enc_bigram], list_cnt = h[min_enc_bigram];
				for (size_t i = 0; i < list_cnt; i++, list++) {
					oid ol = rids[list];
					if ((sigs[ol] & sig) == sig) {
						const char *ls = VALUE(l, ol);
						if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
							APPEND(rl, ol + l_base);
							if (rr) APPEND(rr, or);
							if (BATcount(rl) == BATcapacity(rl)) {
								new_cap = BATgrows(rl);
								if (BATextend(rl, new_cap) != GDK_SUCCEED ||
									(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
									ng_destroy(ng);
									throw(MAL, fname, GDK_EXCEPTION);
								}
							}
						}
					}
				}
			} else {
				canditer_reset(lci);
				TIMEOUT_LOOP(lci->ncand, qry_ctx) {
					oid ol = canditer_next(lci);
					if ((sigs[ol - l_base] & sig) == sig) {
						const char *ls = VALUE(l, ol - l_base);
						if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
							APPEND(rl, ol);
							if (rr) APPEND(rr, or);
							if (BATcount(rl) == BATcapacity(rl)) {
								new_cap = BATgrows(rl);
								if (BATextend(rl, new_cap) != GDK_SUCCEED ||
									(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
									ng_destroy(ng);
									throw(MAL, fname, GDK_EXCEPTION);
								}
							}
						}
					}
				}
			}
		}
	}

	BATsetcount(rl, BATcount(rl));
	if (rr) BATsetcount(rr, BATcount(rr));

	if (BATcount(rl) > 0) {
		BATnegateprops(rl);
		if (rr) BATnegateprops(rr);
	}

	ng_destroy(ng);

	TRC_DEBUG(ALGO, "(%s, %s, l=%s #%zu [%s], r=%s #%zu [%s], cl=%s #%zu, cr=%s #%zu, time="LLFMT"usecs)\n",
			  fname, "bigram_strjoin",
			  BATgetId(li->b), li->count, ATOMname(li->b->ttype),
			  BATgetId(ri->b), ri->count, ATOMname(ri->b->ttype),
			  lci ? BATgetId(lci->s) : "NULL", lci ? lci->ncand : 0,
			  rci ? BATgetId(rci->s) : "NULL", rci ? rci->ncand : 0,
			  GDKusec() - t0);

	return msg;
}

static str
sorted_strjoin(BAT **rl_ptr, BAT **rr_ptr, BATiter *li, BATiter *ri,
			   struct canditer *lci, struct canditer *rci,
			   int (*str_cmp)(const char *, const char *, int),
			   const char *fname, QryCtx *qry_ctx)
{
	str msg = MAL_SUCCEED;
	BAT *l = li->b, *r = ri->b, *cl = lci->s, *cr = rci->s,
		*sorted_l = NULL, *sorted_r = NULL,
		*sorted_cl = NULL, *sorted_cr = NULL,
		*ord_sorted_l = NULL, *ord_sorted_r = NULL,
		*proj_rl = NULL, *proj_rr = NULL,
		*rl = *rl_ptr, *rr = *rr_ptr;

	BATiter *sorted_li = NULL, *sorted_ri = NULL, tmp_li, tmp_ri;
	struct canditer sorted_lci, sorted_rci;

	if (!BATordered(l)) {
		if (BATsort(&sorted_l, &ord_sorted_l, NULL,
					l, NULL, NULL, false, false, false) != GDK_SUCCEED) {
			throw(MAL, fname, GDK_EXCEPTION);
		}
		if (cl && BATsort(&sorted_cl, NULL, NULL,
						  cl, ord_sorted_l, NULL, false, false, false) != GDK_SUCCEED) {
			BBPreclaim_n(2, sorted_l, ord_sorted_l);
			throw(MAL, fname, GDK_EXCEPTION);
		}
		tmp_li = bat_iterator(sorted_l);
		sorted_li = &tmp_li;
		canditer_init(&sorted_lci, sorted_l, sorted_cl);
	} else {
		sorted_l = l;
		sorted_cl = cl;
		sorted_li = li;
		sorted_lci = *lci;
	}

	if (!BATordered(r)) {
		if (BATsort(&sorted_r, &ord_sorted_r, NULL,
					r, NULL, NULL, false, false, false) != GDK_SUCCEED) {
			BBPreclaim_n(3, sorted_l, ord_sorted_l, sorted_cl);
			throw(MAL, fname, GDK_EXCEPTION);
		}
		if (cr && BATsort(&sorted_cr, NULL, NULL,
						  cr, ord_sorted_r, NULL, false, false, false)) {
			BBPreclaim_n(5, sorted_l, ord_sorted_l, sorted_cl, sorted_r, ord_sorted_r);
			throw(MAL, fname, GDK_EXCEPTION);
		}
		tmp_ri = bat_iterator(sorted_r);
		sorted_ri = &tmp_ri;
		canditer_init(&sorted_rci, sorted_r, sorted_cr);
	} else {
		sorted_r = r;
		sorted_cr = cr;
		sorted_ri = ri;
		sorted_rci = *rci;
	}

	oid sorted_l_base = sorted_l->hseqbase, sorted_r_base = sorted_r->hseqbase,
		ol = 0, or = 0, ly = 0, rx = 0, n;
	const char *sorted_l_vars = sorted_li->vh->base, *sorted_r_vars = sorted_ri->vh->base,
		*sorted_l_vals = sorted_li->base, *sorted_r_vals = sorted_ri->base;
	size_t new_cap;

	TIMEOUT_LOOP(sorted_lci.ncand, qry_ctx) {
		ol = canditer_next(&sorted_lci);
		const char *ls = VALUE(sorted_l, ol - sorted_l_base);
		if (!strNil(ls))
			break;
		ly++;
	}
	TIMEOUT_LOOP(sorted_rci.ncand, qry_ctx) {
		or = canditer_next(&sorted_rci);
		const char *rs = VALUE(sorted_r, or - sorted_r_base);
		if (!strNil(rs))
			break;
		rx++;
	}

	TIMEOUT_LOOP(sorted_rci.ncand - rx, qry_ctx) {
		const char *rs = VALUE(sorted_r, or - sorted_r_base);
		for (canditer_setidx(&sorted_lci, ly), n = ly; n < sorted_lci.ncand; n++) {
			ol = canditer_next(&sorted_lci);
			const char *ls = VALUE(sorted_l, ol - sorted_l_base);
			int cmp = str_cmp(ls, rs, str_strlen(rs));
			if (cmp < 0) {
				ly++;
				continue;
			} else if (cmp > 0)
				break;
			APPEND(rl, ol);
			if (rr) APPEND(rr, or);
			if (BATcount(rl) == BATcapacity(rl)) {
				new_cap = BATgrows(rl);
				if (BATextend(rl, new_cap) != GDK_SUCCEED ||
					(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
					if (!BATordered(l))
						bat_iterator_end(sorted_li);
					if (!BATordered(r))
						bat_iterator_end(sorted_ri);
					BBPreclaim_n(6, sorted_l, ord_sorted_l, sorted_cl,
								 sorted_r, ord_sorted_r, sorted_cr);
					throw(MAL, fname, GDK_EXCEPTION);
				}
			}
		}
		or = canditer_next(&sorted_rci);
	}

	BATsetcount(rl, BATcount(rl));
	if (rr) BATsetcount(rr, BATcount(rr));

	if (BATcount(rl) > 0) {
		BATnegateprops(rl);
		rl->tnonil = true;
		rl->tnil = false;
		if (rr) {
			BATnegateprops(rr);
			rr->tnonil = true;
			rr->tnil = false;
			rr->tsorted = ri->sorted;
			rr->trevsorted = ri->revsorted;
		}
	}

	if (!BATordered(l)) {
		bat_iterator_end(sorted_li);
		proj_rl = BATproject(rl, ord_sorted_l);
		if (!proj_rl) {
			msg = createException(MAL, fname, GDK_EXCEPTION);
		} else {
			BBPreclaim(rl);
			*rl_ptr = proj_rl;
		}
		BBPreclaim_n(3, sorted_l, ord_sorted_l, sorted_cl);
	}

	if (!msg && rr && !BATordered(r)) {
		bat_iterator_end(sorted_ri);
		proj_rr = BATproject(rr, ord_sorted_r);
		if (!proj_rr) {
			BBPreclaim(proj_rl);
			msg = createException(MAL, fname, GDK_EXCEPTION);
		} else {
			BBPreclaim(rr);
			*rr_ptr = proj_rr;
		}
		BBPreclaim_n(3, sorted_r, ord_sorted_r, sorted_cr);
	}

	TRC_DEBUG(ALGO, "(%s, l=%s,r=%s)=(%s#" BUNFMT "%s%s,%s#" BUNFMT "%s%s\n",
			  fname, BATgetId(l), BATgetId(r), BATgetId(rl), BATcount(rl),
			  rl->tsorted ? "-sorted" : "", rl->trevsorted ? "-revsorted" : "",
			  rr ? BATgetId(rr) : NULL, rr ? BATcount(rr) : 0,
			  rr && rr->tsorted ? "-sorted" : "", rr && rr->trevsorted ? "-revsorted" : "");

	return msg;
}

static inline str
ignorecase(const bat IC, bool *icase, const str fname)
{
	str msg = MAL_SUCCEED;
	BAT *b = BATdescriptor(IC);
	BATiter bi = bat_iterator(b);

	if (bi.b != NULL && bi.count == 1)
		*icase = *(bit *) BUNtloc(bi, 0);
	else if (bi.count != 1)
		msg = createException(MAL, fname, SQLSTATE(HY009) "Invalid case ignore. Single value expected");
	else
		msg = createException(MAL, fname, RUNTIME_OBJECT_MISSING);

	bat_iterator_end(&bi);
	BBPreclaim(b);
	return msg;
}

static str
STRjoin(MalStkPtr stk, InstrPtr pci, const str fname,
		int (*str_cmp)(const char *, const char *, int))
{
	str msg = MAL_SUCCEED;
	int offset = pci->retc;
	int in_argc = pci->argc - pci->retc;
	BAT *rl = NULL, *rr = NULL, *l = NULL, *r = NULL, *cl = NULL, *cr = NULL;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	bat *RL = getArgReference_bat(stk, pci, 0);
	bat *RR = pci->retc == 1 ? NULL : getArgReference_bat(stk, pci, 1);
	bat L = *getArgReference_bat(stk, pci, offset++);
	bat R = *getArgReference_bat(stk, pci, offset++);
	bat IC = in_argc == 7 ? 0 : *getArgReference_bat(stk, pci, offset++);
	bat CL = *getArgReference_bat(stk, pci, offset++);
	bat CR = *getArgReference_bat(stk, pci, offset++);
	bool anti = in_argc == 7 ? *getArgReference_bit(stk, pci, 8) : *getArgReference_bit(stk, pci, 9);

	bool icase = false;

	if (in_argc == 8 && (msg = ignorecase(IC, &icase, fname)))
		return msg;

	if (!(l = BATdescriptor(L)) || !(r = BATdescriptor(R))) {
		BBPreclaim_n(2, l, r);
		throw(MAL, fname, RUNTIME_OBJECT_MISSING);
	}

	if ((CL && !is_bat_nil(CL) && (cl = BATdescriptor(CL)) == NULL) ||
		(CR && !is_bat_nil(CR) && (cr = BATdescriptor(CR)) == NULL)) {
		BBPreclaim_n(4, l, r, cl, cr);
		throw(MAL, fname, RUNTIME_OBJECT_MISSING);
	}

	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	struct canditer lci, rci;
	canditer_init(&lci, l, cl);
	canditer_init(&rci, r, cr);
	size_t l_cnt = lci.ncand, r_cnt = rci.ncand;
	size_t nested_cost = lci.ncand * rci.ncand,
		sorted_cost = (size_t)floor(0.8 * (l_cnt * log2((double)l_cnt) + r_cnt * log2((double)r_cnt)));

	rl = COLnew(0, TYPE_oid, l_cnt, TRANSIENT);
	if (RR)
		rr = COLnew(0, TYPE_oid, l_cnt, TRANSIENT);

	if (!rl || (RR && !rr)) {
		bat_iterator_end(&li);
		bat_iterator_end(&ri);
		BBPreclaim_n(6, rl, rr, l, r, cl, cr);
		throw(MAL, fname, MAL_MALLOC_FAIL);
	}

	if (anti || nested_cost < 1000 || nested_cost <= sorted_cost) {
		if (icase) {
			if (str_cmp == str_is_prefix)
				str_cmp = str_is_iprefix;
			else if (str_cmp == str_is_suffix)
				str_cmp = str_is_isuffix;
			else
				str_cmp = str_icontains;
		}
		msg = nested_loop_strjoin(rl, rr, &li, &ri, &lci, &rci, str_cmp, anti, fname, qry_ctx);
	} else {
		if (icase) {
			BAT *l_low = NULL, *r_low = NULL;
			if (!(l_low = BATcasefold(l, NULL)) || !(r_low = BATcasefold(r, NULL))) {
				BBPreclaim_n(5, l, r, cl, cr, l_low);
				throw(MAL, fname, "Failed string lowering input bats");
			}
			bat_iterator_end(&li);
			bat_iterator_end(&ri);
			BBPreclaim_n(2, l, r);
			l = l_low;
			r = r_low;
			li= bat_iterator(l);
			ri = bat_iterator(r);
		}
		if (str_cmp == str_contains || str_cmp == str_icontains) {
			msg = bigram_strjoin(rl, rr, &li, &ri, &lci, &rci, str_cmp, fname, qry_ctx);
		} else {
			if (str_cmp == str_is_suffix || str_cmp == str_is_isuffix) {
				BAT *l_rev = NULL, *r_rev = NULL;
				if (!(l_rev = strbat_reverse(l)) || !(r_rev = strbat_reverse(r))) {
					BBPreclaim_n(5, l, r, cl, cr, l_rev);
					throw(MAL, fname, "Failed string reversing input bats");
				}
				bat_iterator_end(&li);
				bat_iterator_end(&ri);
				BBPreclaim_n(2, l, r);
				l = l_rev;
				r = r_rev;
				li = bat_iterator(l);
				ri = bat_iterator(r);
			}
			msg = sorted_strjoin(&rl, &rr, &li, &ri, &lci, &rci, str_is_prefix, fname, qry_ctx);
		}
	}

	bat_iterator_end(&li);
	bat_iterator_end(&ri);

	if (!msg) {
		*RL = rl->batCacheid;
		BBPkeepref(rl);
		if (RR) {
			*RR = rr->batCacheid;
			BBPkeepref(rr);
		}
	} else {
		BBPreclaim_n(2, rl, rr);
	}

	BBPreclaim_n(4, l, r, cl, cr);
	return msg;
}

static str
STRstartswithjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	return STRjoin(stk, pci, "str.startswithjoin", str_is_prefix);
}

static str
STRendswithjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	return STRjoin(stk, pci, "str.endswithjoin", str_is_suffix);
}

static str
STRcontainsjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	return STRjoin(stk, pci, "str.containsjoin", str_contains);
}

#include "mel.h"
mel_func str_init_funcs[] = {
	command("str", "str", STRtostr, false, "Noop routine.", args(1,2, arg("",str),arg("s",str))),
	command("str", "string", STRTail, false, "Return the tail s[offset..n]\nof a string s[0..n].", args(1,3, arg("",str),arg("s",str),arg("offset",int))),
	command("str", "string3", STRSubString, false, "Return substring s[offset..offset+count] of a string s[0..n]", args(1,4, arg("",str),arg("s",str),arg("offset",int),arg("count",int))),
	command("str", "length", STRLength, false, "Return the length of a string.", args(1,2, arg("",int),arg("s",str))),
	command("str", "nbytes", STRBytes, false, "Return the string length in bytes.", args(1,2, arg("",int),arg("s",str))),
	command("str", "unicodeAt", STRWChrAt, false, "get a unicode character\n(as an int) from a string position.", args(1,3, arg("",int),arg("s",str),arg("index",int))),
	command("str", "unicode", STRFromWChr, false, "convert a unicode to a character.", args(1,2, arg("",str),arg("wchar",int))),
	pattern("str", "startswith", STRstartswith, false, "Check if string starts with substring.", args(1,3, arg("",bit),arg("s",str),arg("prefix",str))),
	pattern("str", "startswith", STRstartswith, false, "Check if string starts with substring, icase flag.", args(1,4, arg("",bit),arg("s",str),arg("prefix",str),arg("icase",bit))),
	pattern("str", "endswith", STRendswith, false, "Check if string ends with substring.", args(1,3, arg("",bit),arg("s",str),arg("suffix",str))),
	pattern("str", "endswith", STRendswith, false, "Check if string ends with substring, icase flag.", args(1,4, arg("",bit),arg("s",str),arg("suffix",str),arg("icase",bit))),
	pattern("str", "contains", STRcontains, false, "Check if string haystack contains string needle.", args(1,3, arg("",bit),arg("haystack",str),arg("needle",str))),
	pattern("str", "contains", STRcontains, false, "Check if string haystack contains string needle, icase flag.", args(1,4, arg("",bit),arg("haystack",str),arg("needle",str),arg("icase",bit))),
	command("str", "toLower", STRlower, false, "Convert a string to lower case.", args(1,2, arg("",str),arg("s",str))),
	command("str", "toUpper", STRupper, false, "Convert a string to upper case.", args(1,2, arg("",str),arg("s",str))),
	command("str", "caseFold", STRcasefold, false, "Fold the case of a string.", args(1,2, arg("",str),arg("s",str))),
	pattern("str", "search", STRstr_search, false, "Search for a substring. Returns\nposition, -1 if not found.", args(1,3, arg("",int),arg("s",str),arg("c",str))),
	pattern("str", "search", STRstr_search, false, "Search for a substring, icase flag. Returns\nposition, -1 if not found.", args(1,4, arg("",int),arg("s",str),arg("c",str),arg("icase",bit))),
	pattern("str", "r_search", STRrevstr_search, false, "Reverse search for a substring. Returns\nposition, -1 if not found.", args(1,3, arg("",int),arg("s",str),arg("c",str))),
	pattern("str", "r_search", STRrevstr_search, false, "Reverse search for a substring, icase flag. Returns\nposition, -1 if not found.", args(1,4, arg("",int),arg("s",str),arg("c",str),arg("icase",bit))),
	command("str", "splitpart", STRsplitpart, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, arg("",str),arg("s",str),arg("needle",str),arg("field",int))),
	command("str", "trim", STRStrip, false, "Strip whitespaces around a string.", args(1,2, arg("",str),arg("s",str))),
	command("str", "ltrim", STRLtrim, false, "Strip whitespaces from start of a string.", args(1,2, arg("",str),arg("s",str))),
	command("str", "rtrim", STRRtrim, false, "Strip whitespaces from end of a string.", args(1,2, arg("",str),arg("s",str))),
	command("str", "trim2", STRStrip2, false, "Remove the longest string containing only characters from the second string around the first string.", args(1,3, arg("",str),arg("s",str),arg("s2",str))),
	command("str", "ltrim2", STRLtrim2, false, "Remove the longest string containing only characters from the second string from the start of the first string.", args(1,3, arg("",str),arg("s",str),arg("s2",str))),
	command("str", "rtrim2", STRRtrim2, false, "Remove the longest string containing only characters from the second string from the end of the first string.", args(1,3, arg("",str),arg("s",str),arg("s2",str))),
	command("str", "lpad", STRLpad, false, "Fill up a string to the given length prepending the whitespace character.", args(1,3, arg("",str),arg("s",str),arg("len",int))),
	command("str", "rpad", STRRpad, false, "Fill up a string to the given length appending the whitespace character.", args(1,3, arg("",str),arg("s",str),arg("len",int))),
	command("str", "lpad3", STRLpad3, false, "Fill up the first string to the given length prepending characters of the second string.", args(1,4, arg("",str),arg("s",str),arg("len",int),arg("s2",str))),
	command("str", "rpad3", STRRpad3, false, "Fill up the first string to the given length appending characters of the second string.", args(1,4, arg("",str),arg("s",str),arg("len",int),arg("s2",str))),
	command("str", "substitute", STRSubstitute, false, "Substitute first occurrence of 'src' by\n'dst'.  Iff repeated = true this is\nrepeated while 'src' can be found in the\nresult string. In order to prevent\nrecursion and result strings of unlimited\nsize, repeating is only done iff src is\nnot a substring of dst.", args(1,5, arg("",str),arg("s",str),arg("src",str),arg("dst",str),arg("rep",bit))),
	command("str", "like", STRlikewrap, false, "SQL pattern match function", args(1,3, arg("",bit),arg("s",str),arg("pat",str))),
	command("str", "like3", STRlikewrap3, false, "SQL pattern match function", args(1,4, arg("",bit),arg("s",str),arg("pat",str),arg("esc",str))),
	command("str", "ascii", STRascii, false, "Return unicode of head of string", args(1,2, arg("",int),arg("s",str))),
	command("str", "substring", STRsubstringTail, false, "Extract the tail of a string", args(1,3, arg("",str),arg("s",str),arg("start",int))),
	command("str", "substring3", STRsubstring, false, "Extract a substring from str starting at start, for length len", args(1,4, arg("",str),arg("s",str),arg("start",int),arg("len",int))),
	command("str", "prefix", STRprefix, false, "Extract the prefix of a given length", args(1,3, arg("",str),arg("s",str),arg("l",int))),
	command("str", "suffix", STRsuffix, false, "Extract the suffix of a given length", args(1,3, arg("",str),arg("s",str),arg("l",int))),
	command("str", "stringleft", STRprefix, false, "", args(1,3, arg("",str),arg("s",str),arg("l",int))),
	command("str", "stringright", STRsuffix, false, "", args(1,3, arg("",str),arg("s",str),arg("l",int))),
	command("str", "locate", STRlocate, false, "Locate the start position of a string", args(1,3, arg("",int),arg("s1",str),arg("s2",str))),
	command("str", "locate3", STRlocate3, false, "Locate the start position of a string", args(1,4, arg("",int),arg("s1",str),arg("s2",str),arg("start",int))),
	command("str", "insert", STRinsert, false, "Insert a string into another", args(1,5, arg("",str),arg("s",str),arg("start",int),arg("l",int),arg("s2",str))),
	command("str", "replace", STRreplace, false, "Insert a string into another", args(1,4, arg("",str),arg("s",str),arg("pat",str),arg("s2",str))),
	command("str", "repeat", STRrepeat, false, "", args(1,3, arg("",str),arg("s2",str),arg("c",int))),
	command("str", "space", STRspace, false, "", args(1,2, arg("",str),arg("l",int))),
	command("str", "asciify", STRasciify, false, "Transform string from UTF8 to ASCII", args(1, 2, arg("out",str), arg("in",str))),
	pattern("str", "startswithselect", STRstartswithselect, false, "Select all head values of the first input BAT for which the\ntail value starts with the given prefix.", args(1,5, batarg("",oid),batarg("b",str),batarg("s",oid),arg("prefix",str),arg("anti",bit))),
	pattern("str", "startswithselect", STRstartswithselect, false, "Select all head values of the first input BAT for which the\ntail value starts with the given prefix + icase.", args(1,6, batarg("",oid),batarg("b",str),batarg("s",oid),arg("prefix",str),arg("caseignore",bit),arg("anti",bit))),
	pattern("str", "endswithselect", STRendswithselect, false, "Select all head values of the first input BAT for which the\ntail value end with the given suffix.", args(1,5, batarg("",oid),batarg("b",str),batarg("s",oid),arg("suffix",str),arg("anti",bit))),
	pattern("str", "endswithselect", STRendswithselect, false, "Select all head values of the first input BAT for which the\ntail value end with the given suffix + icase.", args(1,6, batarg("",oid),batarg("b",str),batarg("s",oid),arg("suffix",str),arg("caseignore",bit),arg("anti",bit))),
	pattern("str", "containsselect", STRcontainsselect, false, "Select all head values of the first input BAT for which the\ntail value contains the given needle.", args(1,5, batarg("",oid),batarg("b",str),batarg("s",oid),arg("needle",str),arg("anti",bit))),
	pattern("str", "containsselect", STRcontainsselect, false, "Select all head values of the first input BAT for which the\ntail value contains the given needle + icase.", args(1,6, batarg("",oid),batarg("b",str),batarg("s",oid),arg("needle",str),arg("caseignore",bit),arg("anti",bit))),

	pattern("str", "startswithjoin", STRstartswithjoin, false, "Join the string bat L with the prefix bat R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows.", args(2,9, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
	pattern("str", "startswithjoin", STRstartswithjoin, false, "Join the string bat L with the prefix bat R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows + icase.", args(2,10, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
	pattern("str", "startswithjoin", STRstartswithjoin, false, "The same as STRstartswithjoin, but only produce one output.", args(1,8,batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
	pattern("str", "startswithjoin", STRstartswithjoin, false, "The same as STRstartswithjoin, but only produce one output + icase.", args(1,9,batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),

	pattern("str", "endswithjoin", STRendswithjoin, false, "Join the string bat L with the suffix bat R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows.", args(2,9, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
	pattern("str", "endswithjoin", STRendswithjoin, false, "Join the string bat L with the suffix bat R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows + icase.", args(2,10, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
	pattern("str", "endswithjoin", STRendswithjoin, false, "The same as STRendswithjoin, but only produce one output.", args(1,8,batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
	pattern("str", "endswithjoin", STRendswithjoin, false, "The same as STRendswithjoin, but only produce one output + icase.", args(1,9,batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
	pattern("str", "containsjoin", STRcontainsjoin, false, "Join the string bat L with the bat R if L contains the string of R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows.", args(2,9, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
	pattern("str", "containsjoin", STRcontainsjoin, false, "Join the string bat L with the bat R if L contains the string of R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows + icase.", args(2,10, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
	pattern("str", "containsjoin", STRcontainsjoin, false, "The same as STRcontainsjoin, but only produce one output.", args(1,8,batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
	pattern("str", "containsjoin", STRcontainsjoin, false, "The same as STRcontainsjoin, but only produce one output + icase.", args(1,9,batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
	{ .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_str_mal)
{ mal_module2("str", NULL, str_init_funcs, NULL, NULL); }
