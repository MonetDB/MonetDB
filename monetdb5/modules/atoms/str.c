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
STRlikewrap3(bit *ret, const char *const *s, const char *const *pat, const char *const *esc)
{
	if (strNil(*s) || strNil(*pat) || strNil(*esc))
		*ret = bit_nil;
	else
		*ret = (bit) STRlike(*s, *pat, *esc);
	return MAL_SUCCEED;
}

static str
STRlikewrap(bit *ret, const char *const *s, const char *const *pat)
{
	if (strNil(*s) || strNil(*pat))
		*ret = bit_nil;
	else
		*ret = (bit) STRlike(*s, *pat, NULL);
	return MAL_SUCCEED;
}

static str
STRtostr(str *res, const char *const *src)
{
	if (*src == 0)
		*res = GDKstrdup(str_nil);
	else
		*res = GDKstrdup(*src);
	if (*res == NULL)
		throw(MAL, "str.str", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
STRLength(int *res, const char *const *arg1)
{
	const char *s = *arg1;

	*res = strNil(s) ? int_nil : UTF8_strlen(s);
	return MAL_SUCCEED;
}

static str
STRBytes(int *res, const char *const *arg1)
{
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
STRTail(str *res, const char *const *arg1, const int *offset)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int off = *offset;

	if (strNil(s) || is_int_nil(off)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.tail", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_tail(&buf, &buflen, s, off)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRSubString(str *res, const char *const *arg1, const int *offset, const int *length)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int off = *offset, len = *length;

	if (strNil(s) || is_int_nil(off) || is_int_nil(len)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_Sub_String(&buf, &buflen, s, off, len)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRFromWChr(str *res, const int *c)
{
	str buf = NULL, msg = MAL_SUCCEED;
	int cc = *c;

	if (is_int_nil(cc)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = MAX(strlen(str_nil) + 1, 8);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_from_wchr(&buf, &buflen, cc)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRWChrAt(int *res, const char *const *arg1, const int *at)
{
	return str_wchr_at(res, *arg1, *at);
}

static inline str
doStrConvert(str *res, const char *arg1, gdk_return (*func)(char **restrict, size_t *restrict, const char *restrict))
{
	str buf = NULL, msg = MAL_SUCCEED;

	if (strNil(arg1)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.lower", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((*func)(&buf, &buflen, arg1) != GDK_SUCCEED) {
			GDKfree(buf);
			throw(MAL, "str.lower", GDK_EXCEPTION);
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.lower",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static inline str
STRlower(str *res, const char *const *arg1)
{
	return doStrConvert(res, *arg1, GDKtolower);
}

static inline str
STRupper(str *res, const char *const *arg1)
{
	return doStrConvert(res, *arg1, GDKtoupper);
}

static inline str
STRcasefold(str *res, const char *const *arg1)
{
	return doStrConvert(res, *arg1, GDKcasefold);
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
STRsplitpart(str *res, const char *const *haystack, const char *const *needle, const int *field)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *haystack, *s2 = *needle;
	int f = *field;

	if (strNil(s) || strNil(s2) || is_int_nil(f)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_splitpart(&buf, &buflen, s, s2, f)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRStrip(str *res, const char *const *arg1)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;

	if (strNil(s)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.strip", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_strip(&buf, &buflen, s)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRLtrim(str *res, const char *const *arg1)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;

	if (strNil(s)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.ltrim", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_ltrim(&buf, &buflen, s)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRRtrim(str *res, const char *const *arg1)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;

	if (strNil(s)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rtrim", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rtrim(&buf, &buflen, s)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRStrip2(str *res, const char *const *arg1, const char *const *arg2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;

	if (strNil(s) || strNil(s2)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH * sizeof(int);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.strip2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_strip2(&buf, &buflen, s, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRLtrim2(str *res, const char *const *arg1, const char *const *arg2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;

	if (strNil(s) || strNil(s2)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH * sizeof(int);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.ltrim2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_ltrim2(&buf, &buflen, s, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRRtrim2(str *res, const char *const *arg1, const char *const *arg2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;

	if (strNil(s) || strNil(s2)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH * sizeof(int);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rtrim2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rtrim2(&buf, &buflen, s, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRLpad(str *res, const char *const *arg1, const int *len)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *len;

	if (strNil(s) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.lpad", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_lpad(&buf, &buflen, s, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRRpad(str *res, const char *const *arg1, const int *len)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *len;

	if (strNil(s) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rpad", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rpad(&buf, &buflen, s, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRLpad3(str *res, const char *const *arg1, const int *len, const char *const *arg2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;
	int l = *len;

	if (strNil(s) || strNil(s2) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.lpad2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_lpad3(&buf, &buflen, s, l, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRRpad3(str *res, const char *const *arg1, const int *len, const char *const *arg2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;
	int l = *len;

	if (strNil(s) || strNil(s2) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rpad2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rpad3(&buf, &buflen, s, l, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRSubstitute(str *res, const char *const *arg1, const char *const *arg2, const char *const *arg3,
			  const bit *g)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2, *s3 = *arg3;

	if (strNil(s) || strNil(s2) || strNil(s3)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substitute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_substitute(&buf, &buflen, s, s2, s3, *g)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.substitute",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRascii(int *ret, const char *const *s)
{
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
STRsubstringTail(str *res, const char *const *arg1, const int *start)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int st = *start;

	if (strNil(s) || is_int_nil(st)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substringTail", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_substring_tail(&buf, &buflen, s, st)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRsubstring(str *res, const char *const *arg1, const int *start, const int *ll)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int st = *start, l = *ll;

	if (strNil(s) || is_int_nil(st) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_sub_string(&buf, &buflen, s, st, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.substring",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRprefix(str *res, const char *const *arg1, const int *ll)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *ll;

	if (strNil(s) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.prefix", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_Sub_String(&buf, &buflen, s, 0, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRsuffix(str *res, const char *const *arg1, const int *ll)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *ll;

	if (strNil(s) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.suffix", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_suffix(&buf, &buflen, s, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
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
STRlocate3(int *ret, const char *const *needle, const char *const *haystack, const int *start)
{
	const char *s = *needle, *s2 = *haystack;
	int st = *start;

	*ret = (strNil(s) || strNil(s2) || is_int_nil(st)) ?
		int_nil :
		str_locate2(s, s2, st);
	return MAL_SUCCEED;
}

static str
STRlocate(int *ret, const char *const *needle, const char *const *haystack)
{
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
	strcpy(v, s2);
	if (strt + l < l1)
		strcat(v, UTF8_strtail(s, strt + l));
	return MAL_SUCCEED;
}

static str
STRinsert(str *res, const char *const *input, const int *start, const int *nchars,
		  const char *const *input2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *input, *s2 = *input2;
	int st = *start, n = *nchars;

	if (strNil(s) || is_int_nil(st) || is_int_nil(n) || strNil(s2)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_insert(&buf, &buflen, s, st, n, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.insert",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRreplace(str *ret, const char *const *s1, const char *const *s2, const char *const *s3)
{
	bit flag = TRUE;
	return STRSubstitute(ret, s1, s2, s3, &flag);
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
STRrepeat(str *res, const char *const *arg1, const int *c)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int cc = *c;

	if (strNil(s) || is_int_nil(cc) || cc < 0) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_repeat(&buf, &buflen, s, cc)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.repeat",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRspace(str *res, const int *ll)
{
	str buf = NULL, msg = MAL_SUCCEED;
	int l = *ll;

	if (is_int_nil(l) || l < 0) {
		*res = GDKstrdup(str_nil);
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
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.space",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRasciify(str *r, const char *const *s)
{
	char *buf = NULL;
	size_t buflen = 0;
	if (GDKasciify(&buf, &buflen, *s) != GDK_SUCCEED)
		throw(MAL, "str.asciify", GDK_EXCEPTION);
	*r = buf;
	return MAL_SUCCEED;
}

static inline void
BBPnreclaim(int nargs, ...)
{
	va_list valist;
	va_start(valist, nargs);
	for (int i = 0; i < nargs; i++) {
		BAT *b = va_arg(valist, BAT *);
		BBPreclaim(b);
	}
	va_end(valist);
}

#define HANDLE_TIMEOUT(qc)									\
	do {													\
		TIMEOUT_ERROR(qc, __FILE__, __func__, __LINE__);	\
		msg = createException(MAL, fname, GDK_EXCEPTION);	\
	} while (0)

#define scanloop(TEST, canditer_next)						\
	do {													\
		const oid off = b->hseqbase;						\
		TIMEOUT_LOOP(ci.ncand, qry_ctx) {					\
			oid o = canditer_next(&ci);						\
			const char *restrict v = BUNtvar(bi, o - off);	\
			assert(rcnt < BATcapacity(bn));					\
			if (TEST)										\
				vals[rcnt++] = o;							\
		}													\
	} while (0)

static str
STRselect(MalStkPtr stk, InstrPtr pci,
		  int (*str_icmp)(const char *, const char *, int),
		  int (*str_cmp)(const char *, const char *, int),
		  const char *fname)
{
	str msg = MAL_SUCCEED;

	bat *r_id = getArgReference_bat(stk, pci, 0);
	bat b_id = *getArgReference_bat(stk, pci, 1);
	bat cb_id = *getArgReference_bat(stk, pci, 2);
	const char *key = *getArgReference_str(stk, pci, 3);
	bit icase = pci->argc != 5;
	bit anti = pci->argc == 5 ? *getArgReference_bit(stk, pci, 4) :
		*getArgReference_bit(stk, pci, 5);

	BAT *b, *cb = NULL, *bn = NULL, *old_s = NULL;;
	BUN rcnt = 0;
	struct canditer ci;
	bool with_strimps = false,
		with_strimps_anti = false;

	if (!(b = BATdescriptor(b_id)))
		throw(MAL, fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!is_bat_nil(cb_id) && !(cb = BATdescriptor(cb_id))) {
		BBPreclaim(b);
		throw(MAL, fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	assert(ATOMstorage(b->ttype) == TYPE_str);

	if (BAThasstrimps(b)) {
		BAT *tmp_s;
		if (STRMPcreate(b, NULL) == GDK_SUCCEED && (tmp_s = STRMPfilter(b, cb, key, anti)) != NULL) {
			old_s = cb;
			cb = tmp_s;
			if (!anti)
				with_strimps = true;
			else
				with_strimps_anti = true;
		} else {
			/* strimps failed, continue without */
			GDKclrerr();
		}
	}

	MT_thread_setalgorithm(with_strimps ?
						   "string_select: strcmp function using strimps" :
						   (with_strimps_anti ?
							"string_select: strcmp function using strimps anti"
							: "string_select: strcmp function with no accelerator"));

	canditer_init(&ci, b, cb);
	if (!(bn = COLnew(0, TYPE_oid, ci.ncand, TRANSIENT))) {
		BBPnreclaim(2, b, cb);
		throw(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	if (!strNil(key)) {
		BATiter bi = bat_iterator(b);
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		if (icase)
			str_cmp = str_icmp;
		oid *vals = Tloc(bn, 0);
		const int klen = str_strlen(key);
		if (ci.tpe == cand_dense) {
			if (with_strimps_anti)
				scanloop(strNil(v) || str_cmp(v, key, klen) == 0, canditer_next_dense);
			else if (anti)
				scanloop(!strNil(v) && str_cmp(v, key, klen) != 0, canditer_next_dense);
			else
				scanloop(!strNil(v) && str_cmp(v, key, klen) == 0, canditer_next_dense);
		} else {
			if (with_strimps_anti)
				scanloop(strNil(v) || str_cmp(v, key, klen) == 0, canditer_next);
			else if (anti)
				scanloop(!strNil(v) && str_cmp(v, key, klen) != 0, canditer_next);
			else
				scanloop(!strNil(v) && str_cmp(v, key, klen) == 0, canditer_next);
		}
		bat_iterator_end(&bi);
		TIMEOUT_CHECK(qry_ctx, HANDLE_TIMEOUT(qry_ctx));

		if (!msg) {
			BATsetcount(bn, rcnt);
			bn->tsorted = true;
			bn->trevsorted = bn->batCount <= 1;
			bn->tkey = true;
			bn->tnil = false;
			bn->tnonil = true;
			bn->tseqbase = rcnt == 0 ?
				0 : rcnt == 1 ?
				*(const oid *) Tloc(bn, 0) : rcnt == ci.ncand && ci.tpe == cand_dense ? ci.hseq : oid_nil;

			if (with_strimps_anti) {
				BAT *rev;
				if (old_s) {
					rev = BATdiffcand(old_s, bn);
#ifndef NDEBUG
					BAT *is = BATintersectcand(old_s, bn);
					if (is) {
						assert(is->batCount == bn->batCount);
						BBPreclaim(is);
					}
					assert(rev->batCount == old_s->batCount - bn->batCount);
#endif
				} else
					rev = BATnegcands(0, b->batCount, bn);

				BBPreclaim(bn);
				bn = rev;
				if (bn == NULL)
					msg = createException(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	}

	if (bn && !msg) {
		*r_id = bn->batCacheid;
		BBPkeepref(bn);
	} else {
		BBPreclaim(bn);
	}

	BBPnreclaim(3, b, cb, old_s);
	return msg;
}

/**
 * @r_id: result oid
 * @b_id: input bat oid
 * @cb_id: input bat candidates oid
 * @key: input string
 * @icase: ignore case
 * @anti: anti join
 */
static str
STRstartswithselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	return STRselect(stk, pci,
					 str_is_iprefix, str_is_prefix, "str.startswithselect");
}

/**
 * @r_id: result oid
 * @b_id: input bat oid
 * @cb_id: input bat candidates oid
 * @key: input string
 * @icase: ignore case
 * @anti: anti join
 */
static str
STRendswithselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	return STRselect(stk, pci,
					 str_is_isuffix, str_is_suffix, "str.endswithselect");
}

/**
 * @r_id: result oid
 * @b_id: input bat oid
 * @cb_id: input bat candidates oid
 * @key: input string
 * @icase: ignore case
 * @anti: anti join
 */
static str
STRcontainsselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	return STRselect(stk, pci,
					 str_icontains, str_contains, "str.containsselect");
}

#define APPEND(b, o) (((oid *) b->theap->base)[b->batCount++] = (o))
#define VALUE(s, x)  (s##vars + VarHeapVal(s##vals, (x), s##i.width))

#define set_empty_bat_props(B)					\
	do {										\
		B->tnil = false;						\
		B->tnonil = true;						\
		B->tkey = true;							\
		B->tsorted = true;						\
		B->trevsorted = true;					\
		B->tseqbase = 0;						\
	} while (0)

#define CONTAINS_JOIN_LOOP(STR_CMP, STR_LEN)							\
	do {																\
		canditer_init(&rci, r, cr);										\
		for (BUN ridx = 0; ridx < rci.ncand; ridx++) {					\
			BAT *filtered_sl = NULL;									\
			GDK_CHECK_TIMEOUT(qry_ctx, counter, GOTO_LABEL_TIMEOUT_HANDLER(exit, qry_ctx)); \
			ro = canditer_next(&rci);									\
			vr = VALUE(r, ro - rbase);									\
			matches = 0;												\
			if (!strNil(vr)) {											\
				vr_len = STR_LEN;										\
				if (with_strimps)										\
					filtered_sl = STRMPfilter(l, cl, vr, anti);			\
				if (filtered_sl)										\
					canditer_init(&lci, l, filtered_sl);				\
				else													\
					canditer_init(&lci, l, cl);							\
				for (BUN lidx = 0; lidx < lci.ncand; lidx++) {			\
					lo = canditer_next(&lci);							\
					vl = VALUE(l, lo - lbase);							\
					if (strNil(vl))										\
						continue;										\
					if (STR_CMP)										\
						continue;										\
					if (BATcount(rl) == BATcapacity(rl)) {				\
						newcap = BATgrows(rl);							\
						BATsetcount(rl, BATcount(rl));					\
						if (rr)											\
							BATsetcount(rr, BATcount(rr));				\
						if (BATextend(rl, newcap) != GDK_SUCCEED ||		\
							(rr && BATextend(rr, newcap) != GDK_SUCCEED)) { \
							msg = createException(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
							goto exit;									\
						}												\
						assert(!rr || BATcapacity(rl) == BATcapacity(rr)); \
					}													\
					if (BATcount(rl) > 0) {								\
						if (lastl + 1 != lo)							\
							rl->tseqbase = oid_nil;						\
						if (matches == 0) {								\
							if (rr)										\
								rr->trevsorted = false;					\
							if (lastl > lo) {							\
								rl->tsorted = false;					\
								rl->tkey = false;						\
							} else if (lastl < lo) {					\
								rl->trevsorted = false;					\
							} else {									\
								rl->tkey = false;						\
							}											\
						}												\
					}													\
					APPEND(rl, lo);										\
					if (rr)												\
						APPEND(rr, ro);									\
					lastl = lo;											\
					matches++;											\
				}														\
				BBPreclaim(filtered_sl);								\
			}															\
			if (rr) {													\
				if (matches > 1) {										\
					rr->tkey = false;									\
					rr->tseqbase = oid_nil;								\
					rl->trevsorted = false;								\
				} else if (matches == 0) {								\
					rskipped = BATcount(rr) > 0;						\
				} else if (rskipped) {									\
					rr->tseqbase = oid_nil;								\
				}														\
			} else if (matches > 1) {									\
				rl->trevsorted = false;									\
			}															\
		}																\
	} while (0)

#define STR_JOIN_NESTED_LOOP(STR_CMP, STR_LEN, FNAME)					\
	do {																\
		canditer_init(&rci, r, cr);										\
		for (BUN ridx = 0; ridx < rci.ncand; ridx++) {					\
			GDK_CHECK_TIMEOUT(qry_ctx, counter, GOTO_LABEL_TIMEOUT_HANDLER(exit, qry_ctx)); \
			ro = canditer_next(&rci);									\
			vr = VALUE(r, ro - rbase);									\
			matches = 0;												\
			if (!strNil(vr)) {											\
				vr_len = STR_LEN;										\
				canditer_init(&lci, l, cl);								\
				for (BUN lidx = 0; lidx < lci.ncand; lidx++) {			\
					lo = canditer_next(&lci);							\
					vl = VALUE(l, lo - lbase);							\
					if (strNil(vl))										\
						continue;										\
					if (!(STR_CMP))										\
						continue;										\
					if (BATcount(rl) == BATcapacity(rl)) {				\
						newcap = BATgrows(rl);							\
						BATsetcount(rl, BATcount(rl));					\
						if (rr)											\
							BATsetcount(rr, BATcount(rr));				\
						if (BATextend(rl, newcap) != GDK_SUCCEED ||		\
							(rr && BATextend(rr, newcap) != GDK_SUCCEED)) { \
							msg = createException(MAL, FNAME, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
							goto exit;									\
						}												\
						assert(!rr || BATcapacity(rl) == BATcapacity(rr)); \
					}													\
					if (BATcount(rl) > 0) {								\
						if (last_lo + 1 != lo)							\
							rl->tseqbase = oid_nil;						\
						if (matches == 0) {								\
							if (rr)										\
								rr->trevsorted = false;					\
							if (last_lo > lo) {							\
								rl->tsorted = false;					\
								rl->tkey = false;						\
							} else if (last_lo < lo) {					\
								rl->trevsorted = false;					\
							} else {									\
								rl->tkey = false;						\
							}											\
						}												\
					}													\
					APPEND(rl, lo);										\
					if (rr)												\
						APPEND(rr, ro);									\
					last_lo = lo;										\
					matches++;											\
				}														\
			}															\
			if (rr) {													\
				if (matches > 1) {										\
					rr->tkey = false;									\
					rr->tseqbase = oid_nil;								\
					rl->trevsorted = false;								\
				} else if (matches == 0) {								\
					rskipped = BATcount(rr) > 0;						\
				} else if (rskipped) {									\
					rr->tseqbase = oid_nil;								\
				}														\
			} else if (matches > 1) {									\
				rl->trevsorted = false;									\
			}															\
		}																\
	} while (0)

#define STARTSWITH_SORTED_LOOP(STR_CMP, STR_LEN, FNAME)					\
	do {																\
		canditer_init(&rci, sorted_r, sorted_cr);						\
		canditer_init(&lci, sorted_l, sorted_cl);						\
		for (lx = 0; lx < lci.ncand; lx++) {							\
			lo = canditer_next(&lci);									\
			vl = VALUE(l, lo - lbase);									\
			if (!strNil(vl))											\
				break;													\
		}																\
		for (rx = 0; rx < rci.ncand; rx++) {							\
			ro = canditer_next(&rci);									\
			vr = VALUE(r, ro - rbase);									\
			if (!strNil(vr)) {											\
				canditer_setidx(&rci, rx);								\
				break;													\
			}															\
		}																\
		for (; rx < rci.ncand; rx++) {									\
			GDK_CHECK_TIMEOUT(qry_ctx, counter, GOTO_LABEL_TIMEOUT_HANDLER(exit, qry_ctx)); \
			ro = canditer_next(&rci);									\
			vr = VALUE(r, ro - rbase);									\
			vr_len = STR_LEN;											\
			matches = 0;												\
			for (canditer_setidx(&lci, lx), n = lx; n < lci.ncand; n++) { \
				lo = canditer_next_dense(&lci);							\
				vl = VALUE(l, lo - lbase);								\
				cmp = STR_CMP;											\
				if (cmp < 0) {											\
					lx++;												\
					continue;											\
				}														\
				else if (cmp > 0)										\
					break;												\
				if (BATcount(rl) == BATcapacity(rl)) {					\
					newcap = BATgrows(rl);								\
					BATsetcount(rl, BATcount(rl));						\
					if (rr)												\
						BATsetcount(rr, BATcount(rr));					\
					if (BATextend(rl, newcap) != GDK_SUCCEED ||			\
						(rr && BATextend(rr, newcap) != GDK_SUCCEED)) { \
						msg = createException(MAL, FNAME, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
						goto exit;										\
					}													\
					assert(!rr || BATcapacity(rl) == BATcapacity(rr));	\
				}														\
				if (BATcount(rl) > 0) {									\
					if (last_lo + 1 != lo)								\
						rl->tseqbase = oid_nil;							\
					if (matches == 0) {									\
						if (rr)											\
							rr->trevsorted = false;						\
						if (last_lo > lo) {								\
							rl->tsorted = false;						\
							rl->tkey = false;							\
						} else if (last_lo < lo) {						\
							rl->trevsorted = false;						\
						} else {										\
							rl->tkey = false;							\
						}												\
					}													\
				}														\
				APPEND(rl, lo);											\
				if (rr)													\
					APPEND(rr, ro);										\
				last_lo = lo;											\
				matches++;												\
			}															\
			if (rr) {													\
				if (matches > 1) {										\
					rr->tkey = false;									\
					rr->tseqbase = oid_nil;								\
					rl->trevsorted = false;								\
				} else if (matches == 0) {								\
					rskipped = BATcount(rr) > 0;						\
				} else if (rskipped) {									\
					rr->tseqbase = oid_nil;								\
				}														\
			} else if (matches > 1) {									\
				rl->trevsorted = false;									\
			}															\
		}																\
	} while (0)

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
batstr_strrev(BAT *b)
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

static BAT *
batstr_strlower(BAT *b)
{
	BAT *bn = NULL;
	BATiter bi;
	BUN p, q;

	assert(b->ttype == TYPE_str);

	bn = COLnew(b->hseqbase, TYPE_str, BATcount(b), TRANSIENT);
	if (bn == NULL)
		return NULL;

	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *vb = BUNtail(bi, p);
		char *vb_low = NULL;
		if (STRlower(&vb_low, &vb)) {
			bat_iterator_end(&bi);
			BBPreclaim(bn);
			return NULL;
		}
		if (BUNappend(bn, vb_low, false) != GDK_SUCCEED) {
			GDKfree(vb_low);
			bat_iterator_end(&bi);
			BBPreclaim(bn);
			return NULL;
		}
		GDKfree(vb_low);
	}
	bat_iterator_end(&bi);
	return bn;
}

static str
str_join_nested(BAT *rl, BAT *rr, BAT *l, BAT *r, BAT *cl, BAT *cr,
				bit anti, int (*str_cmp)(const char *, const char *, int),
				const char *fname)
{
	str msg = MAL_SUCCEED;

	size_t counter = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	TRC_DEBUG(ALGO,
			  "(%s, %s, l=%s#" BUNFMT "[%s]%s%s,"
			  "r=%s#" BUNFMT "[%s]%s%s,sl=%s#" BUNFMT "%s%s,"
			  "sr=%s#" BUNFMT "%s%s)\n",
			  fname, "nested loop",
			  BATgetId(l), BATcount(l), ATOMname(l->ttype),
			  l->tsorted ? "-sorted" : "",
			  l->trevsorted ? "-revsorted" : "",
			  BATgetId(r), BATcount(r), ATOMname(r->ttype),
			  r->tsorted ? "-sorted" : "",
			  r->trevsorted ? "-revsorted" : "",
			  cl ? BATgetId(cl) : "NULL", cl ? BATcount(cl) : 0,
			  cl && cl->tsorted ? "-sorted" : "",
			  cl && cl->trevsorted ? "-revsorted" : "",
			  cr ? BATgetId(cr) : "NULL", cr ? BATcount(cr) : 0,
			  cr && cr->tsorted ? "-sorted" : "",
			  cr && cr->trevsorted ? "-revsorted" : "");

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(ATOMtype(l->ttype) == TYPE_str);

	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	assert(ri.vh && r->ttype);

	struct canditer lci, rci;
	oid lbase = l->hseqbase,
		rbase = r->hseqbase,
		lo, ro, last_lo = 0;
	const char *lvals = (const char *) li.base,
		*rvals = (const char *) ri.base,
		*lvars = li.vh->base,
		*rvars = ri.vh->base,
		*vl, *vr;
	BUN matches, newcap;
	int rskipped = 0, vr_len = 0;

	if (anti)
		STR_JOIN_NESTED_LOOP((str_cmp(vl, vr, vr_len) != 0), str_strlen(vr), fname);
	else
		STR_JOIN_NESTED_LOOP((str_cmp(vl, vr, vr_len) == 0), str_strlen(vr), fname);

	assert(!rr || BATcount(rl) == BATcount(rr));
	BATsetcount(rl, BATcount(rl));
	if (rr)
		BATsetcount(rr, BATcount(rr));

	if (BATcount(rl) > 0) {
		if (BATtdense(rl))
			rl->tseqbase = ((oid *) rl->theap->base)[0];
		if (rr && BATtdense(rr))
			rr->tseqbase = ((oid *) rr->theap->base)[0];
	} else {
		rl->tseqbase = 0;
		if (rr)
			rr->tseqbase = 0;
	}

	TRC_DEBUG(ALGO,
			  "(%s, l=%s,r=%s)=(%s#" BUNFMT "%s%s,%s#" BUNFMT "%s%s\n",
			  fname,
			  BATgetId(l), BATgetId(r), BATgetId(rl), BATcount(rl),
			  rl->tsorted ? "-sorted" : "",
			  rl->trevsorted ? "-revsorted" : "",
			  rr ? BATgetId(rr) : NULL, rr ? BATcount(rr) : 0,
			  rr && rr->tsorted ? "-sorted" : "",
			  rr && rr->trevsorted ? "-revsorted" : "");

exit:
	bat_iterator_end(&li);
	bat_iterator_end(&ri);
	return msg;
}

static str
contains_join(BAT *rl, BAT *rr, BAT *l, BAT *r, BAT *cl, BAT *cr, bit anti,
			  int (*str_cmp)(const char *, const char *, int),
			  const char *fname)
{
	str msg = MAL_SUCCEED;

	size_t counter = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	TRC_DEBUG(ALGO,
			  "(%s, l=%s#" BUNFMT "[%s]%s%s,"
			  "r=%s#" BUNFMT "[%s]%s%s,sl=%s#" BUNFMT "%s%s,"
			  "sr=%s#" BUNFMT "%s%s)\n",
			  fname,
			  BATgetId(l), BATcount(l), ATOMname(l->ttype),
			  l->tsorted ? "-sorted" : "",
			  l->trevsorted ? "-revsorted" : "",
			  BATgetId(r), BATcount(r), ATOMname(r->ttype),
			  r->tsorted ? "-sorted" : "",
			  r->trevsorted ? "-revsorted" : "",
			  cl ? BATgetId(cl) : "NULL", cl ? BATcount(cl) : 0,
			  cl && cl->tsorted ? "-sorted" : "",
			  cl && cl->trevsorted ? "-revsorted" : "",
			  cr ? BATgetId(cr) : "NULL", cr ? BATcount(cr) : 0,
			  cr && cr->tsorted ? "-sorted" : "",
			  cr && cr->trevsorted ? "-revsorted" : "");

	bool with_strimps = false;

	if (BAThasstrimps(l)) {
		with_strimps = true;
		if (STRMPcreate(l, NULL) != GDK_SUCCEED) {
			GDKclrerr();
			with_strimps = false;
		}
	}

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(ATOMtype(l->ttype) == TYPE_str);

	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	assert(ri.vh && r->ttype);

	struct canditer lci, rci;
	oid lbase = l->hseqbase,
		rbase = r->hseqbase,
		lo, ro, lastl = 0;
	const char *lvals = (const char *) li.base,
		*rvals = (const char *) ri.base,
		*lvars = li.vh->base,
		*rvars = ri.vh->base,
		*vl, *vr;
	int rskipped = 0, vr_len = 0;
	BUN matches, newcap;

	if (anti)
		CONTAINS_JOIN_LOOP(str_cmp(vl, vr, vr_len) == 0, str_strlen(vr));
	else
		CONTAINS_JOIN_LOOP(str_cmp(vl, vr, vr_len) != 0, str_strlen(vr));

	assert(!rr || BATcount(rl) == BATcount(rr));
	BATsetcount(rl, BATcount(rl));
	if (rr)
		BATsetcount(rr, BATcount(rr));
	if (BATcount(rl) > 0) {
		if (BATtdense(rl))
			rl->tseqbase = ((oid *) rl->theap->base)[0];
		if (rr && BATtdense(rr))
			rr->tseqbase = ((oid *) rr->theap->base)[0];
	} else {
		rl->tseqbase = 0;
		if (rr)
			rr->tseqbase = 0;
	}

	TRC_DEBUG(ALGO,
			  "(%s, l=%s,r=%s)=(%s#" BUNFMT "%s%s,%s#" BUNFMT "%s%s\n",
			  fname,
			  BATgetId(l), BATgetId(r), BATgetId(rl), BATcount(rl),
			  rl->tsorted ? "-sorted" : "",
			  rl->trevsorted ? "-revsorted" : "",
			  rr ? BATgetId(rr) : NULL, rr ? BATcount(rr) : 0,
			  rr && rr->tsorted ? "-sorted" : "",
			  rr && rr->trevsorted ? "-revsorted" : "");
exit:
	bat_iterator_end(&li);
	bat_iterator_end(&ri);
	return msg;
}

static str
startswith_join(BAT **rl_ptr, BAT **rr_ptr, BAT *l, BAT *r, BAT *cl, BAT *cr,
				bit anti, int (*str_cmp)(const char *, const char *, int),
				const char *fname)
{
	str msg = MAL_SUCCEED;
	gdk_return rc;

	size_t counter = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	assert(*rl_ptr && *rr_ptr);

	BAT *sorted_l = NULL, *sorted_r = NULL,
		*sorted_cl = NULL, *sorted_cr = NULL,
		*ord_sorted_l = NULL, *ord_sorted_r = NULL,
		*proj_rl = NULL, *proj_rr = NULL,
		*rl = *rl_ptr, *rr = *rr_ptr;

	TRC_DEBUG(ALGO,
			  "(%s, %s, l=%s#" BUNFMT "[%s]%s%s,"
			  "r=%s#" BUNFMT "[%s]%s%s,sl=%s#" BUNFMT "%s%s,"
			  "sr=%s#" BUNFMT "%s%s)\n",
			  fname, "sorted inputs",
			  BATgetId(l), BATcount(l), ATOMname(l->ttype),
			  l->tsorted ? "-sorted" : "",
			  l->trevsorted ? "-revsorted" : "",
			  BATgetId(r), BATcount(r), ATOMname(r->ttype),
			  r->tsorted ? "-sorted" : "",
			  r->trevsorted ? "-revsorted" : "",
			  cl ? BATgetId(cl) : "NULL", cl ? BATcount(cl) : 0,
			  cl && cl->tsorted ? "-sorted" : "",
			  cl && cl->trevsorted ? "-revsorted" : "",
			  cr ? BATgetId(cr) : "NULL", cr ? BATcount(cr) : 0,
			  cr && cr->tsorted ? "-sorted" : "",
			  cr && cr->trevsorted ? "-revsorted" : "");

	bool l_sorted = BATordered(l);
	bool r_sorted = BATordered(r);

	if (l_sorted == FALSE) {
		rc = BATsort(&sorted_l, &ord_sorted_l, NULL,
					 l, NULL, NULL, false, false, false);
		if (rc != GDK_SUCCEED) {
			throw(MAL, fname, "Sorting left input failed");
		} else {
			if (cl) {
				rc = BATsort(&sorted_cl, NULL, NULL,
							 cl, ord_sorted_l, NULL, false, false, false);
				if (rc != GDK_SUCCEED) {
					BBPnreclaim(2, sorted_l, ord_sorted_l);
					throw(MAL, fname, "Sorting left candidates input failed");
				}
			}
		}
	} else {
		sorted_l = l;
		sorted_cl = cl;
	}

	if (r_sorted == FALSE) {
		rc = BATsort(&sorted_r, &ord_sorted_r, NULL,
					 r, NULL, NULL, false, false, false);
		if (rc != GDK_SUCCEED) {
			BBPnreclaim(3, sorted_l, ord_sorted_l, sorted_cl);
			throw(MAL, fname, "Sorting right input failed");
		} else {
			if (cr) {
				rc = BATsort(&sorted_cr, NULL, NULL,
							 cr, ord_sorted_r, NULL, false, false, false);
				if (rc != GDK_SUCCEED) {
					BBPnreclaim(5, sorted_l, ord_sorted_l, sorted_cl, sorted_r, ord_sorted_r);
					throw(MAL, fname, "Sorting right candidates input failed");
				}
			}
		}
	} else {
		sorted_r = r;
		sorted_cr = cr;
	}

	assert(BATordered(sorted_l) && BATordered(sorted_r));

	BATiter li = bat_iterator(sorted_l);
	BATiter ri = bat_iterator(sorted_r);
	assert(ri.vh && r->ttype);

	struct canditer lci, rci;
	oid lbase = sorted_l->hseqbase,
		rbase = sorted_r->hseqbase,
		lo, ro, last_lo = 0;
	const char *lvals = (const char *) li.base,
		*rvals = (const char *) ri.base,
		*lvars = li.vh->base,
		*rvars = ri.vh->base,
		*vl, *vr;
	BUN matches, newcap, n = 0, rx = 0, lx = 0;
	int rskipped = 0, vr_len = 0, cmp = 0;

	if (anti)
		STR_JOIN_NESTED_LOOP(str_cmp(vl, vr, vr_len) != 0, str_strlen(vr), fname);
	else
		STARTSWITH_SORTED_LOOP(str_cmp(vl, vr, vr_len), str_strlen(vr), fname);

	assert(!rr || BATcount(rl) == BATcount(rr));
	BATsetcount(rl, BATcount(rl));
	if (rr)
		BATsetcount(rr, BATcount(rr));

	if (BATcount(rl) > 0) {
		if (BATtdense(rl))
			rl->tseqbase = ((oid *) rl->theap->base)[0];
		if (rr && BATtdense(rr))
			rr->tseqbase = ((oid *) rr->theap->base)[0];
	} else {
		rl->tseqbase = 0;
		if (rr)
			rr->tseqbase = 0;
	}

	if (l_sorted == FALSE) {
		proj_rl = BATproject(rl, ord_sorted_l);
		if (!proj_rl) {
			msg = createException(MAL, fname, "Project left pre-sort order failed");
			goto exit;
		} else {
			BBPreclaim(rl);
			*rl_ptr = proj_rl;
		}
	}

	if (rr && r_sorted == FALSE) {
		proj_rr = BATproject(rr, ord_sorted_r);
		if (!proj_rr) {
			BBPreclaim(proj_rl);
			msg = createException(MAL, fname, "Project right pre-sort order failed");
			goto exit;
		} else {
			BBPreclaim(rr);
			*rr_ptr = proj_rr;
		}
	}

	TRC_DEBUG(ALGO,
			  "(%s, l=%s,r=%s)=(%s#" BUNFMT "%s%s,%s#" BUNFMT "%s%s\n",
			  fname,
			  BATgetId(l), BATgetId(r), BATgetId(rl), BATcount(rl),
			  rl->tsorted ? "-sorted" : "",
			  rl->trevsorted ? "-revsorted" : "",
			  rr ? BATgetId(rr) : NULL, rr ? BATcount(rr) : 0,
			  rr && rr->tsorted ? "-sorted" : "",
			  rr && rr->trevsorted ? "-revsorted" : "");

exit:
	if (l_sorted == FALSE)
		BBPnreclaim(3, sorted_l, ord_sorted_l, sorted_cl);

	if (r_sorted == FALSE)
		BBPnreclaim(3, sorted_r, ord_sorted_r, sorted_cr);

	bat_iterator_end(&li);
	bat_iterator_end(&ri);
	return msg;
}

static str
STRjoin(bat *rl_id, bat *rr_id, const bat l_id, const bat r_id,
		const bat cl_id, const bat cr_id, const bit anti, bool icase,
		int (*str_cmp)(const char *, const char *, int), const char *fname)
{
	str msg = MAL_SUCCEED;

	BAT *rl = NULL, *rr = NULL, *l = NULL, *r = NULL, *cl = NULL, *cr = NULL;

	if (!(l = BATdescriptor(l_id)) || !(r = BATdescriptor(r_id))) {
		BBPnreclaim(2, l, r);
		throw(MAL, fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if ((cl_id && !is_bat_nil(cl_id) && (cl = BATdescriptor(cl_id)) == NULL) ||
		(cr_id && !is_bat_nil(cr_id) && (cr = BATdescriptor(cr_id)) == NULL)) {
		BBPnreclaim(4, l, r, cl, cr);
		throw(MAL, fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	rl = COLnew(0, TYPE_oid, BATcount(l), TRANSIENT);
	if (rr_id)
		rr = COLnew(0, TYPE_oid, BATcount(l), TRANSIENT);

	if (!rl || (rr_id && !rr)) {
		BBPnreclaim(6, l, r, cl, cr, rl, rr);
		throw(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	set_empty_bat_props(rl);
	if (rr_id)
		set_empty_bat_props(rr);

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(ATOMtype(l->ttype) == TYPE_str);

	BAT *nl = l, *nr = r;

	if (strcmp(fname, "str.containsjoin") == 0) {
		msg = contains_join(rl, rr, l, r, cl, cr, anti, str_cmp, fname);
		if (msg) {
			BBPnreclaim(6, rl, rr, l, r, cl, cr);
			return msg;
		}
	} else {
		struct canditer lci, rci;
		canditer_init(&lci, l, cl);
		canditer_init(&rci, r, cr);
		BUN lcnt = lci.ncand, rcnt = rci.ncand;
		BUN nl_cost = lci.ncand * rci.ncand,
			sorted_cost =
			(BUN) floor(0.8 * (lcnt*log2((double)lcnt)
							   + rcnt*log2((double)rcnt)));

		if (nl_cost < sorted_cost) {
			msg = str_join_nested(rl, rr, nl, nr, cl, cr, anti, str_cmp, fname);
		} else {
			BAT *l_low = NULL, *r_low = NULL, *l_rev = NULL, *r_rev = NULL;
			if (icase) {
				l_low = batstr_strlower(nl);
				if (l_low == NULL) {
					BBPnreclaim(6, rl, rr, nl, nr, cl, cr);
					throw(MAL, fname, "Failed lowering strings of left input");
				}
				r_low = batstr_strlower(nr);
				if (r_low == NULL) {
					BBPnreclaim(7, rl, rr, nl, nr, cl, cr, l_low);
					throw(MAL, fname, "Failed lowering strings of right input");
				}
				BBPnreclaim(2, nl, nr);
				nl = l_low;
				nr = r_low;
			}
			if (strcmp(fname, "str.endswithjoin") == 0) {
				l_rev = batstr_strrev(nl);
				if (l_rev == NULL) {
					BBPnreclaim(6, rl, rr, nl, nr, cl, cr);
					throw(MAL, fname, "Failed reversing strings of left input");
				}
				r_rev = batstr_strrev(nr);
				if (r_rev == NULL) {
					BBPnreclaim(7, rl, rr, nl, nr, cl, cr, l_rev);
					throw(MAL, fname, "Failed reversing strings of right input");
				}
				BBPnreclaim(2, nl, nr);
				nl = l_rev;
				nr = r_rev;
			}
			msg = startswith_join(&rl, &rr, nl, nr, cl, cr, anti, str_is_prefix, fname);
		}
	}

	if (!msg) {
		*rl_id = rl->batCacheid;
		BBPkeepref(rl);
		if (rr_id) {
			*rr_id = rr->batCacheid;
			BBPkeepref(rr);
		}
	} else {
		BBPnreclaim(2, rl, rr);
	}

	BBPnreclaim(4, nl, nr, cl, cr);
	return msg;
}

#define STRJOIN_MAPARGS(STK, PCI, RL_ID, RR_ID, L_ID, R_ID, CL_ID, CR_ID, IC_ID, ANTI) \
	do {																\
		RL_ID = getArgReference(STK, PCI, 0);							\
		RR_ID = PCI->retc == 1 ? 0 : getArgReference(STK, PCI, 1);		\
		int i = PCI->retc == 1 ? 1 : 2;									\
		L_ID = getArgReference(STK, PCI, i++);							\
		R_ID = getArgReference(STK, PCI, i++);							\
		IC_ID = PCI->argc - PCI->retc == 7 ?							\
			NULL : getArgReference(stk, pci, i++);						\
		CL_ID = getArgReference(STK, PCI, i++);							\
		CR_ID = getArgReference(STK, PCI, i++);							\
		ANTI = PCI->argc - PCI->retc == 7 ?								\
			getArgReference(STK, PCI, 8) : getArgReference(STK, PCI, 9); \
	} while (0)

static inline str
ignorecase(const bat *ic_id, bool *icase, str fname)
{
	BAT *c = NULL;

	if ((c = BATdescriptor(*ic_id)) == NULL)
		throw(MAL, fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	BUN cnt = BATcount(c);
	if (cnt < 1) {
		BBPreclaim(c);
		throw(MAL, fname, SQLSTATE(42000) "Missing ignore case value\n");
	}

	BATiter bi = bat_iterator(c);
	*icase = *(bit *) BUNtloc(bi, 0);
	for(BUN i = 1; i<cnt; i++) {
		if (*icase != *(bit*)BUNtloc(bi, i)) {
			bat_iterator_end(&bi);
			BBPreclaim(c);
			throw(MAL, fname, SQLSTATE(42000) "Multiple ignore case values passed, only one expected\n");
		}
	}
	bat_iterator_end(&bi);
	BBPreclaim(c);
	return MAL_SUCCEED;
}

/**
 * @rl_id: result left oid
 * @rr_id: result right oid
 * @l_id: left oid
 * @r_id: right oid
 * @cl_id: candidates left oid
 * @cr_id: candidates right oid
 * @ic_id: ignore case oid
 * @anti: anti join oid
 */
static str
STRstartswithjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;

	str msg = MAL_SUCCEED;
	bat *rl_id = NULL, *rr_id = NULL, *l_id = NULL, *r_id = NULL,
		*cl_id = NULL, *cr_id = NULL, *ic_id = NULL;
	bit *anti = NULL;
	bool icase = false;

	STRJOIN_MAPARGS(stk, pci, rl_id, rr_id, l_id, r_id, cl_id, cr_id, ic_id, anti);

	if (pci->argc - pci->retc == 8)
		msg = ignorecase(ic_id, &icase, "str.startswithjoin");

	return msg ? msg : STRjoin(rl_id, rr_id, *l_id, *r_id,
							   cl_id ? *cl_id : 0,
							   cr_id ? *cr_id : 0,
							   *anti, icase, icase ? str_is_iprefix : str_is_prefix,
							   "str.startswithjoin");
}

/**
 * @rl_id: result left oid
 * @rr_id: result right oid
 * @l_id: left oid
 * @r_id: right oid
 * @cl_id: candidates left oid
 * @cr_id: candidates right oid
 * @ic_id: ignore case oid
 * @anti: anti join oid
 */
static str
STRendswithjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	str msg = MAL_SUCCEED;
	bat *rl_id = NULL, *rr_id = NULL, *l_id = NULL, *r_id = NULL,
		*cl_id = NULL, *cr_id = NULL, *ic_id = NULL;
	bit *anti = NULL;
	bool icase = false;

	STRJOIN_MAPARGS(stk, pci, rl_id, rr_id, l_id, r_id, cl_id, cr_id, ic_id, anti);

	if (pci->argc - pci->retc == 8)
		msg = ignorecase(ic_id, &icase, "str.endswithjoin");

	return msg ? msg : STRjoin(rl_id, rr_id, *l_id, *r_id,
							   cl_id ? *cl_id : 0, cr_id ? *cr_id : 0,
							   *anti, icase, icase ? str_is_isuffix : str_is_suffix,
							   "str.endswithjoin");
}

/**
 * @rl_id: result left oid
 * @rr_id: result right oid
 * @l_id: left oid
 * @r_id: right oid
 * @cl_id: candidates left oid
 * @cr_id: candidates right oid
 * @ic_id: ignore case oid
 * @anti: anti join oid
 */
static str
STRcontainsjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	str msg = MAL_SUCCEED;
	bat *rl_id = NULL, *rr_id = NULL, *l_id = NULL, *r_id = NULL,
		*cl_id = NULL, *cr_id = NULL, *ic_id = NULL;
	bit *anti = NULL;
	bool icase = false;

	STRJOIN_MAPARGS(stk, pci, rl_id, rr_id, l_id, r_id, cl_id, cr_id, ic_id, anti);

	if (pci->argc - pci->retc == 8)
		msg = ignorecase(ic_id, &icase, "str.containsjoin");

	return msg ? msg : STRjoin(rl_id, rr_id, *l_id, *r_id,
							   cl_id ? *cl_id : 0, cr_id ? *cr_id : 0,
							   *anti, icase, icase ? str_icontains : str_contains,
							   "str.containsjoin");
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
