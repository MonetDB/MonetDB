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

#ifndef _MSTRING_H_
#define _MSTRING_H_

#include <stdarg.h>		/* va_list etc. */
#include <string.h>		/* strlen */

#if defined(__GNUC__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ > 4))
/* not on CentOS 6 (GCC 4.4.7) */
#define GCC_Pragma(pragma)	_Pragma(pragma)
#else
#define GCC_Pragma(pragma)
#endif

#if defined(__has_attribute)
#if ! __has_attribute(__access__)
#define __access__(...)
#endif
#else
#define __access__(...)
#endif

/* copy at most (n-1) bytes from src to dst and add a terminating NULL
 * byte; return length of src (i.e. can be more than what is copied) */
__attribute__((__access__(write_only, 1, 3)))
static inline size_t
strcpy_len(char *restrict dst, const char *restrict src, size_t n)
{
	if (dst != NULL && n != 0) {
		for (size_t i = 0; i < n; i++) {
			if ((dst[i] = src[i]) == 0)
				return i;
		}
		dst[n - 1] = 0;
	}
	return strlen(src);
}

/* copy the NULL terminated list of src strings with a maximum of n
 * bytes to dst; return the combined length of the src strings */
__attribute__((__access__(write_only, 1, 2)))
__attribute__((__sentinel__))
static inline size_t
strconcat_len(char *restrict dst, size_t n, const char *restrict src, ...)
{
	va_list ap;
	size_t i = 0;

	va_start(ap, src);
	while (src) {
		size_t l;
		if (dst && i < n)
			l = strcpy_len(dst + i, src, n - i);
		else
			l = strlen(src);
		i += l;
		src = va_arg(ap, const char *);
	}
	va_end(ap);
	return i;
}

#ifdef __has_builtin
#if __has_builtin(__builtin_expect)
/* __builtin_expect returns its first argument; it is expected to be
 * equal to the second argument */
#define unlikely(expr)	__builtin_expect((expr) != 0, 0)
#define likely(expr)	__builtin_expect((expr) != 0, 1)
#endif
#endif
#ifndef unlikely
#ifdef _MSC_VER
#define unlikely(expr)	(__assume(!(expr)), (expr))
#define likely(expr)	(__assume((expr)), (expr))
#else
#define unlikely(expr)	(expr)
#define likely(expr)	(expr)
#endif
#endif

/*
 * UTF-8 encoding is as follows:
 * U-00000000 - U-0000007F: 0xxxxxxx
 * U-00000080 - U-000007FF: 110zzzzx 10xxxxxx
 * U-00000800 - U-0000FFFF: 1110zzzz 10zxxxxx 10xxxxxx
 * U-00010000 - U-0010FFFF: 11110zzz 10zzxxxx 10xxxxxx 10xxxxxx
 *
 * To be correctly coded UTF-8, the sequence should be the shortest
 * possible encoding of the value being encoded.  This means that at
 * least one of the z bits must be non-zero.  Also note that the four
 * byte sequence can encode more than is allowed and that the values
 * U+D800..U+DFFF are not allowed to be encoded.
 */
static inline bool
checkUTF8(const char *v)
{
	/* It is unlikely that this functions returns false, because it is
	 * likely that the string presented is a correctly coded UTF-8
	 * string.  So we annotate the tests that are very (un)likely to
	 * succeed, i.e. the ones that lead to a return of false.  This can
	 * help the compiler produce more efficient code. */
	if (v != NULL) {
		if (v[0] != '\200' || v[1] != '\0') {
			/* check that string is correctly encoded UTF-8 */
			for (size_t i = 0; v[i]; i++) {
				/* we do not annotate all tests, only the ones
				 * leading directly to an unlikely return
				 * statement */
				if ((v[i] & 0x80) == 0) {
					;
				} else if ((v[i] & 0xE0) == 0xC0) {
					if (unlikely(((v[i] & 0x1E) == 0)))
						return false;
					if (unlikely(((v[++i] & 0xC0) != 0x80)))
						return false;
				} else if ((v[i] & 0xF0) == 0xE0) {
					if ((v[i++] & 0x0F) == 0) {
						if (unlikely(((v[i] & 0xE0) != 0xA0)))
							return false;
					} else {
						if (unlikely(((v[i] & 0xC0) != 0x80)))
							return false;
					}
					if (unlikely(((v[++i] & 0xC0) != 0x80)))
						return false;
				} else if (likely(((v[i] & 0xF8) == 0xF0))) {
					if ((v[i++] & 0x07) == 0) {
						if (unlikely(((v[i] & 0x30) == 0)))
							return false;
					}
					if (unlikely(((v[i] & 0xC0) != 0x80)))
						return false;
					if (unlikely(((v[++i] & 0xC0) != 0x80)))
						return false;
					if (unlikely(((v[++i] & 0xC0) != 0x80)))
						return false;
				} else {
					return false;
				}
			}
		}
	}
	return true;
}

static inline int vreallocprintf(char **buf, size_t *pos, size_t *size, const char *fmt, va_list ap)
	__attribute__((__format__(__printf__, 4, 0)));

static inline int
vreallocprintf(char **buf, size_t *pos, size_t *capacity, const char *fmt, va_list args)
{
	va_list ap;

	assert(*pos <= *capacity);
	assert(*buf == NULL || *capacity > 0);

	size_t need_at_least = strlen(fmt);
	need_at_least += 1; // trailing NUL
	need_at_least += 80; // some space for the items
	while (1) {
		// Common cases:
		// 1. buf=NULL, pos=cap=0: allocate reasonable amount
		// 2. buf=NULL, pos=0, cap=something: start with allocating cap
		// 3. buf not NULL, cap=something: allocate larger cap
		if (*buf == NULL || need_at_least > *capacity - *pos) {
			size_t cap1 = *pos + need_at_least;
			size_t cap2 = *capacity;
			if (*buf)
				cap2 += cap2 / 2;
			size_t new_cap = cap1 > cap2 ? cap1 : cap2;
			char *new_buf = realloc(*buf, new_cap);
			if (new_buf == 0)
				return -1;
			*buf = new_buf;
			*capacity = new_cap;
		}
		assert(*buf);
		assert(need_at_least <= *capacity - *pos);
		char *output = &(*buf)[*pos];
		size_t avail = *capacity - *pos;
		assert(avail >= 1);

		va_copy(ap, args);
		int n = vsnprintf(output, avail, fmt, ap);
		va_end(ap);

		if (n < 0)
			return n;
		size_t needed = (size_t)n;
		if (needed <= avail - 1) {
			// it wanted to print n chars and it could
			*pos += needed;
			return n;
		}
		need_at_least = needed + 1;
	}
}

static inline int reallocprintf(char **buf, size_t *pos, size_t *size, const char *fmt, ...)
	__attribute__((__format__(__printf__, 4, 5)));

static inline int
reallocprintf(char **buf, size_t *pos, size_t *capacity, const char *fmt, ...)
{
	int n;
	va_list ap;
	va_start(ap, fmt);
	n = vreallocprintf(buf, pos, capacity, fmt, ap);
	va_end(ap);
	return n;
}

#undef unlikely
#undef likely

#endif
