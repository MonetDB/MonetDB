/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

/* copy at most (n-1) bytes from src to dst and add a terminating NULL
 * byte; return length of src (i.e. can be more than what is copied) */
static inline size_t
strcpy_len(char *restrict dst, const char *restrict src, size_t n)
{
	if (dst != NULL && n != 0) {
		for (size_t i = 0; i < n; i++) {
			if ((dst[i] = src[i]) == 0)
				return i;
		}
		/* for correctness, the decrement isn't needed (just assigning 0
		 * to dst[n-1] would be sufficient), but to work around a too
		 * strict GNU C compiler, we do need it */
		dst[--n] = 0;
/* in some versions of GCC (at least gcc (Ubuntu 7.5.0-3ubuntu1~18.04)
 * 7.5.0), the error just can't be turned off when using
 * --enable-strict, so we just use the (more) expensive way of getting the
 * right answer (rescan the whole string) */
#if !defined(__GNUC__) || __GNUC__ > 7 || (__GNUC__ == 7 && __GNUC_MINOR__ > 5)
/* This code is correct, but GCC gives a warning in certain
 * conditions, so we disable the warning temporarily.
 * The warning happens e.g. in
 *   strcpy_len(buf, "fixed string", sizeof(buf))
 * where buf is larger than the string. In that case we never get here
 * since return is executed in the loop above, but the compiler
 * complains anyway about reading out-of-bounds.
 * For GCC we use _Pragma to disable the warning (and hence error).
 * Since other compilers may warn (and hence error out) on
 * unrecognized pragmas, we use some preprocessor trickery. */
GCC_Pragma("GCC diagnostic push")
GCC_Pragma("GCC diagnostic ignored \"-Warray-bounds\"")
		return n + strlen(src + n);
GCC_Pragma("GCC diagnostic pop")
#endif
	}
	return strlen(src);
}

/* copy the NULL terminated list of src strings with a maximum of n
 * bytes to dst; return the combined length of the src strings */
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

#ifndef __GNUC__
/* __builtin_expect returns its first argument; it is expected to be
 * equal to the second argument */
#define __builtin_expect(expr, expect)	(expr)
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
	/* It is unlikely that this functions returns false, because
	 * it is likely that the string presented is a correctly coded
	 * UTF-8 string.  So we annotate the tests that are very
	 * unlikely to succeed, i.e. the ones that lead to a return of
	 * false, as being expected to return 0 using the
	 * __builtin_expect function. */
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
					if (__builtin_expect(((v[i] & 0x1E) == 0), 0))
						return false;
					if (__builtin_expect(((v[++i] & 0xC0) != 0x80), 0))
						return false;
				} else if ((v[i] & 0xF0) == 0xE0) {
					if ((v[i++] & 0x0F) == 0) {
						if (__builtin_expect(((v[i] & 0xE0) != 0xA0), 0))
							return false;
					} else {
						if (__builtin_expect(((v[i] & 0xC0) != 0x80), 0))
							return false;
					}
					if (__builtin_expect(((v[++i] & 0xC0) != 0x80), 0))
						return false;
				} else if (__builtin_expect(((v[i] & 0xF8) == 0xF0), 1)) {
					if ((v[i++] & 0x07) == 0) {
						if (__builtin_expect(((v[i] & 0x30) == 0), 0))
							return false;
					}
					if (__builtin_expect(((v[i] & 0xC0) != 0x80), 0))
						return false;
					if (__builtin_expect(((v[++i] & 0xC0) != 0x80), 0))
						return false;
					if (__builtin_expect(((v[++i] & 0xC0) != 0x80), 0))
						return false;
				} else {
					return false;
				}
			}
		}
	}
	return true;
}

#ifndef __GNUC__
#undef __builtin_expect
#endif

#endif
