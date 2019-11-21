/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include <stdarg.h>		/* va_list etc. */
#include <string.h>		/* strlen */

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
		dst[n - 1] = 0;
		return n-1;
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
