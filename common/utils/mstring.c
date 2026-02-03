/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "mstring.h"
#include <stdarg.h>		/* va_list etc. */

/* copy the NULL terminated list of src strings with a maximum of n
 * bytes to dst; return the combined length of the src strings; dst is
 * guaranteed to be NULL-terminated (if n > 0) */
size_t
strlconcat(char *restrict dst, size_t n, const char *restrict src, ...)
{
	va_list ap;
	size_t i = 0;

	va_start(ap, src);
	while (src) {
		size_t l;
		if (i < n)
			l = strlcpy(dst + i, src, n - i);
		else
			l = strlen(src);
		i += l;
		src = va_arg(ap, const char *);
	}
	va_end(ap);
	return i;
}

/* copy the NULL terminated list of src strings with a maximum of n
 * bytes to dst; return -1 if the buffer was too small, else the
 * combined length of the src strings; dst is guaranteed to be
 * NULL-terminated (if n > 0) */
ssize_t
strtconcat(char *restrict dst, size_t n, const char *restrict src, ...)
{
	va_list ap;
	char *end = dst + n;

	if (n == 0) {
		errno = ENOBUFS;
		return -1;
	}
	va_start(ap, src);
	while (src) {
		dst = stpecpy(dst, end, src);
		if (dst == NULL) {
			va_end(ap);
			errno = E2BIG;
			return -1;
		}
		src = va_arg(ap, const char *);
	}
	va_end(ap);
	return dst - (end - n);
}
