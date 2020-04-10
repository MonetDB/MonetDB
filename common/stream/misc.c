/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"

#ifdef HAVE__WFOPEN
/* convert a string from UTF-8 to wide characters; the return value is
 * freshly allocated */
wchar_t *
utf8towchar(const char *src)
{
	wchar_t *dest;
	size_t i = 0;
	size_t j = 0;
	uint32_t c;

	/* count how many wchar_t's we need, while also checking for
	 * correctness of the input */
	while (src[j]) {
		i++;
		if ((src[j+0] & 0x80) == 0) {
			j += 1;
		} else if ((src[j+0] & 0xE0) == 0xC0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+0] & 0x1E) != 0) {
			j += 2;
		} else if ((src[j+0] & 0xF0) == 0xE0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && ((src[j+0] & 0x0F) != 0
			       || (src[j+1] & 0x20) != 0)) {
			j += 3;
		} else if ((src[j+0] & 0xF8) == 0xF0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && (src[j+3] & 0xC0) == 0x80) {
			c = (src[j+0] & 0x07) << 18
				| (src[j+1] & 0x3F) << 12
				| (src[j+2] & 0x3F) << 6
				| (src[j+3] & 0x3F);
			if (c < 0x10000
			    || c > 0x10FFFF
			    || (c & 0x1FF800) == 0x00D800)
				return NULL;
#if SIZEOF_WCHAR_T == 2
			i++;
#endif
			j += 4;
		} else {
			return NULL;
		}
	}
	dest = malloc((i + 1) * sizeof(wchar_t));
	if (dest == NULL)
		return NULL;
	/* go through the source string again, this time we can skip
	 * the correctness tests */
	i = j = 0;
	while (src[j]) {
		if ((src[j+0] & 0x80) == 0) {
			dest[i++] = src[j+0];
			j += 1;
		} else if ((src[j+0] & 0xE0) == 0xC0) {
			dest[i++] = (src[j+0] & 0x1F) << 6
				| (src[j+1] & 0x3F);
			j += 2;
		} else if ((src[j+0] & 0xF0) == 0xE0) {
			dest[i++] = (src[j+0] & 0x0F) << 12
				| (src[j+1] & 0x3F) << 6
				| (src[j+2] & 0x3F);
			j += 3;
		} else if ((src[j+0] & 0xF8) == 0xF0) {
			c = (src[j+0] & 0x07) << 18
				| (src[j+1] & 0x3F) << 12
				| (src[j+2] & 0x3F) << 6
				| (src[j+3] & 0x3F);
#if SIZEOF_WCHAR_T == 2
			dest[i++] = 0xD800 | ((c - 0x10000) >> 10);
			dest[i++] = 0xDE00 | (c & 0x3FF);
#else
			dest[i++] = c;
#endif
			j += 4;
		}
	}
	dest[i] = 0;
	return dest;
}
#else
char *
cvfilename(const char *filename)
{
#if defined(HAVE_NL_LANGINFO) && defined(HAVE_ICONV)
	char *code_set = nl_langinfo(CODESET);

	if (code_set != NULL && strcmp(code_set, "UTF-8") != 0) {
		iconv_t cd = iconv_open("UTF-8", code_set);

		if (cd != (iconv_t) -1) {
			size_t len = strlen(filename);
			size_t size = 4 * len;
			ICONV_CONST char *from = (ICONV_CONST char *) filename;
			char *r = malloc(size + 1);
			char *p = r;

			if (r) {
				if (iconv(cd, &from, &len, &p, &size) != (size_t) -1) {
					iconv_close(cd);
					*p = 0;
					return r;
				}
				free(r);
			}
			iconv_close(cd);
		}
	}
#endif
	/* couldn't use iconv for whatever reason; alternative is to
	 * use utf8towchar above to convert to a wide character string
	 * (wcs) and convert that to the locale-specific encoding
	 * using wcstombs or wcsrtombs (but preferably only if the
	 * locale's encoding is not UTF-8) */
	return strdup(filename);
}
#endif






const char *
get_extension(const char *file)
{
	char *ext_start;

	return (ext_start = strrchr(file, '.')) != NULL ? ext_start + 1 : "";
}
