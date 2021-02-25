/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* This counts how many unicode codepoints the given string
 * contains.
 */
static size_t
GDKstrimp_strlen(const uint8_t *s)
{
	size_t ret = 0;
	size_t i;
	int m,n;
	uint8_t c;

	i = 0;
	while((c = *(s + i)) != 0) {
		if (c < 0x80)
			i++;
		else {
			for (n = 0, m=0x40; c & m; n++, m >>= 1)
				;
			/* n is now the number of 10xxxxxx bytes that should
			   follow. */
			if (n == 0 || n >= 4)
				/* TODO: handle invalid utf-8 */
				{}
			i += n+1;
		}
		ret++;
	}

	return ret;
}

/* Given a BAT return the number of digrams in it. The observation is
 * that the number of digrams is the number of characters - 1:
 *
 * 1 digram starting at character 1
 * 1 digram starting at character 2
 * [...]
 * 1 digram starting at character n - 1
 */
gdk_return
GDKstrimp_ndigrams(BAT *b, size_t *n)
{
	// lng t0;
	BUN i;
	BATiter bi;
	uint8_t *s;
	// GDKtracer_set_component_level("ALGO", "DEBUG");
	// struct canditer ci;

	// t0 = GDKusec();
	// BATcheck(b, NULL);
	assert(b->ttype == TYPE_str);

	bi = bat_iterator(b);
	*n = 0;
	for (i = 0; i < b->batCount; i++) {
		s = (uint8_t *)BUNtail(bi, i);
                *n += GDKstrimp_strlen(s) - 1;
		// TRC_DEBUG(ALGO, "s["LLFMT"]=%s\n", i, s);
	}

	// TRC_DEBUG(ALGO, LLFMT "usec\n", GDKusec() - t0);
	// GDKtracer_flush_buffer();

	return GDK_SUCCEED;
}

/* The isIgnored is a bit suspect in terms of unicode. There are
 * non-ASCII codepoints that are considered spaces, for example the
 * codepoints in the range U+2000-U+200f.
 */
#define isIgnored(x) (isspace((x)) || isdigit((x)) || ispunct((x)))
#define isNotIgnored(x) (!isIgnored(x))
#define pairToIndex(b1, b2) (((uint8_t)b1)<<8 | ((uint8_t)b2))

/* Construct a histogram of pairs of bytes.
 *
 * Return the histogram in hist and the number of non-zero bins in
 * count.
 */
gdk_return
GDKstrimp_makehistogram(BAT *b, uint64_t *hist, size_t hist_size, size_t *count)
{
	lng t0;
	size_t hi;
	BUN i;
	BATiter bi;
	char *ptr, *s;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	assert(b->ttype == TYPE_str);

	for(hi = 0; hi < hist_size; hi++)
		hist[hi] = 0;

	bi = bat_iterator(b);
	*count = 0;
	for(i = 0; i < b->batCount; i++) {
		s = (char *)BUNtvar(bi, i);
		if (!strNil(s)) {
			for(ptr = s; *ptr != 0 && *(ptr + 1) != 0; ptr++) {
				if (isNotIgnored(*ptr) && isNotIgnored(*(ptr+1))) {
					hi = pairToIndex(*(ptr), *(ptr+1));
					assert(hi < hist_size);
					if (hist[hi] == 0)
						(*count)++;
					hist[hi]++;
				}
			}
		}
	}

	TRC_DEBUG(ALGO, LLFMT "usec\n", GDKusec() - t0);
	GDKtracer_flush_buffer();
	return GDK_SUCCEED;
}
