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
GDKstrimp_strlen(const char *s)
{
	return strlen(s);
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
	char *s;
	// GDKtracer_set_component_level("ALGO", "DEBUG");
	// struct canditer ci;

	// t0 = GDKusec();
	// BATcheck(b, NULL);
	assert(b->ttype == TYPE_str);

	bi = bat_iterator(b);
	*n = 0;
	for (i = 0; i < b->batCount; i++) {
		s = (char *)BUNtail(bi, i);
                *n += GDKstrimp_strlen(s) - 1;
		// TRC_DEBUG(ALGO, "s["LLFMT"]=%s\n", i, (char *)BUNtail(bi, i));
	}

	// TRC_DEBUG(ALGO, LLFMT "usec\n", GDKusec() - t0);

	return GDK_SUCCEED;
}
