/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * (author) R Cijvat
 * This file contains some global definitions, used by multiple bam
 * library files
 */




#include "monetdb_config.h"

#include "bam_globals.h"

stream *
bsopen(str filepath)
{
	stream *s;
	stream *wbs;

	if ((s = open_wastream(filepath)) == NULL) {
		return NULL;
	}
	if (mnstr_errnr(s)) {
		close_stream(s);
		return NULL;
	}
	if ((wbs = wbstream(s, BSTREAM_CHUNK_SIZE)) == NULL) {
		close_stream(s);
		return NULL;
	}
	return wbs;
}
