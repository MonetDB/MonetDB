/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/*
 * @+ Implementation
 * The implementation needs the stream abstraction, which also provides
 * primitives to compress/decompress files on the fly.
 * The file can plain ASCII, gzipped or bzipped, decided by the extention
 * (none, gz or bz2). The default is plain ASCII, which is formatted to
 * pre presented on the screen directly.
 */
#ifndef _TABLET_IO2_H_
#define _TABLET_IO2_H_

#include "gdk.h"
#include "mal_exception.h"
#include "mal_client.h"
#include "mal_interpreter.h"

typedef struct {
		const char *name;			/* column title */
	const char *sep;
	const char *rsep;
	int seplen;
	const char *type;
	int adt;					/* type index */
	BAT *c;						/* set to NULL when scalar is meant */
	BATiter ci;
	BUN p;
	const char *nullstr;		/* null representation */
	ssize_t (*tostr)(void *extra, char **buf, size_t *len, int type, const void *a);
	void *extra;
	char quote;					/* if set use this character for string quotes */
} OutputColumn;

/*
 * All table printing is based on building a report structure first.
 * This table structure is private to a client, which made us to
 * keep it in an ADT.
 */

typedef struct {
	BUN offset;
	BUN nr;						/* allocated space for table loads */
	BUN nr_attrs;				/* attributes found sofar */
	OutputColumn *format;				/* remove later */
	str error;					/* last error */
	int tryall;					/* skip erroneous lines */
	str filename;				/* source */
	BAT *complaints;			/* lines that did not match the required input */
} OutputTable;

mal_export void TABLETdestroy_outputformat(OutputTable *as);

mal_export int TABLEToutput_file(OutputTable *as, BAT *order, stream *s);

#endif
