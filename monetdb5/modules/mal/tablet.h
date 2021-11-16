/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

typedef struct Column_t {
	const char *name;			/* column title */
	const char *sep;
	const char *rsep;
	int seplen;
	char *type;
	int adt;					/* type index */
	BAT *c;						/* set to NULL when scalar is meant */
	BATiter ci;
	BUN p;
	unsigned int tabs;			/* field size in tab positions */
	const char *nullstr;		/* null representation */
	size_t null_length;			/* its length */
	unsigned int width;			/* actual column width */
	unsigned int maxwidth;		/* permissible width */
	int fieldstart;				/* Fixed character field load positions */
	int fieldwidth;
	int scale, precision;
	ssize_t (*tostr)(void *extra, char **buf, size_t *len, int type, const void *a);
	void *(*frstr)(struct Column_t *fmt, int type, const char *s);
	void *extra;
	void *data;
	int skip;					/* only skip to the next field */
	size_t len;
	bit ws;						/* if set we need to skip white space */
	char quote;					/* if set use this character for string quotes */
	const void *nildata;
	int size;
	void *appendcol;			/* temporary, can probably use Columnt_t.extra in the future */
} Column;

/*
 * All table printing is based on building a report structure first.
 * This table structure is private to a client, which made us to
 * keep it in an ADT.
 */

typedef struct Table_t {
	BUN offset;
	BUN nr;						/* allocated space for table loads */
	BUN nr_attrs;				/* attributes found sofar */
	Column *format;				/* remove later */
	str error;					/* last error */
	int tryall;					/* skip erroneous lines */
	str filename;				/* source */
	BAT *complaints;			/* lines that did not match the required input */
} Tablet;

// Callback interface to append the data directly instead of storing it in intermediate BATs.
// SQLload_file doesn't know how to manipulate the sql transaction bookkeeping, caller does.
typedef str (*loadfile_claim_fptr)(void *state, size_t nrows, size_t ncols, Column *cols[]);
typedef str (*loadfile_append_one_fptr)(void *state, size_t idx, const void *data, void *col);
typedef str (*loadfile_append_batch_fptr)(void *state, const void *data, BUN count, int width, void *col);
typedef BAT *(*loadfile_get_offsets_bat_fptr)(void *state);
typedef struct LoadOps {
	void *state;
	loadfile_claim_fptr claim;
	loadfile_append_one_fptr append_one;
	loadfile_append_batch_fptr append_batch;
	loadfile_get_offsets_bat_fptr get_offsets;
} LoadOps;


mal_export BUN SQLload_file(Client cntxt, Tablet *as, bstream *b, stream *out, const char *csep, const char *rsep, char quote, lng skip, lng maxrow, int best, bool from_stdin, const char *tabnam, bool escape, LoadOps *loadops);
mal_export str TABLETcreate_bats(Tablet *as, BUN est);
mal_export str TABLETcollect(BAT **bats, Tablet *as);
mal_export str TABLETcollect_parts(BAT **bats, Tablet *as, BUN offset);
mal_export void TABLETdestroy_format(Tablet *as);
mal_export int TABLEToutput_file(Tablet *as, BAT *order, stream *s);
mal_export str COPYrejects(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str COPYrejects_clear(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif
