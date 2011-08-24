/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @include prelude.mx
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

/* #define _DEBUG_TABLET_ */

#include <gdk.h>
#include "streams.h"
#include <mal_exception.h>
#include <mal_client.h>
#include <mal_interpreter.h>
#include <mapi.h>				/* for PROMPT1, PROMPT2 */

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define tablet_export extern __declspec(dllimport)
#else
#define tablet_export extern __declspec(dllexport)
#endif
#else
#define tablet_export extern
#endif

#define SIZE 1*1024*1024
#define SLICES 2
#define BINS 100

struct Column_t;
typedef ptr *(*frStr) (struct Column_t *fmt, int type, char *s, char *e, char quote);
/* as toString functions are also used outside tablet we don't pas the column here */
typedef int (*toStr) (void *extra, char **buf, int *len, int type, ptr a);

typedef struct Column_t {
	char *batname;
	char *name;					/* column title */
	char *sep;
	int seplen;
	char *type;
	int adt;					/* type index */
	BAT *c[SLICES];				/* set to NULL when scalar is meant */
	BATiter ci[SLICES];
	BAT *bin[BINS];
	BUN p;
	unsigned int tabs;			/* field size in tab positions */
	str lbrk, rbrk;				/* column brackets */
	str nullstr;				/* null representation */
	size_t null_length;			/* its length */
	unsigned int width;			/* actual column width */
	unsigned int maxwidth;		/* permissible width */
	int fieldstart;				/* Fixed character field load positions */
	int fieldwidth;
	int scale, precision;
	toStr tostr;
	frStr frstr;
	void *extra;
	void *data;
	int len;
	int nillen;
	bit ws;						/* if set we need to skip white space */
	bit quote;					/* if set use this character for string quotes */
	void *nildata;
	str batfile;				/* what is the BAT to be replaced */
	str rawfile;				/* where to find the raw file */
	stream *raw;				/* this column should be stored directly on stream */
	int size;
} Column;

/*
 * @-
 * All table printing is based on building a report structure first.
 * This table structure is private to a client, which made us to
 * keep it in an ADT.
 */

typedef struct Table_t {
	char *sep;					/* default separator */
	str ttopbrk, tbotbrk;		/* table brackets */
	str rlbrk, rrbrk;			/* row brackets */
	str properties;				/* of header to display */
	str title, footer;			/* alternatives */
	BUN offset;
	BUN nr;						/* allocated space for table loads */
	size_t pageLimit;
	size_t firstrow, lastrow;	/* last window to print */
	BUN nr_attrs;				/* attributes found sofar */
	size_t max_attrs;
	Column *format;				/* remove later */
	stream *fd;
	BAT *pivot;
	str error;					/* last error */
	int tryall;					/* skip erroneous lines */
	BAT *complaints;			/* lines that did not match the required input */
	unsigned int rowwidth;		/* sum of columns used for mallocs */
	bstream *input;				/* where to get the data from */
	stream *output;				/* where to leave immediate output */
	lng bytes;					/* required bytes to load (round up to end of record) */
	MT_Id tid;					/* Thread id for parallel loads only */
	int partid;					/* partition number */
	Column columns[1];			/* at least one column, enlarged upon need */
} Tablet;

tablet_export BAT *TABLETload(Tablet *as, char *datafile);
tablet_export BUN TABLEToutput(BAT *order, BAT *seps, BAT *bats, stream *s);
tablet_export void TABLETdump(BAT *names, BAT *seps, BAT *bats, char *datafile, BUN nr);

/* The low level routines are primarilly used by the SQL front-end.*/
tablet_export int TABLETcreate_bats(Tablet *as, BUN est);
tablet_export BUN TABLETassign_BATs(Tablet *as, BAT *bats);
tablet_export BUN TABLETload_file(Tablet *as, bstream *b, stream *out);
tablet_export BUN SQLload_file(Client cntxt, Tablet *as, bstream *b, stream *out, char *csep, char *rsep, char quote, lng skip, lng maxrow);
tablet_export BAT **TABLETcollect(Tablet *as);
tablet_export BAT *TABLETcollect_bats(Tablet *as);
tablet_export BAT **TABLETcollect_parts(Tablet *as, BUN offset);
tablet_export void TABLETdestroy_format(Tablet *as);
tablet_export int TABLEToutput_file(Tablet *as, BAT *order, stream *s);

tablet_export ptr *TABLETstrFrStr(Column *c, char *s, char *e);
tablet_export ptr *TABLETadt_frStr(Column *c, int type, char *s, char *e, char quote);
tablet_export int TABLETadt_toStr(void *extra, char **buf, int *len, int type, ptr a);
tablet_export int insert_line(Tablet *as, char *line, ptr key, BUN col1, BUN col2);
tablet_export int output_file_dense(Tablet *as, stream *fd);
tablet_export int has_whitespace(char *sep);

#ifdef LIBMAL
/* not exported since only used within library */
extern int tablet_read_more(bstream *in, stream *out, size_t n);
extern char *tablet_skip_string(char *s, char quote);
#endif

#endif
