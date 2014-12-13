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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
	int (*tostr)(void *extra, char **buf, int *len, int type, const void *a);
	void *(*frstr)(struct Column_t *fmt, int type, const char *s, const char *e, char quote);
	void *extra;
	void *data;
	int len;
	int nillen;
	bit ws;						/* if set we need to skip white space */
	char quote;					/* if set use this character for string quotes */
	const void *nildata;
	int size;
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
	BAT *complaints;			/* lines that did not match the required input */
	BAT *error_file;			/* copy file */
	BAT *error_row;				/* line number */
	BAT *error_fld;				/* field with error */
	BAT *error_msg;				/* reason */
	BAT *error_input;			/* original input */
} Tablet;

tablet_export BUN SQLload_file(Client cntxt, Tablet *as, bstream *b, stream *out, char *csep, char *rsep, char quote, lng skip, lng maxrow);
tablet_export int TABLETcreate_bats(Tablet *as, BUN est);
tablet_export BAT **TABLETcollect(Tablet *as);
tablet_export BAT **TABLETcollect_parts(Tablet *as, BUN offset);
tablet_export void TABLETdestroy_format(Tablet *as);
tablet_export int TABLEToutput_file(Tablet *as, BAT *order, stream *s);
tablet_export void *TABLETadt_frStr(Column *c, int type, char *s, char *e, char quote);
tablet_export int TABLETadt_toStr(void *extra, char **buf, int *len, int type, ptr a);

#endif
