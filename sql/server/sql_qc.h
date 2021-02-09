/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _SQL_QC_H_
#define _SQL_QC_H_

#include "sql_mem.h"
#include "sql_mvc.h"
#include "sql_list.h"
#include "sql_symbol.h"
#include "sql_backend.h"
#include "gdk_time.h"

#define DEFAULT_CACHESIZE 100
typedef struct cq {
	struct cq *next;	/* link them into a queue */
	mapi_query_t type;	/* sql_query_t: Q_PARSE,Q_SCHEMA,.. */
	sql_allocator *sa;	/* the symbols are allocated from this sa */
	sql_rel *rel;		/* relational query */
	symbol *s;			/* the SQL parse tree */
	int id;				/* cache identity */
	const char *name;	/* name of cached query */
	int no_mitosis;		/* run query without mitosis */
	int count;			/* number of times the query is matched */
	timestamp created;	/* when the query was created */
	sql_func *f;
} cq;

typedef struct qc {
	int clientid;
	int id;
	int nr;
	cq *q;
} qc;

extern qc *qc_create(sql_allocator *sa, int clientid, int seqnr);
extern void qc_clean(qc *cache);
extern void qc_destroy(qc *cache);
sql_export cq *qc_find(qc *cache, int id);
sql_export cq *qc_insert(qc *cache, sql_allocator *sa, sql_rel *r, symbol *s, list *params, mapi_query_t type, char *codedstr, int no_mitosis);
sql_export void qc_delete(qc *cache, cq *q);
extern int qc_size(qc *cache);

#endif /*_SQL_QC_H_*/
