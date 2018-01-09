/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_QC_H_
#define _SQL_QC_H_

#include "sql_mem.h"
#include "sql_list.h"
#include "sql_symbol.h"
#include "sql_backend.h"

#define DEFAULT_CACHESIZE 100
typedef struct cq {
	struct cq *next;	/* link them into a queue */
	int type;		/* sql_query_t: Q_PARSE,Q_SCHEMA,.. */
	sql_allocator *sa;	/* the symbols are allocated from this sa */
	sql_rel *rel;		/* relational query */
	symbol *s;		/* the SQL parse tree */
	sql_subtype *params;	/* parameter types */
	int paramlen;		/* number of parameters */
	backend_stack stk;	/* V4 state information */
	backend_code code;	/* V4 state information */
	int id;			/* cache identity */
	int key;		/* the hash key for the query text */
	char *codestring;	/* keep code in string form to aid debugging */
	char *name;		/* name of cache query */
	int no_mitosis;		/* run query without mitosis */
	int count;		/* number of times the query is matched */
} cq;

typedef struct qc {
	int clientid;
	int id;
	int nr;
	cq *q;
} qc;

extern qc *qc_create(int clientid, int seqnr);
extern void qc_destroy(qc *cache);
extern void qc_clean(qc *cache);
extern cq *qc_find(qc *cache, int id);
extern cq *qc_match(qc *cache, symbol *s, atom **params, int plen, int key);
extern cq *qc_insert(qc *cache, sql_allocator *sa, sql_rel *r, char *qname, symbol *s, atom **params, int paramlen, int key, int type, char *codedstr, int no_mitosis);
extern void qc_delete(qc *cache, cq *q);
extern int qc_size(qc *cache);
extern int qc_isaquerytemplate(char *nme);
extern int qc_isapreparedquerytemplate(char *nme);

#endif /*_SQL_QC_H_*/

