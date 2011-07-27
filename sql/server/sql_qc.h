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

#ifndef _SQL_QC_H_
#define _SQL_QC_H_

#include <sql_mem.h>
#include <sql_list.h>
#include <sql_symbol.h>
#include <sql_backend.h>

typedef struct cq {
	struct cq *next;	/* link them into a queue */
	int type;		/* sql_query_t: Q_PARSE,Q_SCHEMA,.. */
	sql_allocator *sa;	/* the symbols are allocated from this sa */
	symbol *s;		/* the SQL parse tree */
	sql_subtype *params;	/* parameter types */
	int paramlen;		/* number of parameters */
	backend_stack stk;	/* V4 state information */
	backend_code code;	/* V4 state information */
	int id;			/* cache identity */
	int key;		/* the hash key for the query text */
	char *codestring;	/* keep code in string form to aid debugging */
	char *name;		/* name of cache query */
	int count;		/* number of times the query is matched */
} cq;

typedef struct qc {
	int clientid;
	int id;
	int nr;
	cq *q;
} qc;

extern qc *qc_create(int clientid);
extern void qc_destroy(qc *cache);
extern void qc_clean(qc *cache);
extern cq *qc_find(qc *cache, int id);
extern cq *qc_match(qc *cache, symbol *s, atom **params, int plen, int key);
extern cq *qc_insert(qc *cache, sql_allocator *sa, symbol *s, atom **params, int paramlen, int key, int type, char *codedstr);
extern void qc_delete(qc *cache, cq *q);
extern int qc_size(qc *cache);

#endif /*_SQL_QC_H_*/

