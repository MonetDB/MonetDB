/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef MAL_BACKEND_H
#define MAL_BACKEND_H

#include "mal.h"
#include "mal_client.h"
#include "sql_mvc.h"
#include "sql_qc.h"

/*
 * The back-end structure collects the information needed to support
 * compilation and execution of the SQL code against the Monet Version 5
 * back end. Note that the back-end can be called upon by the front-end
 * to handle specific tasks, such as catalog management (sql_mvc)
 * and query execution (sql_qc). For this purpose, the front-end needs
 * access to operations defined in the back-end, in particular for
 * freeing the stack and code segment.
 */

typedef enum output_format {
	OFMT_CSV  = 	0,
	OFMT_JSON =	1,
	OFMT_NONE = 3
} ofmt;

/* The cur_append variable on an insert/update/delete on a partitioned table, tracks the current MAL variable holding
 * the total number of rows affected. The first_statement_generated looks if the first of the sub-statements was
 * generated or not */

typedef struct backend {
	char 	language;		/* 'S' or 's' or 'X' */
	char	depth;			/* depth >= 1 means no output for trans/schema statements */
	int 	remote;			/* counter to make remote function names unique */
	mvc 	*mvc;
	stream 	*out;
	ofmt	output_format;	/* csv, json */
	Client 	client;
	MalBlkPtr mb;		/* needed during mal generation */

	int 	mvc_var;	/* current variable holding the latest query context (used to create dependencies in mal statements) */
	int 	cur_append; /* The cur_append variable on an insert/update/delete on a partitioned table, tracks the current MAL variable holding
 				         * the total number of rows affected. */
	int		vtop;			/* top of the variable stack before the current function */
	int		vid;			/* old variable id top before the current function */
	int 	join_idx;	/* number of index joins (used in rel_bin) */
	lng 	reloptimizer;	/* timer for optimizer phase */

	bool sizeheader:1,	/* print size header in result set */
	     	no_mitosis:1,	/* run query without mitosis */
	     	first_statement_generated:1, /* The first_statement_generated looks if the first of the sub-statements was generated or not */
		console:1;
	cq 	*q;		/* pointer to the cached query */

	int result_id;
	res_table *results;
	lng last_id;
	lng rowcnt;
} backend;

extern backend *backend_reset(backend *b);
extern backend *backend_create(mvc *m, Client c);
extern void backend_destroy(backend *b);

#endif /*MAL_BACKEND_H*/
