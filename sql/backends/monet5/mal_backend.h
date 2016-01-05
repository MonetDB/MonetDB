/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef MAL_BACKEND_H
#define MAL_BACKEND_H

#include <streams.h>
#include <mal_client.h>
#include <sql_mvc.h>
#include <sql_qc.h>

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

typedef struct backend {
	int 	console;
	char 	language;		/* 'S' or 's' or 'X' */
	mvc 	*mvc;
	stream 	*out;
	ofmt	output_format;	/* csv, json */
	Client 	client;
	int 	mvc_var;	
	int	vtop;		/* top of the variable stack before the current function */
	cq 	*q;		/* pointer to the cached query */
} backend;

extern backend *backend_reset(backend *b);
extern backend *backend_create(mvc *m, Client c);
extern void backend_destroy(backend *b);

#endif /*MAL_BACKEND_H*/
