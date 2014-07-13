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
	OFMT_JSON =	1
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
