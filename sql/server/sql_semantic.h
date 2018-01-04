/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_SEMANTIC_H_
#define _SQL_SEMANTIC_H_

#include <stdio.h>
#include <stdarg.h>
#include "sql_list.h"
#include "sql_symbol.h"
#include "sql_parser.h"

typedef struct exp_kind_t {
	bte type;
	bte card;
	bit reduce;	
} exp_kind;

extern sql_schema *cur_schema(mvc *sql);
extern sql_schema *tmp_schema(mvc *sql);
extern char *qname_schema(dlist *qname);
extern char *qname_table(dlist *qname);
extern char *qname_catalog(dlist *qname);
#define qname_module(qname) qname_schema(qname)
#define qname_fname(qname) qname_table(qname)

extern sql_subtype *supertype(sql_subtype *super, sql_subtype *r, sql_subtype *i);

typedef enum {
	type_set,	/* set operations have very limiting coersion rules */
	type_equal,
	type_equal_no_any,
	type_cast	/* also truncate */
} check_type;

/* literals in the parser are kept outside of the abstract syntax tree
   in the arg array, this to allow for more reuse of cached queries */

extern void sql_add_arg(mvc *sql, atom *v);
extern void sql_set_arg(mvc *sql, int nr, atom *v);
extern atom *sql_bind_arg(mvc *sql, int nr);
extern void sql_destroy_args(mvc *sql);		/* used in backend */

/* SQL's parameters '?' (in prepare statements) and parameters of sql
 * functions and procedures are kept in the param list.  */

extern void sql_add_param(mvc *sql, const char *name, sql_subtype *st);	
extern sql_arg *sql_bind_param(mvc *sql, const char *name);
/* once the type of the '?' parameters is known it's set using the set_type
 * function */
extern int set_type_param(mvc *sql, sql_subtype *type, int nr);
extern void sql_destroy_params(mvc *sql);	/* used in backend */

extern char *symbol2string(mvc *sql, symbol *s, char **err);
extern char *dlist2string(mvc *sql, dlist *s, char **err);

extern char * toUpperCopy(char *dest, const char *src); 

#endif /*_SQL_SEMANTIC_H_*/

