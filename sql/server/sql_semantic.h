/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _SQL_SEMANTIC_H_
#define _SQL_SEMANTIC_H_

#include <stdio.h>
#include <stdarg.h>
#include <sql_list.h>
#include "sql_symbol.h"
#include "sql_parser.h"

/* value vs predicate (boolean) */
#define type_value	0
#define type_predicate	1
/* cardinality expected by enclosing operator */
#define card_none	-1	/* psm call doesn't return anything */
#define card_value	0
#define card_row 	1
#define card_column 	2
#define card_set	3 /* some operators require only a set (IN/EXISTS) */
#define card_relation 	4
/* allowed to reduce (in the where and having parts we can reduce) */

typedef struct exp_kind_t {
	bte type;
	bte card;
	bit reduce;	
} exp_kind;

#define sql_from 	0
#define sql_where 	1
#define sql_sel   	2	
#define sql_having 	3
#define sql_orderby   	4	

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

extern void sql_add_param(mvc *sql, char *name, sql_subtype *st);	
extern sql_arg *sql_bind_param(mvc *sql, char *name);
/* once the type of the '?' parameters is known it's set using the set_type
 * function */
extern int set_type_param(mvc *sql, sql_subtype *type, int nr);
extern void sql_destroy_params(mvc *sql);	/* used in backend */

extern char *symbol2string(mvc *sql, symbol *s, char **err);
extern char *dlist2string(mvc *sql, dlist *s, char **err);

extern char * toUpperCopy(char *dest, const char *src); 

#endif /*_SQL_SEMANTIC_H_*/

