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
	chr type;
	chr card;
	bit reduce;	
} exp_kind;

#define sql_from 0	/* nothing */
#define sql_where 1	/* relate to any column from the from */
#define sql_having 2	/* special flag which is used within the having fase */
#define sql_sel   3	


#define create_string_list() list_create((fdestroy)&GDKfree)

extern stmt *semantic(mvc *sql, symbol *sym);
extern stmt *output_semantic(mvc *sql, symbol *s);

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

extern int convert_types(mvc *sql, stmt **L, stmt **R, int scale_fixing, check_type tpe);

extern stmt *check_types(mvc *sql, sql_subtype *ct, stmt *s, check_type tpe);
extern stmt *fix_scale(mvc *sql, sql_subtype *ct, stmt *s, int both, int always);
extern stmt *sum_scales(mvc *sql, sql_subfunc *f, stmt *ls, stmt *rs);
extern stmt *scale_algebra(mvc *sql, sql_subfunc *f, stmt *ls, stmt *rs);
extern stmt *sql_parse(mvc *m, sql_allocator *sql, char *query, char mode);

/* literals in the parser are kept outside of the abstract syntax tree
   in the arg array, this to allow for more reuse of cached queries */

extern void sql_add_arg(mvc *sql, atom *v);
extern atom *sql_bind_arg(mvc *sql, int nr);
extern void sql_destroy_args(mvc *sql);		/* used in backend */

/* SQL's parameters '?' (in prepare statements) and parameters of sql
 * functions and procedures are kept in the param list.  */

extern void sql_add_param(mvc *sql, char *name, sql_subtype *st);	
extern sql_arg *sql_bind_param(mvc *sql, char *name);
/* once the type of the '?' parameters is known its set using the set_type
 * function */
extern int set_type_param(mvc *sql, sql_subtype *type, int nr);
extern int stmt_set_type_param(mvc *sql, sql_subtype *type, stmt *param);
extern void sql_destroy_params(mvc *sql);	/* used in backend */

extern char *symbol2string(mvc *sql, symbol *s, char **err);
extern char *dlist2string(mvc *sql, dlist *s, char **err);

extern char * toUpperCopy(char *dest, const char *src); 

#endif /*_SQL_SEMANTIC_H_*/

