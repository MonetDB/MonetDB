/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _SQL_SEMANTIC_H_
#define _SQL_SEMANTIC_H_

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

/* Search function for SQL objects with scoping rules. For tables an optional schema can be provided with search priority */
sql_export sql_table *find_table_or_view_on_scope(mvc *sql, sql_schema *s, const char *sname, const char *tname, const char *error, bool isView);
extern sql_sequence *find_sequence_on_scope(mvc *sql, const char *sname, const char *name, const char *error);
extern sql_idx *find_idx_on_scope(mvc *sql, const char *sname, const char *name, const char *error);
extern sql_type *find_type_on_scope(mvc *sql, const char *sname, const char *name, const char *error);
extern sql_trigger *find_trigger_on_scope(mvc *sql, const char *sname, const char *name, const char *error);
extern bool find_variable_on_scope(mvc *sql, const char *sname, const char *name, sql_var **var, sql_arg **a, sql_subtype **tpe, int *level, const char *error);

/* These functions find catalog functions according to scoping rules */
extern sql_subfunc *sql_find_func(mvc *sql, const char *sname, const char *fname, int nrargs, sql_ftype type, sql_subfunc *prev);
extern sql_subfunc *sql_bind_member(mvc *sql, const char *sname, const char *fname, sql_subtype *tp, sql_ftype type, int nrargs, sql_subfunc *prev);
extern sql_subfunc *sql_bind_func(mvc *sql, const char *sname, const char *fname, sql_subtype *tp1, sql_subtype *tp2, sql_ftype type);
extern sql_subfunc *sql_bind_func3(mvc *sql, const char *sname, const char *fname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_ftype type);
extern sql_subfunc *sql_bind_func_result(mvc *sql, const char *sname, const char *fname, sql_ftype type, sql_subtype *res, int nargs, ...);
extern sql_subfunc *sql_bind_func_(mvc *sql, const char *sname, const char *fname, list *ops, sql_ftype type);
extern sql_subfunc *sql_resolve_function_with_undefined_parameters(mvc *sql, const char *sname, const char *fname, list *ops, sql_ftype type);

extern list *sql_find_funcs(mvc *sql, const char *sname, const char *fname, int nrargs, sql_ftype type);
extern list *sql_find_funcs_by_name(mvc *sql, const char *sname, const char *name, sql_ftype type);

extern char *qname_schema(dlist *qname);
extern char *qname_schema_object(dlist *qname);
extern char *qname_catalog(dlist *qname);
#define qname_module(qname) qname_schema(qname)

extern sql_subtype *result_datatype(sql_subtype *super, sql_subtype *l, sql_subtype *r);
extern sql_subtype *supertype(sql_subtype *super, sql_subtype *r, sql_subtype *i);

typedef enum {
	type_set,	/* set operations have very limiting coersion rules */
	type_equal,
	type_equal_no_any,
	type_cast	/* also truncate */
} check_type;

/* SQL's parameters '?' (in prepare statements) and parameters of sql
 * functions and procedures are kept in the param list.  */

extern void sql_add_param(mvc *sql, const char *name, sql_subtype *st);
extern sql_arg *sql_bind_param(mvc *sql, const char *name);
/* once the type of the '?' parameters is known it's set using the set_type
 * function */
extern int set_type_param(mvc *sql, sql_subtype *type, int nr);
extern void sql_destroy_params(mvc *sql);	/* used in backend */

extern char *symbol2string(mvc *sql, symbol *s, int expression, char **err);
//extern char *dlist2string(mvc *sql, dlist *s, int expression, char **err);

extern char * toUpperCopy(char *dest, const char *src);

#endif /*_SQL_SEMANTIC_H_*/

