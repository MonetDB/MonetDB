/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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

sql_export sql_schema *cur_schema(mvc *sql);
extern sql_schema *tmp_schema(mvc *sql);

/* as we don't have OOP in C, I prefer a single macro with the search path algorithm, than passing function pointers */
#define search_object_on_path(CALL, EXTRA, ERROR_CODE) \
	do { \
		sql_schema *found = NULL; \
 \
		assert(objstr); \
		if (sname) { /* user has explicitly typed the schema, so either the object is there or we return error */ \
			if (!(found = mvc_bind_schema(sql, sname))) \
				return sql_error(sql, 02, SQLSTATE(3F000) "%s: no such schema '%s'", error, sname); \
			CALL; \
		} else { \
			char *p, *sp, *search_path_copy; \
 \
			if (*s) { \
				found = *s; /* there's a default schema to search before all others, e.g. bind a child table from a merge table */ \
				CALL; \
			} \
			EXTRA; \
			if (!res && !sql->search_path_has_tmp) { /* if 'tmp' is not in the search path, search it before all others */ \
				found = mvc_bind_schema(sql, "tmp"); \
				CALL; \
			} \
			if (!res) { /* then current session's schema */ \
				found = cur_schema(sql); \
				CALL; \
			} \
			if (!res && !sql->search_path_has_sys) { /* if 'sys' is not in the current path search it next */ \
				found = mvc_bind_schema(sql, "sys"); \
				CALL; \
			} \
			if (!res) { \
				/* object not found yet, look inside search path */ \
				search_path_copy = sa_strdup(sql->ta, sql->search_path); \
				p = strtok_r(search_path_copy, ",", &sp); \
				while (p && !res) { \
					if ((found = mvc_bind_schema(sql, p))) \
						CALL; \
					p = strtok_r(NULL, ",", &sp); \
				} \
			} \
		} \
		if (!res) \
			return sql_error(sql, 02, ERROR_CODE "%s: no such %s %s%s%s'%s'", error, objstr, sname ? "'":"", sname ? sname : "", sname ? "'.":"", name); \
		*s = found; \
	} while (0)

extern sql_table *find_table_or_view_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *tname, const char *error, bool isView);
extern sql_sequence *find_sequence_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *name, const char *error);
extern sql_idx *find_idx_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *name, const char *error);
extern sql_type *find_type_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *name, const char *error);
extern sql_trigger *find_trigger_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *name, const char *error);
extern bool find_variable_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *name, sql_var **var, sql_arg **a, sql_subtype **tpe, int *level, const char *error);

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

