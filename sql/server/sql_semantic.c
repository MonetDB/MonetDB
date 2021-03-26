/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_parser.h"
#include "sql_symbol.h"
#include "sql_semantic.h"
#include "sql_env.h"
#include "sql_privileges.h"
#include "sql_string.h"
#include "sql_atom.h"

#include <unistd.h>
#include <string.h>
#include <ctype.h>

#include "rel_semantic.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"

/*
 * For debugging purposes we need to be able to convert sql-tokens to
 * a string representation.
 *
 * !SQL ERROR <sqlerrno> : <details>
 * !SQL DEBUG  <details>
 * !SQL WARNING <details>
 * !SQL  <informative message, reserved for ...rows affected>
 */

void
sql_add_param(mvc *sql, const char *name, sql_subtype *st)
{
	sql_arg *a = SA_ZNEW(sql->sa, sql_arg);

	if (name)
		a->name = sa_strdup(sql->sa, name);
	if (st && st->type)
		a->type = *st;
	a->inout = ARG_IN;
	if (name && strcmp(name, "*") == 0)
		a->type = *sql_bind_localtype("int");
	if (!sql->params)
		sql->params = sa_list(sql->sa);
	list_append(sql->params, a);
}

sql_arg *
sql_bind_param(mvc *sql, const char *name)
{
	node *n;

	if (sql->params) {
		for (n = sql->params->h; n; n = n->next) {
			sql_arg *a = n->data;

			if (a->name && strcmp(a->name, name) == 0)
				return a;
		}
	}
	return NULL;
}

static sql_arg *
sql_bind_paramnr(mvc *sql, int nr)
{
	int i=0;
	node *n;

	if (sql->params && nr < list_length(sql->params)) {
		for (n = sql->params->h, i=0; n && i<nr; n = n->next, i++)
			;

		if (n)
			return n->data;
	}
	return NULL;
}

void
sql_destroy_params(mvc *sql)
{
	sql->params = NULL;
}

sql_schema *
cur_schema(mvc *sql)
{
	return sql->session->schema;
}

sql_schema *
tmp_schema(mvc *sql)
{
	return mvc_bind_schema(sql, "tmp");
}

#define DO_NOTHING(x) ;

/* as we don't have OOP in C, I prefer a single macro with the search path algorithm to passing function pointers */
#define search_object_on_path(CALL, EXTRA_CONDITION, EXTRA, ERROR_CODE) \
	do { \
		sql_schema *next = NULL; \
 \
		assert(objstr); \
		if (sname) { /* user has explicitly typed the schema, so either the object is there or we return error */ \
			if (!(next = mvc_bind_schema(sql, sname))) \
				return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "%s: no such schema '%s'", error, sname); \
			EXTRA_CONDITION(EXTRA); /* for functions without schema, 'sys' is a valid schema to bind them */ \
			CALL; \
		} else { \
			sql_schema *cur = cur_schema(sql); \
			char *session_schema = cur->base.name; \
 \
			EXTRA; \
			if (!res && !sql->schema_path_has_tmp && strcmp(session_schema, "tmp") != 0) { /* if 'tmp' is not in the search path, search it before all others */ \
				next = mvc_bind_schema(sql, "tmp"); \
				CALL; \
			} \
			if (!res) { /* then current session's schema */ \
				next = cur; \
				CALL; \
			} \
			if (!res) { \
				/* object not found yet, look inside search path */ \
				for (node *n = sql->schema_path->h ; n && !res ; n = n->next) { \
					str p = (str) n->data; \
					if (strcmp(session_schema, p) != 0 && (next = mvc_bind_schema(sql, p))) \
						CALL; \
				} \
			} \
			if (!res && !sql->schema_path_has_sys && strcmp(session_schema, "sys") != 0) { /* if 'sys' is not in the current path search it next */ \
				next = mvc_bind_schema(sql, "sys"); \
				CALL; \
			} \
		} \
		if (!res) \
			return sql_error(sql, ERR_NOTFOUND, ERROR_CODE "%s: no such %s %s%s%s'%s'", error, objstr, sname ? "'":"", sname ? sname : "", sname ? "'.":"", name); \
	} while (0)

#define table_extra \
	do { \
		if (s) { \
			next = s; /* there's a default schema to search before all others, e.g. bind a child table from a merge table */ \
			res = mvc_bind_table(sql, next, name); \
		} \
		if (!res && strcmp(objstr, "table") == 0 && (res = stack_find_table(sql, name))) /* for tables, first try a declared table from the stack */ \
			return res; \
	} while (0)

sql_table *
find_table_or_view_on_scope(mvc *sql, sql_schema *s, const char *sname, const char *name, const char *error, bool isView)
{
	const char *objstr = isView ? "view" : "table";
	sql_table *res = NULL;

	search_object_on_path(res = mvc_bind_table(sql, next, name), DO_NOTHING, table_extra, SQLSTATE(42S02));
	return res;
}

sql_sequence *
find_sequence_on_scope(mvc *sql, const char *sname, const char *name, const char *error)
{
	const char *objstr = "sequence";
	sql_sequence *res = NULL;

	search_object_on_path(res = find_sql_sequence(sql->session->tr, next, name), DO_NOTHING, ;, SQLSTATE(42000));
	return res;
}

sql_idx *
find_idx_on_scope(mvc *sql, const char *sname, const char *name, const char *error)
{
	const char *objstr = "index";
	sql_idx *res = NULL;

	search_object_on_path(res = mvc_bind_idx(sql, next, name), DO_NOTHING, ;, SQLSTATE(42S12));
	return res;
}

sql_type *
find_type_on_scope(mvc *sql, const char *sname, const char *name, const char *error)
{
	const char *objstr = "type";
	sql_type *res = NULL;

	search_object_on_path(res = schema_bind_type(sql, next, name), DO_NOTHING, ;, SQLSTATE(42S01));
	return res;
}

sql_trigger *
find_trigger_on_scope(mvc *sql, const char *sname, const char *name, const char *error)
{
	const char *objstr = "trigger";
	sql_trigger *res = NULL;

	search_object_on_path(res = mvc_bind_trigger(sql, next, name), DO_NOTHING, ;, SQLSTATE(3F000));
	return res;
}

/* A variable can be any of the following, from the innermost to the outermost:
	- 'parameter of the function' (ie in the param list)
	- local variable, declared earlier
	- global variable, also declared earlier
*/
#define variable_extra \
	do { \
		if (!res) { \
			if ((*var = stack_find_var_frame(sql, name, level))) { /* check if variable is known from the stack */ \
				*tpe = &((*var)->var.tpe); \
				res = true; \
			} else if ((*a = sql_bind_param(sql, name))) { /* then if it is a parameter */ \
				*tpe = &((*a)->type); \
				*level = 1; \
				res = true; \
			} \
		} \
	} while (0)

#define var_find_on_global \
	do { \
		if ((*var = find_global_var(sql, next, name))) { /* then if it is a global var */ \
			*tpe = &((*var)->var.tpe); \
			*level = 0; \
			res = true; \
		} \
	} while (0)

bool
find_variable_on_scope(mvc *sql, const char *sname, const char *name, sql_var **var, sql_arg **a, sql_subtype **tpe, int *level, const char *error)
{
	const char *objstr = "variable";
	bool res = false;

	search_object_on_path(var_find_on_global, DO_NOTHING, variable_extra, SQLSTATE(42000));
	return res;
}

static sql_subfunc *
_dup_subaggr(sql_allocator *sa, sql_func *a, sql_subtype *member)
{
	node *tn;
	unsigned int scale = 0, digits = 0;
	sql_subfunc *ares = SA_ZNEW(sa, sql_subfunc);

	assert (a->res);

	ares->func = a;
	ares->res = sa_list(sa);
	for(tn = a->res->h; tn; tn = tn->next) {
		sql_arg *rarg = tn->data;
		sql_subtype *res, *r = &rarg->type;

		digits = r->digits;
		scale = r->scale;
		/* same scale as the input */
		if (member && (member->scale != scale ||
			(digits != member->digits && !EC_NUMBER(member->type->eclass)))) {
			if (member->digits > digits)
				digits = member->digits;
			scale = member->scale;
		}
		/* same type as the input */
		if (r->type->eclass == EC_ANY && member) {
			r = member;
			digits = member->digits;
		}
		if (!EC_SCALE(r->type->eclass))
			scale = 0;
		res = sql_create_subtype(sa, r->type, digits, scale);
		list_append(ares->res, res);
	}
	return ares;
}

static sql_subfunc *
func_cmp(sql_allocator *sa, sql_func *f, const char *name, int nrargs)
{
	if (strcmp(f->base.name, name) == 0) {
		if (f->vararg)
			return (f->type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, NULL, NULL);
		if (nrargs < 0 || list_length(f->ops) == nrargs)
			return (f->type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, NULL, NULL);
	}
	return NULL;
}

static sql_subfunc *
sql_find_func_internal(mvc *sql, list *ff, const char *fname, int nrargs, sql_ftype type, sql_subfunc *prev)
{
	int key = hash_key(fname);
	sql_subfunc *res = NULL;
	int found = 0;
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;

	if (ff) {
		MT_lock_set(&ff->ht_lock);
		if (ff->ht) {
			sql_hash_e *he = ff->ht->buckets[key&(ff->ht->size-1)];
			if (prev) {
				for (; he && !found; he = he->chain)
					if (he->value == prev->func)
						found = 1;
			}
			for (; he; he = he->chain) {
				sql_func *f = he->value;

				if (f->type != type && f->type != filt)
					continue;
				if ((res = func_cmp(sql->sa, f, fname, nrargs)) != NULL) {
					MT_lock_unset(&ff->ht_lock);
					return res;
				}
			}
			MT_lock_unset(&ff->ht_lock);
		} else {
			MT_lock_unset(&ff->ht_lock);
			node *n = ff->h;
			if (prev) {
				for (; n && !found; n = n->next)
					if (n->data == prev->func)
						found = 1;
			}
			for (; n; n = n->next) {
				sql_func *f = n->data;

				if (f->type != type && f->type != filt)
					continue;
				if ((res = func_cmp(sql->sa, f, fname, nrargs)) != NULL)
					return res;
			}
		}
	}
	return res;
}

static sql_subfunc *
os_find_func_internal(mvc *sql, struct objectset *ff, const char *fname, int nrargs, sql_ftype type, sql_subfunc *prev)
{
	sql_subfunc *res = NULL;
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;

	if (ff) {
		struct os_iter oi;
		os_iterator(&oi, ff, sql->session->tr, fname);
		for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_func *f = (sql_func*)b;
			if (prev && prev->func != f) {
				continue;
			} else if (prev) {
				prev = NULL;
				continue;
			}

			if (f->type != type && f->type != filt)
				continue;
			if ((res = func_cmp(sql->sa, f, fname, nrargs)) != NULL)
				return res;
		}
	}
	return res;
}

#define functions_without_schema(X) if (strcmp(sname, "sys") == 0) X

#define find_func_extra \
	do { \
		if (!res && (res = sql_find_func_internal(sql, funcs, name, nrargs, type, prev))) /* search system wide functions first */ \
			return res; \
	} while (0)

sql_subfunc *
sql_find_func(mvc *sql, const char *sname, const char *name, int nrargs, sql_ftype type, sql_subfunc *prev)
{
	char *F = NULL, *objstr = NULL;
	const char *error = "CATALOG";
	sql_subfunc *res = NULL;

	FUNC_TYPE_STR(type, F, objstr);
	(void) F; /* not used */

	assert(nrargs >= -1);

	search_object_on_path(res = os_find_func_internal(sql, next->funcs, name, nrargs, type, prev), functions_without_schema, find_func_extra, SQLSTATE(42000));
	return res;
}

static int
is_subtypeof(sql_subtype *sub, sql_subtype *super)
/* returns true if sub is a sub type of super */
{
	if (!sub || !super)
		return 0;
	if (super->digits > 0 && sub->digits > super->digits)
		return 0;
	if (super->digits == 0 && super->type->eclass == EC_STRING &&
	    (sub->type->eclass == EC_STRING || sub->type->eclass == EC_CHAR))
		return 1;
	if (super->type->eclass == sub->type->eclass)
		return 1;
	/* subtypes are only equal iff
	   they map onto the same systemtype */
	return (type_cmp(sub->type, super->type) == 0);
}

/* find function based on first argument */
static sql_subfunc *
sql_bind_member_internal(mvc *sql, list *ff, const char *fname, sql_subtype *tp, sql_ftype type, int nrargs, sql_subfunc *prev)
{
	int found = 1;

	assert(nrargs);
	if (ff) {
		node *n = ff->h;
		if (prev) {
			found = 0;
			for(; n && !found; n = n->next)
				if (n->data == prev->func)
					found = 1;
		}
		for (; n; n = n->next) {
			sql_func *f = n->data;

			if (!f->res && !IS_FILT(f))
				continue;
			if (strcmp(f->base.name, fname) == 0 && f->type == type && list_length(f->ops) == nrargs) {
				sql_subtype *ft = &((sql_arg *) f->ops->h->data)->type;
				if ((f->fix_scale == INOUT && type_cmp(tp->type, ft->type) == 0) || (f->fix_scale != INOUT && is_subtypeof(tp, ft)))
					return (type == F_AGGR) ? _dup_subaggr(sql->sa, f, NULL) : sql_dup_subfunc(sql->sa, f, NULL, tp);
			}
		}
	}
	return NULL;
}

static sql_subfunc *
os_bind_member_internal(mvc *sql, struct objectset *ff, const char *fname, sql_subtype *tp, sql_ftype type, int nrargs, sql_subfunc *prev)
{
	assert(nrargs);
	if (ff) {
		struct os_iter oi;
		os_iterator(&oi, ff, sql->session->tr, fname);
		for (sql_base *b = oi_next(&oi); b; b=oi_next(&oi)) {
			sql_func *f = (sql_func*)b;
			if (prev && prev->func != f)
				continue;
			else if (prev) {
				prev = NULL;
				continue;
			}

			if (!f->res && !IS_FILT(f))
				continue;
			if (strcmp(f->base.name, fname) == 0 && f->type == type && list_length(f->ops) == nrargs) {
				sql_subtype *ft = &((sql_arg *) f->ops->h->data)->type;
				if ((f->fix_scale == INOUT && type_cmp(tp->type, ft->type) == 0) || (f->fix_scale != INOUT && is_subtypeof(tp, ft)))
					return (type == F_AGGR) ? _dup_subaggr(sql->sa, f, NULL) : sql_dup_subfunc(sql->sa, f, NULL, tp);
			}
		}
	}
	return NULL;
}


#define sql_bind_member_extra \
	do { \
		if (!res && (res = sql_bind_member_internal(sql, funcs, name, tp, type, nrargs, prev))) /* search system wide functions first */ \
			return res; \
	} while (0)

sql_subfunc *
sql_bind_member(mvc *sql, const char *sname, const char *name, sql_subtype *tp, sql_ftype type, int nrargs, sql_subfunc *prev)
{
	char *F = NULL, *objstr = NULL;
	const char *error = "CATALOG";
	sql_subfunc *res = NULL;

	FUNC_TYPE_STR(type, F, objstr);
	(void) F; /* not used */

	search_object_on_path(res = os_bind_member_internal(sql, next->funcs, name, tp, type, nrargs, prev), functions_without_schema, sql_bind_member_extra, SQLSTATE(42000));
	return res;
}

sql_subfunc *
sql_bind_func(mvc *sql, const char *sname, const char *fname, sql_subtype *tp1, sql_subtype *tp2, sql_ftype type)
{
	list *l = sa_list(sql->sa);

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);
	return sql_bind_func_(sql, sname, fname, l, type);
}

sql_subfunc *
sql_bind_func3(mvc *sql, const char *sname, const char *fname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_ftype type)
{
	list *l = sa_list(sql->sa);

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);
	if (tp3)
		list_append(l, tp3);
	return sql_bind_func_(sql, sname, fname, l, type);
}

static sql_subfunc *
sql_bind_func__(mvc *sql, list *ff, const char *fname, list *ops, sql_ftype type)
{
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;
	sql_subtype *input_type = NULL;

	if (ops && ops->h)
		input_type = ops->h->data;

	if (ff) {
		node *n;
		sql_base_loop( ff, n) {
			sql_func *f = n->data;

			if (f->type != type && f->type != filt)
				continue;
			if (strcmp(f->base.name, fname) == 0 && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0)
				return (type == F_AGGR) ? _dup_subaggr(sql->sa, f, input_type) : sql_dup_subfunc(sql->sa, f, ops, NULL);
		}
	}
	return NULL;
}

static sql_subfunc *
os_bind_func__(mvc *sql, struct objectset *ff, const char *fname, list *ops, sql_ftype type)
{
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;
	sql_subtype *input_type = NULL;

	if (ops && ops->h)
		input_type = ops->h->data;

	if (ff) {
		struct os_iter oi;
		os_iterator(&oi, ff, sql->session->tr, fname);
		for (sql_base *b = oi_next(&oi); b; b=oi_next(&oi)) {
			sql_func *f = (sql_func*)b;

			if (f->type != type && f->type != filt)
				continue;
			if (strcmp(f->base.name, fname) == 0 && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0)
				return (type == F_AGGR) ? _dup_subaggr(sql->sa, f, input_type) : sql_dup_subfunc(sql->sa, f, ops, NULL);
		}
	}
	return NULL;
}

#define sql_bind_func__extra \
	do { \
		if (!res && (res = sql_bind_func__(sql, funcs, name, ops, type))) /* search system wide functions first */ \
			return res; \
	} while (0)

sql_subfunc *
sql_bind_func_(mvc *sql, const char *sname, const char *name, list *ops, sql_ftype type)
{
	char *F = NULL, *objstr = NULL;
	const char *error = "CATALOG";
	sql_subfunc *res = NULL;

	FUNC_TYPE_STR(type, F, objstr);
	(void) F; /* not used */

	search_object_on_path(res = os_bind_func__(sql, next->funcs, name, ops, type), functions_without_schema, sql_bind_func__extra, SQLSTATE(42000));
	return res;
}

static sql_subfunc *
sql_bind_func_result_internal(mvc *sql, list *ff, const char *fname, sql_ftype type, list *ops, sql_subtype *res)
{
	sql_subtype *tp = sql_bind_localtype("bit");

	if (ff) {
		node *n;
		sql_base_loop( ff, n) {
			sql_func *f = n->data;
			sql_arg *firstres = NULL;

			if (!f->res && !IS_FILT(f))
				continue;
			firstres = IS_FILT(f)?tp->type:f->res->h->data;
			if (strcmp(f->base.name, fname) == 0 && f->type == type && (is_subtype(&firstres->type, res) || firstres->type.type->eclass == EC_ANY) && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0)
				return (type == F_AGGR) ? _dup_subaggr(sql->sa, f, NULL) : sql_dup_subfunc(sql->sa, f, ops, NULL);
		}
	}
	return NULL;
}

static sql_subfunc *
os_bind_func_result_internal(mvc *sql, struct objectset *ff, const char *fname, sql_ftype type, list *ops, sql_subtype *res)
{
	sql_subtype *tp = sql_bind_localtype("bit");

	if (ff) {
		struct os_iter oi;
		os_iterator(&oi, ff, sql->session->tr, fname);
		for (sql_base *b = oi_next(&oi); b; b=oi_next(&oi)) {
			sql_func *f = (sql_func*)b;
			sql_arg *firstres = NULL;

			if (!f->res && !IS_FILT(f))
				continue;
			firstres = IS_FILT(f)?tp->type:f->res->h->data;
			if (strcmp(f->base.name, fname) == 0 && f->type == type && (is_subtype(&firstres->type, res) || firstres->type.type->eclass == EC_ANY) && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0)
				return (type == F_AGGR) ? _dup_subaggr(sql->sa, f, NULL) : sql_dup_subfunc(sql->sa, f, ops, NULL);
		}
	}
	return NULL;
}
#define sql_bind_func_result_extra \
	do { \
		if (!res && (res = sql_bind_func_result_internal(sql, funcs, name, type, ops, r_res))) /* search system wide functions first */ \
			return res; \
	} while (0)

sql_subfunc *
sql_bind_func_result(mvc *sql, const char *sname, const char *name, sql_ftype type, sql_subtype *r_res, int nargs, ...)
{
	char *F = NULL, *objstr = NULL;
	const char *error = "CATALOG";
	sql_subfunc *res = NULL;
	list *ops = sa_list(sql->sa);
	va_list valist;

	FUNC_TYPE_STR(type, F, objstr);
	(void) F; /* not used */

	va_start(valist, nargs);
	for (int i = 0; i < nargs; i++) {
		sql_type *tpe = va_arg(valist, sql_type*);
		list_append(ops, tpe);
	}
	va_end(valist);

	search_object_on_path(res = os_bind_func_result_internal(sql, next->funcs, name, type, ops, r_res), functions_without_schema, sql_bind_func_result_extra, SQLSTATE(42000));
	return res;
}

static int
arg_subtype_cmp_null(sql_arg *a, sql_subtype *t)
{
	if (a->type.type->eclass == EC_ANY)
		return 0;
	if (!t)
		return 0;
	return (is_subtypeof(t, &a->type )?0:-1);
}

static sql_subfunc *
sql_resolve_function_with_undefined_parameters_internal(mvc *sql, list *ff, const char *fname, list *ops, sql_ftype type)
{
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;

	if (ff) {
		node *n;
		sql_base_loop( ff, n) {
			sql_func *f = n->data;

			if (f->type != type && f->type != filt)
				continue;
			if (strcmp(f->base.name, fname) == 0) {
				if (list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp_null) == 0)
					return (type == F_AGGR) ? _dup_subaggr(sql->sa, f, NULL) : sql_dup_subfunc(sql->sa, f, ops, NULL);
			}
		}
	}
	return NULL;
}

static sql_subfunc *
os_resolve_function_with_undefined_parameters_internal(mvc *sql, struct objectset *ff, const char *fname, list *ops, sql_ftype type)
{
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;

	if (ff) {
		struct os_iter oi;
		os_iterator(&oi, ff, sql->session->tr, fname);
		for (sql_base *b = oi_next(&oi); b; b=oi_next(&oi)) {
			sql_func *f = (sql_func*)b;

			if (f->type != type && f->type != filt)
				continue;
			if (strcmp(f->base.name, fname) == 0) {
				if (list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp_null) == 0)
					return (type == F_AGGR) ? _dup_subaggr(sql->sa, f, NULL) : sql_dup_subfunc(sql->sa, f, ops, NULL);
			}
		}
	}
	return NULL;
}

#define sql_resolve_function_with_undefined_parameters_extra \
	do { \
		if (!res && (res = sql_resolve_function_with_undefined_parameters_internal(sql, funcs, name, ops, type))) /* search system wide functions first */ \
			return res; \
	} while (0)

sql_subfunc *
sql_resolve_function_with_undefined_parameters(mvc *sql, const char *sname, const char *name, list *ops, sql_ftype type)
{
	char *F = NULL, *objstr = NULL;
	const char *error = "CATALOG";
	sql_subfunc *res = NULL;

	FUNC_TYPE_STR(type, F, objstr);
	(void) F; /* not used */

	search_object_on_path(res = os_resolve_function_with_undefined_parameters_internal(sql, next->funcs, name, ops, type), functions_without_schema, sql_resolve_function_with_undefined_parameters_extra, SQLSTATE(42000));
	return res;
}

static list *
sql_find_funcs_internal(mvc *sql, list *ff, const char *fname, int nrargs, sql_ftype type)
{
	sql_subfunc *fres;
	int key = hash_key(fname);
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;
	list *res = NULL;

	if (ff) {
		MT_lock_set(&ff->ht_lock);
		if (ff->ht) {
			for (sql_hash_e *he = ff->ht->buckets[key&(ff->ht->size-1)]; he; he = he->chain) {
				sql_func *f = he->value;

				if (f->type != type && f->type != filt)
					continue;
				if ((fres = func_cmp(sql->sa, f, fname, nrargs )) != NULL) {
					if (!res)
						res = sa_list(sql->sa);
					list_append(res, fres);
				}
			}
		} else {
			node *n;
			sql_base_loop( ff, n) {
				sql_func *f = n->data;

				if (f->type != type && f->type != filt)
					continue;
				if ((fres = func_cmp(sql->sa, f, fname, nrargs )) != NULL) {
					if (!res)
						res = sa_list(sql->sa);
					list_append(res, fres);
				}
			}
		}
		MT_lock_unset(&ff->ht_lock);
	}
	return res;
}

static list *
os_find_funcs_internal(mvc *sql, struct objectset *ff, const char *fname, int nrargs, sql_ftype type)
{
	sql_subfunc *fres;
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;
	list *res = NULL;

	if (ff) {
		struct os_iter oi;
		os_iterator(&oi, ff, sql->session->tr, fname);
		for (sql_base *b = oi_next(&oi); b; b=oi_next(&oi)) {
			sql_func *f = (sql_func*)b;

			if (f->type != type && f->type != filt)
				continue;
			if ((fres = func_cmp(sql->sa, f, fname, nrargs )) != NULL) {
				if (!res)
					res = sa_list(sql->sa);
				list_append(res, fres);
			}
		}
	}
	return res;
}

#define sql_find_funcs_extra \
	do { \
		if (!res && (res = sql_find_funcs_internal(sql, funcs, name, nrargs, type))) /* search system wide functions first */ \
			return res; \
	} while (0)

list *
sql_find_funcs(mvc *sql, const char *sname, const char *name, int nrargs, sql_ftype type)
{
	char *F = NULL, *objstr = NULL;
	const char *error = "CATALOG";
	list *res = NULL;

	FUNC_TYPE_STR(type, F, objstr);
	(void) F; /* not used */

	search_object_on_path(res = os_find_funcs_internal(sql, next->funcs, name, nrargs, type), functions_without_schema, sql_find_funcs_extra, SQLSTATE(42000));
	return res;
}

static list *
sql_find_funcs_by_name_internal(mvc *sql, list *ff, const char *fname, sql_ftype type)
{
	int key = hash_key(fname);
	list *res = NULL;

	if (ff) {
		MT_lock_set(&ff->ht_lock);
		if (ff->ht) {
			for (sql_hash_e *he = ff->ht->buckets[key&(ff->ht->size-1)]; he; he = he->chain) {
				sql_func *f = he->value;

				if (f->type != type)
					continue;
				if (strcmp(f->base.name, fname) == 0) {
					if (!res)
						res = sa_list(sql->sa);
					list_append(res, f);
				}
			}
		} else {
			node *n;
			sql_base_loop( ff, n) {
				sql_func *f = n->data;

				if (f->type != type)
					continue;
				if (strcmp(f->base.name, fname) == 0) {
					if (!res)
						res = sa_list(sql->sa);
					list_append(res, f);
				}
			}
		}
		MT_lock_unset(&ff->ht_lock);
	}
	return res;
}

static list *
os_find_funcs_by_name_internal(mvc *sql, struct objectset *ff, const char *fname, sql_ftype type)
{
	list *res = NULL;

	if (ff) {
		struct os_iter oi;
		os_iterator(&oi, ff, sql->session->tr, fname);
		for (sql_base *b = oi_next(&oi); b; b=oi_next(&oi)) {
			sql_func *f = (sql_func*)b;

			if (f->type != type)
				continue;
			if (strcmp(f->base.name, fname) == 0) {
				if (!res)
					res = sa_list(sql->sa);
				list_append(res, f);
			}
		}
	}
	return res;
}

#define sql_find_funcs_by_name_extra \
	do { \
		if (!res && (res = sql_find_funcs_by_name_internal(sql, funcs, name, type))) /* search system wide functions first */ \
			return res; \
	} while (0)

list *
sql_find_funcs_by_name(mvc *sql, const char *sname, const char *name, sql_ftype type)
{
	char *F = NULL, *objstr = NULL;
	const char *error = "CATALOG";
	list *res = NULL;

	FUNC_TYPE_STR(type, F, objstr);
	(void) F; /* not used */

	search_object_on_path(res = os_find_funcs_by_name_internal(sql, next->funcs, name, type), functions_without_schema, sql_find_funcs_by_name_extra, SQLSTATE(42000));
	return res;
}

char *
qname_schema(dlist *qname)
{
	assert(qname && qname->h);

	if (dlist_length(qname) == 2) {
		return qname->h->data.sval;
	} else if (dlist_length(qname) == 3) {
		return qname->h->next->data.sval;
	}
	return NULL;
}

char *
qname_schema_object(dlist *qname)
{
	assert(qname && qname->h);

	if (dlist_length(qname) == 1) {
		return qname->h->data.sval;
	} else if (dlist_length(qname) == 2) {
		return qname->h->next->data.sval;
	} else if (dlist_length(qname) == 3) {
		return qname->h->next->next->data.sval;
	}
	return "unknown";
}

char *
qname_catalog(dlist *qname)
{
	assert(qname && qname->h);

	if (dlist_length(qname) == 3) {
		return qname->h->data.sval;
	}
	return NULL;
}

int
set_type_param(mvc *sql, sql_subtype *type, int nr)
{
	sql_arg *a = sql_bind_paramnr(sql, nr);

	if (!a)
		return -1;
	a->type = *type;
	return 0;
}

/*
 * Find the result_datatype for certain combinations of values
 * (like case expressions or coumns in a result of a query expression).
 * See standaard pages 505-507 Result of data type combinations */
sql_subtype *
result_datatype(sql_subtype *super, sql_subtype *l, sql_subtype *r)
{
	int lclass = l->type->eclass, rclass = r->type->eclass;
	int lc=0, rc=0;

	/* case a strings */
	if (EC_VARCHAR(lclass) || EC_VARCHAR(rclass)) {
		char *tpe = "varchar";
		unsigned int digits = 0;
		if (!EC_VARCHAR(lclass)) {
			tpe = r->type->sqlname;
			digits = (!l->digits)?0:r->digits;
		} else if (!EC_VARCHAR(rclass)) {
			tpe = l->type->sqlname;
			digits = (!r->digits)?0:l->digits;
		} else { /* both */
			tpe = !strcmp(l->type->base.name, "clob")?l->type->sqlname:!strcmp(r->type->base.name, "clob")?r->type->sqlname:
			(l->type->base.id > r->type->base.id)?l->type->sqlname:r->type->sqlname;
			digits = (!l->digits||!r->digits)?0:sql_max(l->digits, r->digits);
		}
		sql_find_subtype(super, tpe, digits, 0);
	/* case b blob */
	} else if ((lc=strcmp(l->type->sqlname, "blob")) == 0 || (rc=strcmp(r->type->sqlname, "blob")) == 0) {
		if (!lc)
			*super = *l;
		else
			*super = *r;
	/* case c all exact numeric */
	} else if (EC_EXACTNUM(lclass) && EC_EXACTNUM(rclass)) {
		char *tpe = (l->type->base.id > r->type->base.id)?l->type->sqlname:r->type->sqlname;
		unsigned int digits = sql_max(l->digits, r->digits);
		unsigned int scale = sql_max(l->scale, r->scale);

		if (l->type->radix == 10 && r->type->radix == 10) {
			digits = scale + (sql_max(l->digits - l->scale, r->digits - r->scale));
#ifdef HAVE_HGE
			if (digits > 39) {
				digits = 39;
#else
			if (digits > 19) {
				digits = 19;
#endif
				scale = MIN(scale, digits - 1);
			}
		} else if (l->type->radix == 2 && r->type->radix == 10) { /* change to radix 10 */
			digits = bits2digits(l->type->digits);
			digits = sql_max(r->digits, digits);
			scale = r->scale;
		} else if (l->type->radix == 10 && r->type->radix == 2) { /* change to radix 10 */
			digits = bits2digits(r->type->digits);
			digits = sql_max(l->digits, digits);
			scale = l->scale;
		}
		sql_find_subtype(super, tpe, digits, scale);
	/* case d approximate numeric */
	} else if (EC_APPNUM(lclass) || EC_APPNUM(rclass)) {
		if (!EC_APPNUM(lclass)) {
			*super = *r;
		} else if (!EC_APPNUM(rclass)) {
			*super = *l;
		} else { /* both */
			char *tpe = (l->type->base.id > r->type->base.id)?l->type->sqlname:r->type->sqlname;
			unsigned int digits = sql_max(l->digits, r->digits); /* bits precision */
			sql_find_subtype(super, tpe, digits, 0);
		}
	/* now its getting serious, ie e any 'case e' datetime data type */
	/* 'case f' interval types */
	/* 'case g' boolean */
	/* 'case h-l' compounds like row (tuple), etc */
	} else {
		return supertype(super, l, r);
	}
	return super;
}

sql_subtype *
supertype(sql_subtype *super, sql_subtype *r, sql_subtype *i)
{
	/* first find super type */
	char *tpe = r->type->sqlname;
	unsigned int radix = (unsigned int) r->type->radix;
	unsigned int digits = 0;
	unsigned int idigits = i->digits;
	unsigned int rdigits = r->digits;
	unsigned int scale = sql_max(i->scale, r->scale);
	sql_class eclass = r->type->eclass;
	sql_subtype lsuper;

	lsuper = *r;
	/* EC_STRING class is superior to EC_CHAR */
	if (EC_VARCHAR(i->type->eclass) && EC_VARCHAR(r->type->eclass)) {
		if (!strcmp(i->type->sqlname, "clob") || !strcmp(r->type->sqlname, "clob")) {
			lsuper = !strcmp(i->type->sqlname, "clob") ? *i : *r;
			radix = lsuper.type->radix;
			tpe = lsuper.type->sqlname;
			eclass = lsuper.type->eclass;
		} else {
			lsuper = i->type->base.id > r->type->base.id ? *i : *r;
			radix = lsuper.type->radix;
			tpe = lsuper.type->sqlname;
			eclass = lsuper.type->eclass;
		}
	} else if (i->type->base.id > r->type->base.id || (EC_VARCHAR(i->type->eclass) && !EC_VARCHAR(r->type->eclass))) {
		lsuper = *i;
		radix = i->type->radix;
		tpe = i->type->sqlname;
		eclass = i->type->eclass;
	}
	if (EC_VARCHAR(lsuper.type->eclass))
		scale = 0; /* strings don't have scale */
	if (!lsuper.type->localtype) {
		tpe = "smallint";
		eclass = EC_NUM;
	}
	/*
	 * In case of different radix we should change one.
	 */
	if (i->type->radix != r->type->radix) {
		if (radix == 10 || radix == 0 /* strings */) {
			/* change to radix 10 */
			if (i->type->radix == 2)
				idigits = bits2digits(idigits);
			if (r->type->radix == 2)
				rdigits = bits2digits(rdigits);
		} else if (radix == 2) { /* change to radix 2 */
			if (i->type->radix == 10)
				idigits = digits2bits(idigits);
			if (r->type->radix == 10)
				rdigits = digits2bits(rdigits);
		}
	}
	/* handle OID horror */
	if (i->type->radix == r->type->radix && i->type->base.id < r->type->base.id && strcmp(i->type->sqlname, "oid") == 0) {
		tpe = i->type->sqlname;
		eclass = EC_POS;
	}
	if (scale == 0 && (idigits == 0 || rdigits == 0)) {
		sql_find_subtype(&lsuper, tpe, 0, 0);
	} else {
		/* for strings use the max of both */
		if (EC_VARCHAR(eclass))
			digits = sql_max(type_digits_to_char_digits(i), type_digits_to_char_digits(r));
		else
			digits = sql_max(idigits - i->scale, rdigits - r->scale);
		sql_find_subtype(&lsuper, tpe, digits+scale, scale);
	}
	*super = lsuper;
	return super;
}

char *
toUpperCopy(char *dest, const char *src)
{
	size_t i, len;

	if (src == NULL) {
		*dest = '\0';
		return(dest);
	}

	len = _strlen(src);
	for (i = 0; i < len; i++)
		dest[i] = (char)toupper((int)src[i]);

	dest[i] = '\0';
	return(dest);
}

static char * _symbol2string(mvc *sql, symbol *se, int expression, char **err);

static char *
dlist2string(mvc *sql, dlist *l, int expression, char **err)
{
	char *b = NULL;
	dnode *n;

	for (n=l->h; n; n = n->next) {
		char *s = NULL;

		if (n->type == type_string && n->data.sval)
			s = sa_strdup(sql->ta, n->data.sval);
		else if (n->type == type_symbol)
			s = _symbol2string(sql, n->data.sym, expression, err);

		if (!s)
			return NULL;
		if (b) {
			char *o = SA_NEW_ARRAY(sql->ta, char, strlen(b) + strlen(s) + 2);
			if (o)
				stpcpy(stpcpy(stpcpy(o, b), "."), s);
			b = o;
			if (b == NULL)
				return NULL;
		} else {
			b = s;
		}
	}
	return b;
}

static const char *
symbol_escape_ident(sql_allocator *sa, const char *s)
{
	char *res = NULL;
	if (s) {
		size_t l = strlen(s);
		char *r = SA_NEW_ARRAY(sa, char, (l * 2) + 1);

		res = r;
		while (*s) {
			if (*s == '"')
				*r++ = '"';
			*r++ = *s++;
		}
		*r = '\0';
	}
	return res;
}

char *
_symbol2string(mvc *sql, symbol *se, int expression, char **err)
{
	/* inner symbol2string uses the temporary allocator */
	switch (se->token) {
	case SQL_NOP: {
		dnode *lst = se->data.lval->h, *ops = lst->next->next->data.lval->h, *aux;
		const char *op = symbol_escape_ident(sql->ta, qname_schema_object(lst->data.lval)),
				   *sname = symbol_escape_ident(sql->ta, qname_schema(lst->data.lval));
		int i = 0, nargs = 0;
		char** inputs = NULL, *res;
		size_t inputs_length = 0, extra = sname ? strlen(sname) + 3 : 0;

		for (aux = ops; aux; aux = aux->next)
			nargs++;
		if (!(inputs = SA_ZNEW_ARRAY(sql->ta, char*, nargs)))
			return NULL;

		for (aux = ops; aux; aux = aux->next) {
			if (!(inputs[i] = _symbol2string(sql, aux->data.sym, expression, err))) {
				return NULL;
			}
			inputs_length += strlen(inputs[i]);
			i++;
		}

		if ((res = SA_NEW_ARRAY(sql->ta, char, extra + strlen(op) + inputs_length + 3 + (nargs - 1 /* commas */) + 2))) {
			char *concat = res;
			if (sname)
				concat = stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".");
			concat = stpcpy(stpcpy(stpcpy(concat, "\""), op), "\"(");
			i = 0;
			for (aux = ops; aux; aux = aux->next) {
				concat = stpcpy(concat, inputs[i]);
				if (aux->next)
					concat = stpcpy(concat, ",");
				i++;
			}
			concat = stpcpy(concat, ")");
		}
		return res;
	}
	case SQL_BINOP: {
		dnode *lst = se->data.lval->h;
		const char *op = symbol_escape_ident(sql->ta, qname_schema_object(lst->data.lval)),
				   *sname = symbol_escape_ident(sql->ta, qname_schema(lst->data.lval));
		char *l = NULL, *r = NULL, *res;
		size_t extra = sname ? strlen(sname) + 3 : 0;

		if (!(l = _symbol2string(sql, lst->next->next->data.sym, expression, err)) || !(r = _symbol2string(sql, lst->next->next->next->data.sym, expression, err)))
			return NULL;

		if ((res = SA_NEW_ARRAY(sql->ta, char, extra + strlen(op) + strlen(l) + strlen(r) + 6))) {
			char *concat = res;
			if (sname)
				concat = stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".");
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(concat, "\""), op), "\"("), l), ","), r), ")");
		}
		return res;
	}
	case SQL_OP: {
		dnode *lst = se->data.lval->h;
		const char *op = symbol_escape_ident(sql->ta, qname_schema_object(lst->data.lval)),
				   *sname = symbol_escape_ident(sql->ta, qname_schema(lst->data.lval));
		char *res;
		size_t extra = sname ? strlen(sname) + 3 : 0;

		if ((res = SA_NEW_ARRAY(sql->ta, char, extra + strlen(op) + 5))) {
			char *concat = res;
			if (sname)
				concat = stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".");
			stpcpy(stpcpy(stpcpy(concat, "\""), op), "\"()");
		}
		return res;
	}
	case SQL_UNOP: {
		dnode *lst = se->data.lval->h;
		const char *op = symbol_escape_ident(sql->ta, qname_schema_object(lst->data.lval)),
				   *sname = symbol_escape_ident(sql->ta, qname_schema(lst->data.lval));
		char *l = _symbol2string(sql, lst->next->next->data.sym, expression, err), *res;
		size_t extra = sname ? strlen(sname) + 3 : 0;

		if (!l)
			return NULL;

		if ((res = SA_NEW_ARRAY(sql->ta, char, extra + strlen(op) + strlen(l) + 5))) {
			char *concat = res;
			if (sname)
				concat = stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".");
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(concat, "\""), op), "\"("), l), ")");
		}
		return res;
	}
	case SQL_PARAMETER:
		return sa_strdup(sql->ta, "?");
	case SQL_NULL:
		return sa_strdup(sql->ta, "NULL");
	case SQL_ATOM:{
		AtomNode *an = (AtomNode *) se;
		if (an && an->a)
			return atom2sql(sql->ta, an->a, sql->timezone);
		else
			return sa_strdup(sql->ta, "NULL");
	}
	case SQL_NEXT: {
		const char *seq = symbol_escape_ident(sql->ta, qname_schema_object(se->data.lval)),
				   *sname = qname_schema(se->data.lval);
		char *res;

		if (!sname)
			sname = sql->session->schema->base.name;
		sname = symbol_escape_ident(sql->ta, sname);

		if ((res = SA_NEW_ARRAY(sql->ta, char, strlen("next value for \"") + strlen(sname) + strlen(seq) + 5)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "next value for \""), sname), "\".\""), seq), "\"");
		return res;
	}	break;
	case SQL_IDENT:
	case SQL_COLUMN: {
		/* can only be variables */
		dlist *l = se->data.lval;
		assert(l->h->type != type_lng);
		if (expression && dlist_length(l) == 1 && l->h->type == type_string) {
			/* when compiling an expression, a column of a table might be present in the symbol, so we need this case */
			const char *op = symbol_escape_ident(sql->ta, l->h->data.sval);
			char *res;

			if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(op) + 3)))
				stpcpy(stpcpy(stpcpy(res, "\""), op), "\"");
			return res;
		} else if (expression && dlist_length(l) == 2 && l->h->type == type_string && l->h->next->type == type_string) {
			const char *first = symbol_escape_ident(sql->ta, l->h->data.sval),
					   *second = symbol_escape_ident(sql->ta, l->h->next->data.sval);
			char *res;

			if (!first || !second)
				return NULL;
			if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(first) + strlen(second) + 6)))
				stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "\""), first), "\".\""), second), "\"");
			return res;
		} else {
			char *e = dlist2string(sql, l, expression, err);
			if (e)
				*err = e;
			return NULL;
		}
	}
	case SQL_CAST: {
		dlist *dl = se->data.lval;
		char *val = NULL, *tpe = NULL, *res;

		if (!(val = _symbol2string(sql, dl->h->data.sym, expression, err)) || !(tpe = subtype2string2(sql->ta, &dl->h->next->data.typeval)))
			return NULL;
		if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(val) + strlen(tpe) + 11)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "cast("), val), " as "), tpe), ")");
		return res;
	}
	default: {
		const char *msg = "SQL feature not yet available for expressions and default values: ";
		char *tok_str = token2string(se->token);
		if ((*err = SA_NEW_ARRAY(sql->ta, char, strlen(msg) + strlen(tok_str) + 1)))
			stpcpy(stpcpy(*err, msg), tok_str);
	}
	}
	return NULL;
}

char *
symbol2string(mvc *sql, symbol *se, int expression, char **err)
{
	char *res = _symbol2string(sql, se, expression, err);

	if (res)
		res = sa_strdup(sql->sa, res);
	if (*err)
		*err = sa_strdup(sql->sa, *err);
	sa_reset(sql->ta);
	return res;
}
