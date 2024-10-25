/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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

int
sql_bind_param(mvc *sql, const char *name)
{
	node *n;
	int nr = 0;

	if (sql->params) {
		for (n = sql->params->h; n; n = n->next, nr++) {
			sql_arg *a = n->data;

			if (a->name && strcmp(a->name, name) == 0)
				return nr;
		}
	}
	return -1;
}

sql_arg *
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

sql_arg *
sql_find_param(mvc *sql, char *name)
{
	for (node *n = sql->params->h; n; n = n->next) {
		sql_arg *a = n->data;
		if (strcmp(a->name, name) == 0)
		   return a;
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
		if (sname) { /* user has explicitly typed the schema, so either the object is there or we return error */ \
			if (!(next = mvc_bind_schema(sql, sname))) \
				return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "%s: no such schema '%s'", error, sname); \
			EXTRA_CONDITION(EXTRA); /* for functions without schema, 'sys' is a valid schema to bind them */ \
			if (!res) { \
				CALL; \
			} \
		} else { \
			sql_schema *cur = cur_schema(sql); \
			char *session_schema = cur->base.name; \
 \
			EXTRA; \
			if (!res && !sql->schema_path_has_tmp && strcmp(session_schema, "tmp") != 0) { /* if 'tmp' is not in the search path, search it before all others */ \
				next = tmp_schema(sql); \
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
	const char objstr[] = "sequence";
	sql_sequence *res = NULL;

	search_object_on_path(res = find_sql_sequence(sql->session->tr, next, name), DO_NOTHING, ;, SQLSTATE(42000));
	return res;
}

sql_idx *
find_idx_on_scope(mvc *sql, const char *sname, const char *name, const char *error)
{
	const char objstr[] = "index";
	sql_idx *res = NULL;

	search_object_on_path(res = mvc_bind_idx(sql, next, name), DO_NOTHING, ;, SQLSTATE(42S12));
	return res;
}

sql_type *
find_type_on_scope(mvc *sql, const char *sname, const char *name, const char *error)
{
	const char objstr[] = "type";
	sql_type *res = NULL;

	search_object_on_path(res = schema_bind_type(sql, next, name), DO_NOTHING, ;, SQLSTATE(42S01));
	return res;
}

sql_trigger *
find_trigger_on_scope(mvc *sql, const char *sname, const char *name, const char *error)
{
	const char objstr[] = "trigger";
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
			} else if ((nr = sql_bind_param(sql, name)) >= 0) { /* then if it is a parameter */ \
				*a = sql_bind_paramnr(sql, nr); \
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
	const char objstr[] = "variable";
	bool res = false;
	int nr = 0;

	(void)nr;
	search_object_on_path(var_find_on_global, DO_NOTHING, variable_extra, SQLSTATE(42000));
	return res;
}

static sql_subfunc *
_dup_subaggr(allocator *sa, sql_func *a, sql_subtype *member)
{
	node *tn;
	unsigned int scale = 0, digits = 0;
	sql_subfunc *ares = SA_ZNEW(sa, sql_subfunc);


	ares->func = a;
	if (IS_FILT(a)) {
		ares->res = sa_list(sa);
        list_append(ares->res, sql_bind_localtype("bit"));
	} else if (IS_FUNC(a) || IS_UNION(a) || IS_ANALYTIC(a) || IS_AGGR(a)) { /* not needed for PROC */
		if (a->res) {
			ares->res = sa_list(sa);
			for(tn = a->res->h; tn; tn = tn->next) {
				sql_arg *rarg = tn->data;
				sql_subtype *res, *r = &rarg->type;

				if (a->fix_scale == SCALE_EQ && !IS_AGGR(a)) {
					res = r;
				} else {
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
				}
				list_append(ares->res, res);
			}
		}
	}
	return ares;
}



static sql_subfunc *
func_cmp(allocator *sa, sql_func *f, const char *name, int nrargs)
{
	if (strcmp(f->base.name, name) == 0) {
		if (f->vararg)
			//return (f->type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, NULL, NULL);
			return _dup_subaggr(sa, f, NULL);
		if (nrargs < 0 || list_length(f->ops) == nrargs)
			//return (f->type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, NULL, NULL);
			return _dup_subaggr(sa, f, NULL);
	}
	return NULL;
}

static sql_subfunc *
sql_find_func_internal(mvc *sql, list *ff, const char *fname, int nrargs, sql_ftype type, bool private, sql_subfunc *prev)
{
	int key = hash_key(fname);
	sql_subfunc *res = NULL;
	int found = 0;
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;

	if (ff) {
		if (ff->ht) {
			sql_hash_e *he = ff->ht->buckets[key&(ff->ht->size-1)];
			if (prev) {
				for (; he && !found; he = he->chain)
					if (he->value == prev->func)
						found = 1;
			}
			for (; he; he = he->chain) {
				sql_func *f = he->value;

				if ((f->type != type && f->type != filt) || (f->private && !private))
					continue;
				if ((res = func_cmp(sql->sa, f, fname, nrargs)) != NULL)
					return res;
			}
		} else {
			node *n = ff->h;
			if (prev) {
				for (; n && !found; n = n->next)
					if (n->data == prev->func)
						found = 1;
			}
			for (; n; n = n->next) {
				sql_func *f = n->data;

				if ((f->type != type && f->type != filt) || (f->private && !private))
					continue;
				if ((res = func_cmp(sql->sa, f, fname, nrargs)) != NULL)
					return res;
			}
		}
	}
	return res;
}

static sql_subfunc *
os_find_func_internal(mvc *sql, struct objectset *ff, const char *fname, int nrargs, sql_ftype type, bool private, sql_subfunc *prev)
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

			if ((f->type != type && f->type != filt) || (f->private && !private))
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
		if (!res && (res = sql_find_func_internal(sql, funcs, name, nrargs, type, private, prev))) /* search system wide functions first */ \
			return res; \
	} while (0)

sql_subfunc *
sql_find_func(mvc *sql, const char *sname, const char *name, int nrargs, sql_ftype type, bool private, sql_subfunc *prev)
{
	char *F = NULL, *objstr = NULL;
	const char error[] = "CATALOG";
	sql_subfunc *res = NULL;

	FUNC_TYPE_STR(type, F, objstr);
	(void) F; /* not used */

	assert(nrargs >= -1);

	search_object_on_path(res = os_find_func_internal(sql, next->funcs, name, nrargs, type, private, prev), functions_without_schema, find_func_extra, SQLSTATE(42000));
	return res;
}

sql_subfunc *
sql_bind_func(mvc *sql, const char *sname, const char *fname, sql_subtype *tp1, sql_subtype *tp2, sql_ftype type, bool private, bool exact)
{
	list *l = sa_list(sql->sa);

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);
	return sql_bind_func_(sql, sname, fname, l, type, private, exact);
}

sql_subfunc *
sql_bind_func3(mvc *sql, const char *sname, const char *fname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_ftype type, bool private)
{
	list *l = sa_list(sql->sa);

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);
	if (tp3)
		list_append(l, tp3);
	return sql_bind_func_(sql, sname, fname, l, type, private, false);
}

static int /* bind the function version with more identical type matches */
next_cand_points(list *args, list *ops)
{
	int res = 0;

	if (!list_empty(args) && !list_empty(ops))
	for (node *n = args->h, *m = ops->h; n && m ; n = n->next, m = m->next) {
		sql_arg *a = n->data;
		sql_subtype *t = m->data;

		if (a->type.type->eclass == EC_ANY)
			res += 100;
		else if (t)
			res += a->type.type->base.id == t->type->base.id;
	}
	res += (list_empty(args) && list_empty(ops));
	return res;
}

static int
score_func( sql_func *f, list *tl, bool exact, bool *downcast)
{
	int score = 0;
	node *n, *m;

	if (exact)
		return next_cand_points(f->ops, tl);
	if (!tl)
		return 1;
	if (f->vararg)
		return 1;
	unsigned int digits = 0, scale = 0;
	if (f->fix_scale == SCALE_FIX) {
		for (n = f->ops->h, m = tl->h; n && m; n = n->next, m = m->next) {
			sql_subtype *t = m->data;

			if (!t)
				continue;
			if (t->type->eclass == EC_DEC) {
				if (digits < t->digits)
					digits = t->digits;
				if (scale < t->scale)
					scale = t->scale;
			}
		}
		for (n = f->ops->h, m = tl->h; n && m; n = n->next, m = m->next) {
			sql_arg *a = n->data;
			sql_subtype *t = m->data;

			if (!t)
				continue;
			if (a->type.type->eclass == EC_DEC && t->type->eclass == EC_NUM) {
				if (digits < scale + bits2digits(t->digits))
					digits = scale + bits2digits(t->digits);
			}
		}
	}
	int nr_strconverts = 0;
	for (n = f->ops->h, m = tl->h; n && m; n = n->next, m = m->next) {
		sql_arg *a = n->data;
		sql_subtype *t = m->data;

		if (!t) { /* parameters */
			int ec = a->type.type->eclass;
			score++;
			if (ec == EC_DEC)
				return 0;
			if (a && EC_NUMBER(ec) && !EC_INTERVAL(ec))
				score += a->type.type->localtype * 10; /* premium on larger types */
			else if (a) /* all other types */
				score += 99;
			continue;
		}

		int nscore = sql_type_convert_preference(t->type->eclass, a->type.type->eclass);
		/* EX_EXTERNAL can only convert between equal implementation types */
		if (nscore == 0 &&
			t->type->eclass == a->type.type->eclass &&
			t->type->eclass == EC_EXTERNAL &&
			t->type->localtype == a->type.type->localtype)
				nscore = 10;
		if (nscore &&
			t->type->eclass == EC_NUM && a->type.type->eclass == EC_DEC &&
			t->type->localtype > a->type.type->localtype)
			*downcast = true;
		if (nscore == 0)
			return 0;
		nscore *= 100; /* first based on preferred conversions */
		if (nscore < 0) {
			*downcast = true;
			nscore = -nscore;
		}
		score += nscore;
		if (EC_VARCHAR(t->type->eclass) && EC_NUMBER(a->type.type->eclass))
			nr_strconverts++;
		if (nr_strconverts > 1)
			return 0;

		if (f->fix_scale == SCALE_FIX && a->type.type->eclass == EC_DEC && digits > a->type.type->digits) /* doesn't fit */
			return 0;
		/* sql types equal but implementation differences */
		else if (t->type->eclass == EC_BIT && a->type.type->eclass == EC_NUM && t->type->localtype <= a->type.type->localtype) /* convert bits into smallest number */
			score += (11 + t->type->localtype - a->type.type->localtype) * 8;
		/* sql types close but digits/scale diffs */
		else if (t->type->eclass == EC_NUM && a->type.type->eclass == EC_DEC && /*t->type->localtype <= a->type.type->localtype*/ bits2digits(t->digits) < a->type.type->digits)
			//score += (10 + t->type->localtype - a->type.type->localtype) * 8;
			score += (38 + bits2digits(t->digits) - a->type.type->digits);
		/* same class over converting to other class */
		else if (t->type->eclass == a->type.type->eclass && t->type->localtype <= a->type.type->localtype) {
			score += (11 + t->type->localtype - a->type.type->localtype) * 4;
			/* handle intervals (day, hour, minutes, second) mapped
			 * within same eclass and implementation type. */
			if (t->type->eclass == a->type.type->eclass && t->type->eclass == EC_SEC)
				score += ((t->type->digits > 4 && a->type.type->digits > 4) ||
				          (t->type->digits <= 4 && a->type.type->digits <= 4)) * 4;
		}
		/* conversion matrix needs check if conversion is possible */
		else if (t->type->eclass == a->type.type->eclass) {
				if (t->type->localtype <= a->type.type->localtype) {
					int nscore = sql_type_convert_preference(t->type->eclass, a->type.type->eclass);
					score += (10 + t->type->localtype - a->type.type->localtype) * nscore;
				} else if (t->type->eclass == EC_NUM || t->type->eclass == EC_FLT) { /* down casting */
					*downcast = true;
					score += 10 * (10 - t->type->localtype - a->type.type->localtype);
				}
		}
	}
	score += (list_empty(tl) && list_empty(f->ops));
	return score;
}


static sql_subfunc *
sql_bind_func__(mvc *sql, list *ff, const char *fname, list *ops, sql_ftype type, bool private, bool exact)
{
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;
	sql_subtype *input_type = NULL;
	sql_func *cand = NULL, *dcand = NULL;
	int points = 0, dpoints = 0;

	if (ops && ops->h)
		input_type = ops->h->data;

	assert(ff==funcs);
	if (ff) {
		if (ff->ht) {
			int key = hash_key(fname);
			sql_hash_e *he = ff->ht->buckets[key&(ff->ht->size-1)];
			for (; he; he = he->chain) {
				sql_func *f = he->value;
				bool downcast = false;

				if ((f->type != type && f->type != filt) || (f->private && !private))
					continue;
				if (strcmp(f->base.name, fname) == 0 && ((!exact && (list_length(f->ops) == list_length(ops) || (list_length(f->ops) <= list_length(ops) && f->vararg))) || (exact && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0))) {
					int npoints = score_func(f, ops, exact, &downcast);
					if (downcast) {
						if ((!dcand && (npoints || exact)) || (dcand && npoints > dpoints)) {
							dcand = f;
							dpoints = npoints;
						}
					} else {
						if ((!cand && (npoints || exact)) || (cand && npoints > points)) {
							cand = f;
							points = npoints;
						}
					}
				}
			}
		} else {
			node *n;
			sql_base_loop(ff, n) {
				sql_func *f = n->data;
				bool downcast = false;

				if ((f->type != type && f->type != filt) || (f->private && !private))
					continue;
				if (strcmp(f->base.name, fname) == 0 && ((!exact && (list_length(f->ops) == list_length(ops) || (list_length(f->ops) <= list_length(ops) && f->vararg))) || (exact && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0))) {
					int npoints = score_func(f, ops, exact, &downcast);
					if (downcast) {
						if ((!dcand && (npoints || exact)) || (dcand && npoints > dpoints)) {
							dcand = f;
							dpoints = npoints;
						}
					} else {
						if ((!cand && (npoints || exact)) || (cand && npoints > points)) {
							cand = f;
							points = npoints;
						}
					}
				}
			}
		}
	}
	if (!cand && dcand)
		cand = dcand;
	if (cand && exact && type != F_AGGR)
			return sql_dup_subfunc(sql->sa, cand, ops, NULL);
	if (cand)
		//return (type == F_AGGR) ? _dup_subaggr(sql->sa, cand, input_type) : sql_dup_subfunc(sql->sa, cand, ops, NULL);
		return _dup_subaggr(sql->sa, cand, (type == F_AGGR) ?input_type: NULL);
	return NULL;
}

static sql_subfunc *
os_bind_func__(mvc *sql, struct objectset *ff, const char *fname, list *ops, sql_ftype type, bool private, bool exact)
{
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;
	sql_subtype *input_type = NULL;
	sql_func *cand = NULL, *dcand = NULL;
	int points = 0, dpoints = 0;

	if (ops && ops->h)
		input_type = ops->h->data;

	if (ff) {
		struct os_iter oi;
		os_iterator(&oi, ff, sql->session->tr, fname);
		for (sql_base *b = oi_next(&oi); b; b=oi_next(&oi)) {
			sql_func *f = (sql_func*)b;
			bool downcast = false;

			if ((f->type != type && f->type != filt) || (f->private && !private))
				continue;
			if (strcmp(f->base.name, fname) == 0 && ((!exact && (list_length(f->ops) == list_length(ops) || (list_length(f->ops) <= list_length(ops) && f->vararg))) || (exact && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0))) {
				int npoints = score_func(f, ops, exact, &downcast);
				if (downcast) {
					if ((!dcand && (npoints || exact)) || (dcand && npoints > dpoints)) {
						dcand = f;
						dpoints = npoints;
					}
				} else {
					if ((!cand && (npoints || exact)) || (cand && npoints > points)) {
						cand = f;
						points = npoints;
					}
				}
			}
		}
	}
	if (!cand && dcand)
		cand = dcand;
	if (cand && exact && type != F_AGGR)
			return sql_dup_subfunc(sql->sa, cand, ops, NULL);
	if (cand)
		//return (type == F_AGGR) ? _dup_subaggr(sql->sa, cand, input_type) : sql_dup_subfunc(sql->sa, cand, ops, NULL);
		return _dup_subaggr(sql->sa, cand, (type == F_AGGR) ?input_type: NULL);
	return NULL;
}

#define sql_bind_func__extra \
	do { \
		if (!res && (res = sql_bind_func__(sql, funcs, name, ops, type, private, exact))) /* search system wide functions first */ \
			return res; \
	} while (0)

sql_subfunc *
sql_bind_func_(mvc *sql, const char *sname, const char *name, list *ops, sql_ftype type, bool private, bool exact)
{
	char *F = NULL, *objstr = NULL;
	const char error[] = "CATALOG";
	sql_subfunc *res = NULL;

	FUNC_TYPE_STR(type, F, objstr);
	(void) F; /* not used */

	search_object_on_path(res = os_bind_func__(sql, next->funcs, name, ops, type, private, exact), functions_without_schema, sql_bind_func__extra, SQLSTATE(42000));
	return res;
}

static sql_subfunc *
sql_bind_func_result_internal(mvc *sql, list *ff, const char *fname, sql_ftype type, bool private, list *ops, sql_subtype *res)
{
	sql_subtype *tp = sql_bind_localtype("bit");
	sql_func *cand = NULL;
	int points = 0, npoints = 0;

	if (ff) {
		if (ff->ht) {
			int key = hash_key(fname);
			sql_hash_e *he = ff->ht->buckets[key&(ff->ht->size-1)];
			for (; he; he = he->chain) {
				sql_func *f = he->value;
				sql_arg *firstres = NULL;

				if ((!f->res && !IS_FILT(f)) || (f->private && !private))
					continue;
				firstres = IS_FILT(f)?tp->type:f->res->h->data;
				if (strcmp(f->base.name, fname) == 0 && f->type == type && (is_subtype(&firstres->type, res) || firstres->type.type->eclass == EC_ANY) && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0) {
					npoints = next_cand_points(f->ops, ops);

					if (!cand || npoints > points) {
						cand = f;
						points = npoints;
					}
				}
			}
		} else {
			node *n;
			sql_base_loop( ff, n) {
				sql_func *f = n->data;
				sql_arg *firstres = NULL;

				if ((!f->res && !IS_FILT(f)) || (f->private && !private))
					continue;
				firstres = IS_FILT(f)?tp->type:f->res->h->data;
				if (strcmp(f->base.name, fname) == 0 && f->type == type && (is_subtype(&firstres->type, res) || firstres->type.type->eclass == EC_ANY) && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0) {
					npoints = next_cand_points(f->ops, ops);

					if (!cand || npoints > points) {
						cand = f;
						points = npoints;
					}
				}
			}
		}
	}
	if (cand)
		return (type == F_AGGR) ? _dup_subaggr(sql->sa, cand, NULL) : sql_dup_subfunc(sql->sa, cand, ops, NULL);
	return NULL;
}

static sql_subfunc *
os_bind_func_result_internal(mvc *sql, struct objectset *ff, const char *fname, sql_ftype type, bool private, list *ops, sql_subtype *res)
{
	sql_subtype *tp = sql_bind_localtype("bit");
	sql_func *cand = NULL;
	int points = 0, npoints = 0;

	if (ff) {
		struct os_iter oi;
		os_iterator(&oi, ff, sql->session->tr, fname);
		for (sql_base *b = oi_next(&oi); b; b=oi_next(&oi)) {
			sql_func *f = (sql_func*)b;
			sql_arg *firstres = NULL;

			if ((!f->res && !IS_FILT(f)) || (f->private && !private))
				continue;
			firstres = IS_FILT(f)?tp->type:f->res->h->data;
			if (strcmp(f->base.name, fname) == 0 && f->type == type && (is_subtype(&firstres->type, res) || firstres->type.type->eclass == EC_ANY) && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0) {
				npoints = next_cand_points(f->ops, ops);

				if (!cand || npoints > points) {
					cand = f;
					points = npoints;
				}
			}
		}
	}
	if (cand)
		return (type == F_AGGR) ? _dup_subaggr(sql->sa, cand, NULL) : sql_dup_subfunc(sql->sa, cand, ops, NULL);
	return NULL;
}

#define sql_bind_func_result_extra \
	do { \
		if (!res) \
			res = sql_bind_func_result_internal(sql, funcs, name, type, private, ops, r_res); /* search system wide functions first */ \
	} while (0)


sql_subfunc *
sql_bind_func_result(mvc *sql, const char *sname, const char *name, sql_ftype type, bool private, sql_subtype *r_res, int nargs, ...)
{
	char *F = NULL, *objstr = NULL;
	const char error[] = "CATALOG";
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

	search_object_on_path(res = os_bind_func_result_internal(sql, next->funcs, name, type, private, ops, r_res), functions_without_schema, sql_bind_func_result_extra, SQLSTATE(42000));
	if (res) /* make sure we have the correct result type */
		res->res->h->data = r_res;
	return res;
}

static list *
sql_find_funcs_by_name_internal(mvc *sql, list *ff, const char *fname, sql_ftype type, bool private)
{
	int key = hash_key(fname);
	list *res = NULL;

	if (ff) {
		if (ff->ht) {
			for (sql_hash_e *he = ff->ht->buckets[key&(ff->ht->size-1)]; he; he = he->chain) {
				sql_func *f = he->value;

				if (f->type != type || (f->private && !private))
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

				if (f->type != type || (f->private && !private))
					continue;
				if (strcmp(f->base.name, fname) == 0) {
					if (!res)
						res = sa_list(sql->sa);
					list_append(res, f);
				}
			}
		}
	}
	return res;
}

static list *
os_find_funcs_by_name_internal(mvc *sql, struct objectset *ff, const char *fname, sql_ftype type, bool private)
{
	list *res = NULL;

	if (ff) {
		struct os_iter oi;
		os_iterator(&oi, ff, sql->session->tr, fname);
		for (sql_base *b = oi_next(&oi); b; b=oi_next(&oi)) {
			sql_func *f = (sql_func*)b;

			if (f->type != type || (f->private && !private))
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
		if (!res && (res = sql_find_funcs_by_name_internal(sql, funcs, name, type, private))) /* search system wide functions first */ \
			return res; \
	} while (0)

list *
sql_find_funcs_by_name(mvc *sql, const char *sname, const char *name, sql_ftype type, bool private)
{
	char *F = NULL, *objstr = NULL;
	const char error[] = "CATALOG";
	list *res = NULL;

	FUNC_TYPE_STR(type, F, objstr);
	(void) F; /* not used */

	search_object_on_path(res = os_find_funcs_by_name_internal(sql, next->funcs, name, type, private), functions_without_schema, sql_find_funcs_by_name_extra, SQLSTATE(42000));
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
 * (like case expressions or columns in a result of a query expression).
 * See standaard pages 505-507 Result of data type combinations */
sql_subtype *
result_datatype(sql_subtype *super, sql_subtype *l, sql_subtype *r)
{
	int lclass = l->type->eclass, rclass = r->type->eclass;

	/* case a strings */
	if (EC_VARCHAR(lclass) || EC_VARCHAR(rclass)) {
		const char *tpe = "varchar";
		unsigned int digits = 0;
		if (!EC_VARCHAR(lclass)) {
			tpe = r->type->base.name;
			digits = (!l->digits)?0:r->digits;
		} else if (!EC_VARCHAR(rclass)) {
			tpe = l->type->base.name;
			digits = (!r->digits)?0:l->digits;
		} else { /* both */
			tpe = !strcmp(l->type->base.name, "varchar")?l->type->base.name:!strcmp(r->type->base.name, "varchar")?r->type->base.name:
			(l->type->base.id > r->type->base.id)?l->type->base.name:r->type->base.name;
			digits = (!l->digits||!r->digits)?0:sql_max(l->digits, r->digits);
		}
		sql_find_subtype(super, tpe, digits, 0);
	/* case b blob */
	} else if (lclass == EC_BLOB || rclass == EC_BLOB) {
		*super = (lclass == EC_BLOB) ? *l : *r;
	/* case c all exact numeric */
	} else if (EC_EXACTNUM(lclass) && EC_EXACTNUM(rclass)) {
		char *tpe = (l->type->base.id > r->type->base.id)?l->type->base.name:r->type->base.name;
		unsigned int digits = sql_max(l->digits, r->digits);
		unsigned int scale = sql_max(l->scale, r->scale);

		if (l->type->radix == 10 && r->type->radix == 10) {
			digits = scale + (sql_max(l->digits - l->scale, r->digits - r->scale));
#ifdef HAVE_HGE
			if (digits > 38) {
				digits = 38;
#else
			if (digits > 18) {
				digits = 18;
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
			char *tpe = (l->type->base.id > r->type->base.id)?l->type->base.name:r->type->base.name;
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
symbol_escape_ident(allocator *sa, const char *s)
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
		const char msg[] = "SQL feature not yet available for expressions and default values: ";
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
