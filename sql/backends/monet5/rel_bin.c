/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "rel_bin.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_psm.h"
#include "rel_prop.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"
#include "sql_env.h"
#include "sql_optimizer.h"
#include "sql_gencode.h"
#include "mal_builder.h"
#include "opt_prelude.h"

#define OUTER_ZERO 64

static stmt * exp_bin(backend *be, sql_exp *e, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel, int depth, int reduce, int push);
static stmt * rel_bin(backend *be, sql_rel *rel);
static stmt * subrel_bin(backend *be, sql_rel *rel, list *refs);

static stmt *check_types(backend *be, sql_subtype *ct, stmt *s, check_type tpe);

static void
clean_mal_statements(backend *be, int oldstop, int oldvtop, int oldvid)
{
	MSresetInstructions(be->mb, oldstop);
	freeVariables(be->client, be->mb, NULL, oldvtop, oldvid);
}

static stmt *
stmt_selectnil( backend *be, stmt *col)
{
	sql_subtype *t = tail_type(col);
	stmt *n = stmt_atom(be, atom_general(be->mvc->sa, t, NULL));
	stmt *nn = stmt_uselect2(be, col, n, n, 3, NULL, 0, 1);
	return nn;
}

static stmt *
sql_unop_(backend *be, const char *fname, stmt *rs)
{
	mvc *sql = be->mvc;
	sql_subtype *rt = NULL;
	sql_subfunc *f = NULL;

	rt = tail_type(rs);
	f = sql_bind_func(sql, "sys", fname, rt, NULL, F_FUNC);
	/* try to find the function without a type, and convert
	 * the value to the type needed by this function!
	 */
	if (!f && (f = sql_find_func(sql, "sys", fname, 1, F_FUNC, NULL)) != NULL) {
		sql_arg *a = f->func->ops->h->data;

		sql->session->status = 0;
		sql->errstr[0] = '\0';
		rs = check_types(be, &a->type, rs, type_equal);
		if (!rs)
			f = NULL;
	}
	if (f) {
		/*
		if (f->func->res.scale == INOUT) {
			f->res.digits = rt->digits;
			f->res.scale = rt->scale;
		}
		*/
		return stmt_unop(be, rs, NULL, f);
	} else if (rs) {
		char *type = tail_type(rs)->type->sqlname;

		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: no such unary operator '%s(%s)'", fname, type);
	}
	return NULL;
}

static stmt *
refs_find_rel(list *refs, sql_rel *rel)
{
	node *n;

	for(n=refs->h; n; n = n->next->next) {
		sql_rel *ref = n->data;
		stmt *s = n->next->data;

		if (rel == ref)
			return s;
	}
	return NULL;
}

static void
refs_update_stmt(list *refs, sql_rel *rel, stmt *s)
{
	node *n;

	for(n=refs->h; n; n = n->next->next) {
		sql_rel *ref = n->data;

		if (rel == ref) {
			n->next->data = s;
			break;
		}
	}
}


static void
print_stmtlist(sql_allocator *sa, stmt *l)
{
	node *n;
	if (l) {
		for (n = l->op4.lval->h; n; n = n->next) {
			const char *rnme = table_name(sa, n->data);
			const char *nme = column_name(sa, n->data);

			TRC_INFO(SQL_EXECUTION, "%s.%s\n", rnme ? rnme : "(null!)", nme ? nme : "(null!)");
		}
	}
}

static stmt *
list_find_column(backend *be, list *l, const char *rname, const char *name )
{
	stmt *res = NULL;
	node *n;

	if (!l)
		return NULL;
	if (!l->ht && list_length(l) > HASH_MIN_SIZE) {
		l->ht = hash_new(l->sa, MAX(list_length(l), l->expected_cnt), (fkeyvalue)&stmt_key);
		if (l->ht == NULL)
			return NULL;

		for (n = l->h; n; n = n->next) {
			const char *nme = column_name(be->mvc->sa, n->data);
			if (nme) {
				int key = hash_key(nme);

				if (hash_add(l->ht, key, n->data) == NULL)
					return NULL;
			}
		}
	}
	if (l->ht) {
		int key = hash_key(name);
		sql_hash_e *e = l->ht->buckets[key&(l->ht->size-1)];

		if (rname) {
			for (; e; e = e->chain) {
				stmt *s = e->value;
				const char *rnme = table_name(be->mvc->sa, s);
				const char *nme = column_name(be->mvc->sa, s);

				if (rnme && strcmp(rnme, rname) == 0 &&
		 	            strcmp(nme, name) == 0) {
					res = s;
					break;
				}
			}
		} else {
			for (; e; e = e->chain) {
				stmt *s = e->value;
				const char *rnme = table_name(be->mvc->sa, s);
				const char *nme = column_name(be->mvc->sa, s);

				if (!rnme && nme && strcmp(nme, name) == 0) {
					res = s;
					break;
				}
			}
		}
		if (!res)
			return NULL;
		return res;
	}
	if (rname) {
		for (n = l->h; n; n = n->next) {
			const char *rnme = table_name(be->mvc->sa, n->data);
			const char *nme = column_name(be->mvc->sa, n->data);

			if (rnme && strcmp(rnme, rname) == 0 &&
				    strcmp(nme, name) == 0) {
				res = n->data;
				break;
			}
		}
	} else {
		for (n = l->h; n; n = n->next) {
			const char *rnme = table_name(be->mvc->sa, n->data);
			const char *nme = column_name(be->mvc->sa, n->data);

			if (!rnme && nme && strcmp(nme, name) == 0) {
				res = n->data;
				break;
			}
		}
	}
	if (!res)
		return NULL;
	return res;
}

static stmt *
bin_find_column( backend *be, stmt *sub, const char *rname, const char *name )
{
	return list_find_column( be, sub->op4.lval, rname, name);
}

static list *
bin_find_columns( backend *be, stmt *sub, const char *name )
{
	node *n;
	list *l = sa_list(be->mvc->sa);

	for (n = sub->op4.lval->h; n; n = n->next) {
		const char *nme = column_name(be->mvc->sa, n->data);

		if (strcmp(nme, name) == 0)
			append(l, n->data);
	}
	if (list_length(l))
		return l;
	return NULL;
}

static stmt *column(backend *be, stmt *val )
{
	if (val->nrcols == 0)
		return const_column(be, val);
	return val;
}

static stmt *create_const_column(backend *be, stmt *val )
{
	if (val->nrcols == 0)
		val = const_column(be, val);
	return stmt_append(be, stmt_temp(be, tail_type(val)), val);
}

static int
statment_score(stmt *c)
{
	sql_subtype *t = tail_type(c);
	int score = 0;

	if (c->nrcols != 0) /* no need to create an extra intermediate */
		score += 200;

	if (!t)
		return score;
	switch (ATOMstorage(t->type->localtype)) { /* give preference to smaller types */
		case TYPE_bte:
			score += 150 - 8;
			break;
		case TYPE_sht:
			score += 150 - 16;
			break;
		case TYPE_int:
			score += 150 - 32;
			break;
		case TYPE_void:
		case TYPE_lng:
			score += 150 - 64;
			break;
		case TYPE_uuid:
#ifdef HAVE_HGE
		case TYPE_hge:
#endif
			score += 150 - 128;
			break;
		case TYPE_flt:
			score += 75 - 24;
			break;
		case TYPE_dbl:
			score += 75 - 53;
			break;
		default:
			break;
	}
	return score;
}

static stmt *
bin_find_smallest_column(backend *be, stmt *sub)
{
	stmt *res = NULL;
	int best_score = 0;

	for (node *n = sub->op4.lval->h ; n ; n = n->next) {
		stmt *c = n->data;
		int next_score = statment_score(c);

		if (next_score >= best_score) {
			res = c;
			best_score = next_score;
		}
	}
	if (res->nrcols == 0)
		return const_column(be, res);
	return res;
}

static stmt *
row2cols(backend *be, stmt *sub)
{
	if (sub->nrcols == 0 && sub->key) {
		node *n;
		list *l = sa_list(be->mvc->sa);

		for (n = sub->op4.lval->h; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(be->mvc->sa, sc);
			const char *tname = table_name(be->mvc->sa, sc);

			sc = column(be, sc);
			list_append(l, stmt_alias(be, sc, tname, cname));
		}
		sub = stmt_list(be, l);
	}
	return sub;
}

static stmt*
distinct_value_list(backend *be, list *vals, stmt ** last_null_value)
{
	list *l = sa_list(be->mvc->sa);
	stmt *s;

	/* create bat append values */
	for (node *n = vals->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *i = exp_bin(be, e, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);

		if (exp_is_null(e))
			*last_null_value = i;

		if (!i)
			return NULL;

		list_append(l, i);
	}
	s = stmt_append_bulk(be, stmt_temp(be, exp_subtype(vals->h->data)), l);
	/* Probably faster to filter out the values directly in the underlying list of atoms.
	   But for now use groupby to filter out duplicate values. */
	stmt* groupby = stmt_group(be, s, NULL, NULL, NULL, 1);
	stmt* ext = stmt_result(be, groupby, 1);

	return stmt_project(be, ext, s);
}

static stmt *
stmt_selectnonil( backend *be, stmt *col, stmt *s )
{
	sql_subtype *t = tail_type(col);
	stmt *n = stmt_atom(be, atom_general(be->mvc->sa, t, NULL));
	stmt *nn = stmt_uselect2(be, col, n, n, 3, s, 1, 1);
	return nn;
}

static int
is_tid_chain(stmt *cand)
{
	while(cand && cand->type != st_tid && cand->cand) {
		cand = cand->cand;
	}
	if (cand && cand->type == st_tid)
		return 1;
	return 0;
}

static stmt *
subrel_project( backend *be, stmt *s, list *refs, sql_rel *rel)
{
	if (!s || s->type != st_list || !s->cand)
		return s;

	list *l = sa_list(be->mvc->sa);
	stmt *cand = s->cand;
	if (!l)
		return NULL;
	for(node *n = s->op4.lval->h; n; n = n->next) {
		stmt *c = n->data;

		assert(c->type == st_alias || (c->type == st_join && c->flag == cmp_project) || c->type == st_bat || c->type == st_idxbat || c->type == st_single);
		if (c->type != st_alias) {
			c = stmt_project(be, cand, c);
		} else if (c->op1->type == st_mirror && is_tid_chain(cand)) { /* alias with mirror (ie full row ids) */
			c = stmt_alias(be, cand, c->tname, c->cname);
		} else { /* st_alias */
			stmt *s = c->op1;
			if (s->nrcols == 0)
				s = stmt_const(be, cand, s);
			else
				s = stmt_project(be, cand, s);
			c = stmt_alias(be, s, c->tname, c->cname);
		}
		append(l, c);
	}
	s = stmt_list(be, l);
	if (rel && rel_is_ref(rel))
		refs_update_stmt(refs, rel, s);
	return s;
}

static stmt *
handle_in_exps(backend *be, sql_exp *ce, list *nl, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel, bool in, int use_r, int depth, int reduce)
{
	mvc *sql = be->mvc;
	node *n;
	stmt *s = NULL, *c = exp_bin(be, ce, left, right, grp, ext, cnt, NULL, 0, 0, 0);

	if(!c)
		return NULL;

	if (reduce && c->nrcols == 0)
		c = stmt_const(be, bin_find_smallest_column(be, left), c);

	if (c->nrcols == 0 || depth || !reduce) {
		sql_subtype *bt = sql_bind_localtype("bit");
		sql_subfunc *cmp = (in)
			?sql_bind_func(sql, "sys", "=", tail_type(c), tail_type(c), F_FUNC)
			:sql_bind_func(sql, "sys", "<>", tail_type(c), tail_type(c), F_FUNC);
		sql_subfunc *a = (in)?sql_bind_func(sql, "sys", "or", bt, bt, F_FUNC)
				     :sql_bind_func(sql, "sys", "and", bt, bt, F_FUNC);

		for( n = nl->h; n; n = n->next) {
			sql_exp *e = n->data;
			stmt *i = exp_bin(be, use_r?e->r:e, left, right, grp, ext, cnt, NULL, 0, 0, 0);
			if(!i)
				return NULL;

			i = stmt_binop(be, c, i, NULL, cmp);
			if (s)
				s = stmt_binop(be, s, i, NULL, a);
			else
				s = i;
		}
		if (sel && !(depth || !reduce))
			s = stmt_uselect(be,
				stmt_const(be, bin_find_smallest_column(be, left), s),
				stmt_bool(be, 1), cmp_equal, sel, 0, 0);
	} else if (list_length(nl) < 16) {
		comp_type cmp = (in)?cmp_equal:cmp_notequal;

		if (!in)
			s = sel;
		for( n = nl->h; n; n = n->next) {
			sql_exp *e = n->data;
			stmt *i = exp_bin(be, use_r?e->r:e, left, right, grp, ext, cnt, NULL, 0, 0, 0);
			if(!i)
				return NULL;

			if (in) {
				i = stmt_uselect(be, c, i, cmp, sel, 0, 0);
				if (s)
					s = stmt_tunion(be, s, i);
				else
					s = i;
			} else {
				s = stmt_uselect(be, c, i, cmp, s, 0, 0);
			}
		}
	} else {
		/* TODO: handle_in_exps should contain all necessary logic for in-expressions to be SQL compliant.
		   For non-SQL-standard compliant behavior, e.g. PostgreSQL backwards compatibility, we should
		   make sure that this behavior is replicated by the sql optimizer and not handle_in_exps. */

		stmt* last_null_value = NULL;  /* CORNER CASE ALERT: See description below. */

		/* The actual in-value-list should not contain duplicates to ensure that final join results are unique. */
		s = distinct_value_list(be, nl, &last_null_value);
		if (!s)
			return NULL;

		if (last_null_value) {
			/* The actual in-value-list should not contain null values. */
			s = stmt_project(be, stmt_selectnonil(be, s, NULL), s);
		}

		if (in) {
			s = stmt_semijoin(be, c, s, sel, NULL, 0, false);
		} else {
			if (last_null_value) {
				/* CORNER CASE ALERT:
				   In case of a not-in-expression with the associated in-value-list containing a null value,
				   the entire in-predicate is forced to always return false, i.e. an empty candidate list.
				   This is similar to postgres behavior.
				   TODO: However I do not think this behavior is in accordance with SQL standard 2003.

				   Ugly trick to return empty candidate list, because for all x it holds that: (x == null) == false.
				   list* singleton_bat = sa_list(sql->sa);
				   list_append(singleton_bat, null_value); */
				s = stmt_uselect(be, c, last_null_value, cmp_equal, NULL, 0, 0);
			} else {
				/* BACK TO HAPPY FLOW:
				   Make sure that null values are never returned. */
				stmt* non_nulls;
				non_nulls = stmt_selectnonil(be, c, sel);
				s = stmt_tdiff(be, stmt_project(be, non_nulls, c), s, NULL);
				s = stmt_project(be, s, non_nulls);
			}
		}
	}
	return s;
}

static stmt *
value_list(backend *be, list *vals, stmt *left, stmt *sel)
{
	sql_subtype *type = exp_subtype(vals->h->data);
	list *l;

	if (!type)
		return sql_error(be->mvc, 02, SQLSTATE(42000) "Could not infer the type of a value list column");
	/* create bat append values */
	l = sa_list(be->mvc->sa);
	for (node *n = vals->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *i = exp_bin(be, e, left, NULL, NULL, NULL, NULL, sel, 0, 0, 0);

		if (!i)
			return NULL;

		if (list_length(vals) == 1)
			return i;
		list_append(l, i);
	}
	return stmt_append_bulk(be, stmt_temp(be, type), l);
}

static stmt *
exp_list(backend *be, list *exps, stmt *l, stmt *r, stmt *grp, stmt *ext, stmt *cnt, stmt *sel)
{
	mvc *sql = be->mvc;
	node *n;
	list *nl = sa_list(sql->sa);

	for( n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *i = exp_bin(be, e, l, r, grp, ext, cnt, sel, 0, 0, 0);
		if(!i)
			return NULL;

		if (n->next && i->type == st_table) /* relational statement */
			l = i->op1;
		else
			append(nl, i);
	}
	return stmt_list(be, nl);
}

static stmt *
exp_count_no_nil_arg( sql_exp *e, stmt *ext, sql_exp *ae, stmt *as )
{
	/* small optimization, ie use candidates directly on count(*) */
	if (!need_distinct(e) && !ext && as && (!need_no_nil(e) || !ae || !has_nil(ae))) {
		/* skip alias statements */
		while (as->type == st_alias)
			as = as->op1;
		/* use candidate */
	       	if (as && as->type == st_join && as->flag == cmp_project) {
			if (as->op1 && (as->op1->type != st_result || as->op1->op1->type != st_group)) /* exclude a subquery with select distinct under the count */
				as = as->op1;
		}
	}
	return as;
}

static stmt *
exp_bin_or(backend *be, sql_exp *e, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel, int depth, bool reduce, int push)
{
	sql_subtype *bt = sql_bind_localtype("bit");
	list *l = e->l;
	node *n;
	stmt *sel1 = NULL, *sel2 = NULL, *s = NULL;
	int anti = is_anti(e);

	sel1 = sel;
	sel2 = sel;
	for( n = l->h; n; n = n->next ) {
		sql_exp *c = n->data;
		stmt *sin = (sel1 && sel1->nrcols)?sel1:NULL;

		/* propagate the anti flag */
		if (anti)
			set_anti(c);
		s = exp_bin(be, c, left, right, grp, ext, cnt, sin, depth, reduce, push);
		if (!s)
			return s;

		if (!sin && sel1 && sel1->nrcols == 0 && s->nrcols == 0) {
			sql_subfunc *f = sql_bind_func(be->mvc, "sys", anti?"or":"and", bt, bt, F_FUNC);
			assert(f);
			s = stmt_binop(be, sel1, s, sin, f);
		} else if (sel1 && (sel1->nrcols == 0 || s->nrcols == 0)) {
			stmt *predicate = bin_find_smallest_column(be, left);

			predicate = stmt_const(be, predicate, stmt_bool(be, 1));
			if (s->nrcols == 0)
				s = stmt_uselect(be, predicate, s, cmp_equal, sel1, anti, is_semantics(c));
			else
				s = stmt_uselect(be, predicate, sel1, cmp_equal, s, anti, is_semantics(c));
		}
		sel1 = s;
	}
	l = e->r;
	for( n = l->h; n; n = n->next ) {
		sql_exp *c = n->data;
		stmt *sin = (sel2 && sel2->nrcols)?sel2:NULL;

		/* propagate the anti flag */
		if (anti)
			set_anti(c);
		s = exp_bin(be, c, left, right, grp, ext, cnt, sin, depth, reduce, push);
		if (!s)
			return s;

		if (!sin && sel2 && sel2->nrcols == 0 && s->nrcols == 0) {
			sql_subfunc *f = sql_bind_func(be->mvc, "sys", anti?"or":"and", bt, bt, F_FUNC);
			assert(f);
			s = stmt_binop(be, sel2, s, sin, f);
		} else if (sel2 && (sel2->nrcols == 0 || s->nrcols == 0)) {
			stmt *predicate = bin_find_smallest_column(be, left);

			predicate = stmt_const(be, predicate, stmt_bool(be, 1));
			if (s->nrcols == 0)
				s = stmt_uselect(be, predicate, s, cmp_equal, sel2, anti, 0);
			else
				s = stmt_uselect(be, predicate, sel2, cmp_equal, s, anti, 0);
		}
		sel2 = s;
	}
	if (sel1->nrcols == 0 && sel2->nrcols == 0) {
		sql_subfunc *f = sql_bind_func(be->mvc, "sys", anti?"and":"or", bt, bt, F_FUNC);
		assert(f);
		return stmt_binop(be, sel1, sel2, NULL, f);
	}
	if (sel1->nrcols == 0) {
		stmt *predicate = bin_find_smallest_column(be, left);

		predicate = stmt_const(be, predicate, stmt_bool(be, 1));
		sel1 = stmt_uselect(be, predicate, sel1, cmp_equal, NULL, 0/*anti*/, 0);
	}
	if (sel2->nrcols == 0) {
		stmt *predicate = bin_find_smallest_column(be, left);

		predicate = stmt_const(be, predicate, stmt_bool(be, 1));
		sel2 = stmt_uselect(be, predicate, sel2, cmp_equal, NULL, 0/*anti*/, 0);
	}
	if (anti)
		return stmt_project(be, stmt_tinter(be, sel1, sel2, false), sel1);
	return stmt_tunion(be, sel1, sel2);
}

static stmt *
exp2bin_case(backend *be, sql_exp *fe, stmt *left, stmt *right, stmt *isel, int depth)
{
	stmt *res = NULL, *ires = NULL, *rsel = NULL, *osel = NULL, *ncond = NULL, *ocond = NULL, *cond = NULL;
	int next_cond = 1, single_value = (fe->card <= CARD_ATOM && (!left || !left->nrcols));
	char name[16], *nme = NULL;
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *not = sql_bind_func(be->mvc, "sys", "not", bt, NULL, F_FUNC);
	sql_subfunc *or = sql_bind_func(be->mvc, "sys", "or", bt, bt, F_FUNC);
	sql_subfunc *and = sql_bind_func(be->mvc, "sys", "and", bt, bt, F_FUNC);

	if (single_value) {
		/* var_x = nil; */
		nme = number2name(name, sizeof(name), ++be->mvc->label);
		(void)stmt_var(be, NULL, nme, exp_subtype(fe), 1, 2);
	}

	list *exps = fe->l;

	/*
	 * left - isel: calls down need id's from the range of left
	 * res  - rsel: updates too res need id's in the range from res
	 */
	for (node *en = exps->h; en; en = en->next) {
		sql_exp *e = en->data;

		next_cond = next_cond && en->next; /* last else is only a value */

		stmt *nsel = rsel;
		if (!single_value) {
			if (/*!next_cond &&*/ rsel && isel) {
				/* back into left range */
				nsel = stmt_project(be, rsel, isel);
			} else if (isel && !rsel)
				nsel = isel;
		}
		stmt *es = exp_bin(be, e, left, right, NULL, NULL, NULL, nsel, depth+1, 0, 1);

		if (!es)
			return NULL;
		if (!single_value) {
			/* create result */
			if (!res) {
				stmt *l = isel;
				if (!l)
					l = bin_find_smallest_column(be, left);
				res = stmt_const(be, l, stmt_atom(be, atom_general(be->mvc->sa, exp_subtype(fe), NULL)));
				ires = l;
				if (res)
					res->cand = isel;
			} else if (res && !next_cond) { /* use result too update column */
				stmt *val = es;
				stmt *pos = rsel;

				if (val->nrcols == 0)
					val = stmt_const(be, pos, val);
				else if (!val->cand && nsel)
					val = stmt_project(be, nsel, val);
				res = stmt_replace(be, res, pos, val);

				assert(cond);

				if (en->next) {
					/* osel - rsel */
					if (!osel)
						osel = stmt_mirror(be, ires);
					stmt *d = stmt_tdiff(be, osel, rsel, NULL);
					osel = rsel = stmt_project(be, d, osel);
				}
			}
			if (next_cond) {
				ncond = cond = es;
				if (!ncond->nrcols) {
					if (osel) {
						ncond = stmt_const(be, nsel, ncond);
						ncond->cand = nsel;
					} else if (isel) {
						ncond = stmt_const(be, isel, ncond);
						ncond->cand = isel;
					} else
						ncond = stmt_const(be, bin_find_smallest_column(be, left), ncond);
				}
				if (isel && !ncond->cand) {
					ncond = stmt_project(be, nsel, ncond);
					ncond->cand = nsel;
				}
				stmt *s = stmt_uselect(be, ncond, stmt_bool(be, 1), cmp_equal, !ncond->cand?rsel:NULL, 0/*anti*/, 0);
				if (rsel && ncond->cand)
					rsel = stmt_project(be, s, rsel);
				else
					rsel = s;
			}
		} else {
			if (!res) {
				/* if_barrier ... */
				assert(next_cond);
				if (next_cond) {
					if (cond) {
						ncond = stmt_binop(be, cond, es, nsel, and);
					} else {
						ncond = es;
					}
					cond = es;
				}
			} else {
				/* var_x = s */
				(void)stmt_assign(be, NULL, nme, es, 2);
				/* endif_barrier */
				(void)stmt_control_end(be, res);
				res = NULL;

				if (en->next) {
					cond = stmt_unop(be, cond, nsel, not);

					sql_subfunc *isnull = sql_bind_func(be->mvc, "sys", "isnull", bt, NULL, F_FUNC);
					cond = stmt_binop(be, cond, stmt_unop(be, cond, nsel, isnull), nsel, or);
					if (ocond)
						cond = stmt_binop(be, ocond, cond, nsel, and);
					ocond = cond;
					if (!en->next->next)
						ncond = cond;
				}
			}
			if (ncond && (next_cond || (en->next && !en->next->next))) {
				/* if_barrier ... */
				res = stmt_cond(be, ncond, NULL, 0, 0);
			}
		}
		next_cond = !next_cond;
	}
	if (single_value)
		return stmt_var(be, NULL, nme, exp_subtype(fe), 0, 2);
	return res;
}

static stmt *
exp2bin_casewhen(backend *be, sql_exp *fe, stmt *left, stmt *right, stmt *isel, int depth)
{
	stmt *res = NULL, *ires = NULL, *rsel = NULL, *osel = NULL, *ncond = NULL, *ocond = NULL, *cond = NULL;
	int next_cond = 1, single_value = (fe->card <= CARD_ATOM && (!left || !left->nrcols));
	char name[16], *nme = NULL;
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *not = sql_bind_func(be->mvc, "sys", "not", bt, NULL, F_FUNC);
	sql_subfunc *or = sql_bind_func(be->mvc, "sys", "or", bt, bt, F_FUNC);
	sql_subfunc *and = sql_bind_func(be->mvc, "sys", "and", bt, bt, F_FUNC);
	sql_subfunc *cmp;

	if (single_value) {
		/* var_x = nil; */
		nme = number2name(name, sizeof(name), ++be->mvc->label);
		(void)stmt_var(be, NULL, nme, exp_subtype(fe), 1, 2);
	}

	list *exps = fe->l;
	node *en = exps->h;
	sql_exp *e = en->data;

	stmt *nsel = !single_value?isel:NULL;
	stmt *case_when = exp_bin(be, e, left, right, NULL, NULL, NULL, nsel, depth+1, 0, 1);
	if (!case_when)
		return NULL;
   	cmp = sql_bind_func(be->mvc, "sys", "=", exp_subtype(e), exp_subtype(e), F_FUNC);
	if (!cmp)
		return NULL;
	if (!single_value && !case_when->nrcols) {
		stmt *l = isel;
		if (!l)
			l = bin_find_smallest_column(be, left);
		case_when = stmt_const(be, l, case_when);
		if (case_when)
			case_when->cand = isel;
	}
	if (!single_value && isel && !case_when->cand) {
		case_when = stmt_project(be, isel, case_when);
		case_when->cand = isel;
	}

	/*
	 * left - isel: calls down need id's from the range of left
	 * res  - rsel: updates too res need id's in the range from res
	 */
	for (en = en->next; en; en = en->next) {
		sql_exp *e = en->data;

		next_cond = next_cond && en->next; /* last else is only a value */

		stmt *nsel = rsel;
		if (!single_value) {
			if (/*!next_cond &&*/ rsel && isel) {
				/* back into left range */
				nsel = stmt_project(be, rsel, isel);
			} else if (isel && !rsel)
				nsel = isel;
		}
		stmt *es = exp_bin(be, e, left, right, NULL, NULL, NULL, nsel, depth+1, 0, 1);

		if (!es)
			return NULL;
		if (next_cond) {
			stmt *l = case_when;
			if (!single_value) {
				if (rsel && isel) {
					assert(l->cand == isel);
					l = stmt_project(be, rsel, l);
					l->cand = nsel;
				}

				if (es->cand && !l->cand) {
					assert(es->cand == rsel);
					l = stmt_project(be, es->cand, l);
					l->cand = es->cand;
				} else if (nsel && !es->cand) {
					es = stmt_project(be, nsel, es);
					es->cand = nsel;
					if (!l->cand) {
						l = stmt_project(be, nsel, l);
						l->cand = nsel;
					}
				}
				assert(l->cand == es->cand);
			}
			es = stmt_binop(be, l, es, NULL, cmp);
		}
		if (!single_value) {
			/* create result */
			if (!res) {
				stmt *l = isel;
				if (!l)
					l = bin_find_smallest_column(be, left);
				res = stmt_const(be, l, stmt_atom(be, atom_general(be->mvc->sa, exp_subtype(fe), NULL)));
				ires = l;
				if (res)
					res->cand = isel;
			} else if (res && !next_cond) { /* use result too update column */
				stmt *val = es;
				stmt *pos = rsel;

				if (val->nrcols == 0)
					val = stmt_const(be, pos, val);
				else if (!val->cand && nsel)
					val = stmt_project(be, nsel, val);
				res = stmt_replace(be, res, pos, val);

				assert(cond);

				if (en->next) {
					/* osel - rsel */
					if (!osel)
						osel = stmt_mirror(be, ires);
					stmt *d = stmt_tdiff(be, osel, rsel, NULL);
					osel = rsel = stmt_project(be, d, osel);
				}
			}
			if (next_cond) {
				ncond = cond = es;
				if (!ncond->nrcols) {
					if (osel) {
						ncond = stmt_const(be, nsel, ncond);
						ncond->cand = nsel;
					} else if (isel) {
						ncond = stmt_const(be, isel, ncond);
						ncond->cand = isel;
					} else
						ncond = stmt_const(be, bin_find_smallest_column(be, left), ncond);
				}
				if (isel && !ncond->cand)
					ncond = stmt_project(be, nsel, ncond);
				stmt *s = stmt_uselect(be, ncond, stmt_bool(be, 1), cmp_equal, !ncond->cand?rsel:NULL, 0/*anti*/, 0);
				if (rsel && ncond->cand)
					rsel = stmt_project(be, s, rsel);
				else
					rsel = s;
			}
		} else {
			if (!res) {
				/* if_barrier ... */
				assert(next_cond);
				if (next_cond) {
					if (cond) {
						ncond = stmt_binop(be, cond, es, nsel, and);
					} else {
						ncond = es;
					}
					cond = es;
				}
			} else {
				/* var_x = s */
				(void)stmt_assign(be, NULL, nme, es, 2);
				/* endif_barrier */
				(void)stmt_control_end(be, res);
				res = NULL;

				if (en->next) {
					cond = stmt_unop(be, cond, nsel, not);

					sql_subfunc *isnull = sql_bind_func(be->mvc, "sys", "isnull", bt, NULL, F_FUNC);
					cond = stmt_binop(be, cond, stmt_unop(be, cond, nsel, isnull), nsel, or);
					if (ocond)
						cond = stmt_binop(be, ocond, cond, nsel, and);
					ocond = cond;
					if (!en->next->next)
						ncond = cond;
				}
			}
			if (ncond && (next_cond || (en->next && !en->next->next))) {
				/* if_barrier ... */
				res = stmt_cond(be, ncond, NULL, 0, 0);
			}
		}
		next_cond = !next_cond;
	}
	if (single_value)
		return stmt_var(be, NULL, nme, exp_subtype(fe), 0, 2);
	return res;
}

static stmt*
exp2bin_coalesce(backend *be, sql_exp *fe, stmt *left, stmt *right, stmt *isel, int depth)
{
	stmt *res = NULL, *rsel = NULL, *osel = NULL, *ncond = NULL, *ocond = NULL;
	int single_value = (fe->card <= CARD_ATOM && (!left || !left->nrcols));
	char name[16], *nme = NULL;
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *and = sql_bind_func(be->mvc, "sys", "and", bt, bt, F_FUNC);
	sql_subfunc *not = sql_bind_func(be->mvc, "sys", "not", bt, NULL, F_FUNC);

	if (single_value) {
		/* var_x = nil; */
		nme = number2name(name, sizeof(name), ++be->mvc->label);
		(void)stmt_var(be, NULL, nme, exp_subtype(fe), 1, 2);
	}

	list *exps = fe->l;
	for (node *en = exps->h; en; en = en->next) {
		sql_exp *e = en->data;

		stmt *nsel = rsel;
		if (!single_value) {
			if (/*!next_cond &&*/ rsel && isel) {
				/* back into left range */
				nsel = stmt_project(be, rsel, isel);
			} else if (isel && !rsel)
				nsel = isel;
		}
		stmt *es = exp_bin(be, e, left, right, NULL, NULL, NULL, nsel, depth+1, 0, 1);

		if (!es)
			return NULL;
		/* create result */
		if (!single_value) {
			if (!res) {
				stmt *l = isel;
				if (!l)
					l = bin_find_smallest_column(be, left);
				res = stmt_const(be, l, stmt_atom(be, atom_general(be->mvc->sa, exp_subtype(fe), NULL)));
				if (res)
					res->cand = isel;
			}
			if (res) {
				stmt *val = es;
				stmt *pos = rsel;

				if (en->next) {
					sql_subfunc *a = sql_bind_func(be->mvc, "sys", "isnotnull", tail_type(es), NULL, F_FUNC);
					ncond = stmt_unop(be, es, NULL, a); /*TODO sql/test/coalesce failing for passing 'nsel' here */
					if (ncond->nrcols == 0) {
						stmt *l = bin_find_smallest_column(be, left);
						if (nsel && l)
							l = stmt_project(be, nsel, l);
						ncond = stmt_const(be, l, ncond);
						if (nsel)
							ncond->cand = nsel;
					} else if (!ncond->cand && nsel)
						ncond = stmt_project(be, nsel, ncond);
					stmt *s = stmt_uselect(be, ncond, stmt_bool(be, 1), cmp_equal, NULL, 0/*anti*/, 0);
					if (!val->cand && nsel)
						val = stmt_project(be, nsel, val);
					val = stmt_project(be, s, val);
					if (osel)
						rsel = stmt_project(be, s, osel);
					else
						rsel = s;
					pos = rsel;
					val->cand = pos;
				}
				if (val->nrcols == 0)
					val = stmt_const(be, pos, val);
				else if (!val->cand && nsel)
					val = stmt_project(be, nsel, val);

				res = stmt_replace(be, res, pos, val);
			}
			if (en->next) { /* handled then part */
				stmt *s = stmt_uselect(be, ncond, stmt_bool(be, 1), cmp_equal, NULL, 1/*anti*/, 0);
				if (osel)
					rsel = stmt_project(be, s, osel);
				else
					rsel = s;
				osel = rsel;
			}
		} else {
			stmt *cond = ocond;
			if (en->next) {
				sql_subfunc *a = sql_bind_func(be->mvc, "sys", "isnotnull", tail_type(es), NULL, F_FUNC);
				ncond = stmt_unop(be, es, nsel, a);

				if (ocond)
					cond = stmt_binop(be, ocond, ncond, nsel, and);
				else
					cond = ncond;
			}

			/* if_barrier ... */
			stmt *b = stmt_cond(be, cond, NULL, 0, 0);
			/* var_x = s */
			(void)stmt_assign(be, NULL, nme, es, 2);
			/* endif_barrier */
			(void)stmt_control_end(be, b);

			cond = stmt_unop(be, ncond, nsel, not);
			if (ocond)
				ocond = stmt_binop(be, cond, ocond, nsel, and);
			else
				ocond = cond;
		}
	}
	if (single_value)
		return stmt_var(be, NULL, nme, exp_subtype(fe), 0, 2);
	return res;
}

stmt *
exp_bin(backend *be, sql_exp *e, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel, int depth, int reduce, int push)
{
	mvc *sql = be->mvc;
	stmt *s = NULL;

 	if (THRhighwater())
		return sql_error(be->mvc, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!e) {
		assert(0);
		return NULL;
	}

	switch(e->type) {
	case e_psm:
		if (e->flag & PSM_SET) {
			stmt *r = exp_bin(be, e->l, left, right, grp, ext, cnt, sel, 0, 0, push);
			if(!r)
				return NULL;
			if (e->card <= CARD_ATOM && r->nrcols > 0) /* single value, get result from bat */
				r = stmt_fetch(be, r);
			return stmt_assign(be, exp_relname(e), exp_name(e), r, GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_VAR) {
			if (e->f)
				return stmt_vars(be, exp_name(e), e->f, 1, GET_PSM_LEVEL(e->flag));
			else
				return stmt_var(be, exp_relname(e), exp_name(e), &e->tpe, 1, GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_RETURN) {
			sql_exp *l = e->l;
			stmt *r = exp_bin(be, l, left, right, grp, ext, cnt, sel, 0, 0, push);

			if (!r)
				return NULL;
			/* handle table returning functions */
			if (l->type == e_psm && l->flag & PSM_REL) {
				stmt *lst = r->op1;
				if (r->type == st_table && lst->nrcols == 0 && lst->key && e->card > CARD_ATOM) {
					node *n;
					list *l = sa_list(sql->sa);

					for(n=lst->op4.lval->h; n; n = n->next)
						list_append(l, const_column(be, (stmt*)n->data));
					r = stmt_list(be, l);
				} else if (r->type == st_table && e->card == CARD_ATOM) { /* fetch value */
					sql_rel *ll = (sql_rel*) l->l;
					r = lst->op4.lval->h->data;
					if (!r->aggr && lastexp(ll)->card > CARD_ATOM) /* if the cardinality is atom, no fetch call needed */
						r = stmt_fetch(be, r);
				}
				if (r->type == st_list)
					r = stmt_table(be, r, 1);
			}
			return stmt_return(be, r, GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_WHILE) {
			/* while is a if - block true with leave statement
	 		 * needed because the condition needs to be inside this outer block */
			stmt *ifstmt = stmt_cond(be, stmt_bool(be, 1), NULL, 0, 0);
			stmt *cond = exp_bin(be, e->l, left, right, grp, ext, cnt, sel, 0, 0, push);
			stmt *wstmt;

			if(!cond)
				return NULL;
			wstmt = stmt_cond(be, cond, ifstmt, 1, 0);

			if (!exp_list(be, e->r, left, right, grp, ext, cnt, sel))
				return NULL;
			(void)stmt_control_end(be, wstmt);
			return stmt_control_end(be, ifstmt);
		} else if (e->flag & PSM_IF) {
			stmt *cond = exp_bin(be, e->l, left, right, grp, ext, cnt, sel, 0, 0, push);
			stmt *ifstmt, *res;

			if(!cond)
				return NULL;
			ifstmt = stmt_cond(be, cond, NULL, 0, 0);
			if (!exp_list(be, e->r, left, right, grp, ext, cnt, sel))
				return NULL;
			res = stmt_control_end(be, ifstmt);
			if (e->f) {
				stmt *elsestmt = stmt_cond(be, cond, NULL, 0, 1);

				if (!exp_list(be, e->f, left, right, grp, ext, cnt, sel))
					return NULL;
				res = stmt_control_end(be, elsestmt);
			}
			return res;
		} else if (e->flag & PSM_REL) {
			sql_rel *rel = e->l;
			stmt *r = rel_bin(be, rel);

			if (!r)
				return NULL;
			if (is_modify(rel->op) || is_ddl(rel->op))
				return r;
			return stmt_table(be, r, 1);
		} else if (e->flag & PSM_EXCEPTION) {
			stmt *cond = exp_bin(be, e->l, left, right, grp, ext, cnt, sel, 0, 0, push);
			if (!cond)
				return NULL;
			return stmt_exception(be, cond, (const char *) e->r, 0);
		}
		break;
	case e_atom: {
		if (e->l) { 			/* literals */
			atom *a = e->l;
			s = stmt_atom(be, atom_dup(sql->sa, a));
		} else if (e->r) { 		/* parameters and declared variables */
			sql_var_name *vname = (sql_var_name*) e->r;
			assert(vname->name);
			s = stmt_var(be, vname->sname ? sa_strdup(sql->sa, vname->sname) : NULL, sa_strdup(sql->sa, vname->name), e->tpe.type?&e->tpe:NULL, 0, e->flag);
		} else if (e->f) { 		/* values */
			s = value_list(be, e->f, left, sel);
		} else { 			/* arguments */
			s = stmt_varnr(be, e->flag, e->tpe.type?&e->tpe:NULL);
		}
	}	break;
	case e_convert: {
		/* if input is type any NULL or column of nulls, change type */
		list *tps = e->r;
		sql_subtype *from = tps->h->data;
		sql_subtype *to = tps->h->next->data;
		stmt *l;

		if (from->type->localtype == 0) {
			l = stmt_atom(be, atom_general(sql->sa, to, NULL));
		} else {
			l = exp_bin(be, e->l, left, right, grp, ext, cnt, sel, depth+1, 0, push);
		}
		if (!l)
			return NULL;
		s = stmt_convert(be, l, (!push&&l->nrcols==0)?NULL:sel, from, to);
	} 	break;
	case e_func: {
		node *en;
		list *l = sa_list(sql->sa), *exps = e->l;
		sql_subfunc *f = e->f;
		stmt *rows = NULL;
		int push_cands = can_push_cands(sel, f);

		if (f->func->side_effect && left && left->nrcols > 0) {
			sql_subfunc *f1 = NULL;
			/* we cannot assume all SQL functions with no arguments have a correspondent with one argument, so attempt to find it. 'rand' function is the exception */
			if (list_empty(exps) && (strcmp(f->func->base.name, "rand") == 0 || (f1 = sql_find_func(sql, f->func->s ? f->func->s->base.name : NULL, f->func->base.name, 1, f->func->type, NULL)))) {
				if (f1)
					f = f1;
				list_append(l, stmt_const(be, bin_find_smallest_column(be, left),
										  stmt_atom(be, atom_general(sql->sa, f1 ? &(((sql_arg*)f1->func->ops->h->data)->type) : sql_bind_localtype("int"), NULL))));
			} else if (exps_card(exps) < CARD_MULTI) {
				rows = bin_find_smallest_column(be, left);
			}
			sql->session->status = 0; /* if the function was not found clean the error */
			sql->errstr[0] = '\0';
		}
		assert(!e->r);
		if (strcmp(sql_func_mod(f->func), "") == 0 && strcmp(sql_func_imp(f->func), "") == 0 && strcmp(f->func->base.name, "star") == 0)
			return left->op4.lval->h->data;
		else if (!list_empty(exps)) {
			unsigned nrcols = 0;

			if (strcmp(sql_func_mod(f->func), "calc") == 0 && strcmp(sql_func_imp(f->func), "ifthenelse") == 0)
				return exp2bin_case(be, e, left, right, sel, depth);
			if (strcmp(sql_func_mod(f->func), "") == 0 && strcmp(sql_func_imp(f->func), "") == 0 && strcmp(f->func->base.name, "casewhen") == 0)
				return exp2bin_casewhen(be, e, left, right, sel, depth);
			if (strcmp(sql_func_mod(f->func), "") == 0 && strcmp(sql_func_imp(f->func), "") == 0 && strcmp(f->func->base.name, "coalesce") == 0)
				return exp2bin_coalesce(be, e, left, right, sel, depth);

			assert(list_length(exps) == list_length(f->func->ops) || f->func->type == F_ANALYTIC || f->func->type == F_LOADER || f->func->vararg || f->func->varres);
			for (en = exps->h; en; en = en->next) {
				sql_exp *e = en->data;
				stmt *es = exp_bin(be, e, left, right, grp, ext, cnt, (push_cands)?sel:NULL, depth+1, 0, push);

				if (!es)
					return NULL;
				if (rows && en == exps->h && f->func->type != F_LOADER)
					es = stmt_const(be, rows, es);
				else if (f->func->type == F_ANALYTIC && es->nrcols == 0) {
					if (en == exps->h && left->nrcols)
						es = stmt_const(be, bin_find_smallest_column(be, left), es); /* ensure the first argument is a column */
					if (!f->func->s && !strcmp(f->func->base.name, "window_bound")
						&& exps->h->next && list_length(f->func->ops) == 6 && en == exps->h->next && left->nrcols)
						es = stmt_const(be, bin_find_smallest_column(be, left), es);
				}
				if (es->nrcols > nrcols)
					nrcols = es->nrcols;
				list_append(l, es);
			}
		}
		if (f->func->rel)
			s = stmt_func(be, stmt_list(be, l), sa_strdup(sql->sa, f->func->base.name), f->func->rel, (f->func->type == F_UNION));
		else
			s = stmt_Nop(be, stmt_list(be, l), sel, f);
		if (!s)
			return NULL;
	} 	break;
	case e_aggr: {
		list *attr = e->l;
		stmt *as = NULL;
		sql_subfunc *a = e->f;

		assert(sel == NULL);
		if (attr && attr->h) {
			node *en;
			list *l = sa_list(sql->sa);

			for (en = attr->h; en; en = en->next) {
				sql_exp *at = en->data;

				as = exp_bin(be, at, left, right, NULL, NULL, NULL, sel, depth+1, 0, push);

				if (as && as->nrcols <= 0 && left)
					as = stmt_const(be, bin_find_smallest_column(be, left), as);
				if (en == attr->h && !en->next && exp_aggr_is_count(e))
					as = exp_count_no_nil_arg(e, ext, at, as);
				/* insert single value into a column */
				if (as && as->nrcols <= 0 && !left)
					as = const_column(be, as);

				if (!as)
					return NULL;
				append(l, as);
			}
			if (need_distinct(e) && (grp || list_length(l) > 1)){
				list *nl = sa_list(sql->sa);
				stmt *ngrp = grp;
				stmt *next = ext;
				stmt *ncnt = cnt;
				for (en = l->h; en; en = en->next) {
					stmt *as = en->data;
					stmt *g = stmt_group(be, as, ngrp, next, ncnt, 1);
					ngrp = stmt_result(be, g, 0);
					next = stmt_result(be, g, 1);
					ncnt = stmt_result(be, g, 2);
				}
				for (en = l->h; en; en = en->next) {
					stmt *as = en->data;
					append(nl, stmt_project(be, next, as));
				}
				if (grp)
					grp = stmt_project(be, next, grp);
				l = nl;
			} else if (need_distinct(e)) {
				stmt *a = l->h->data;
				stmt *u = stmt_unique(be, a);
				l = sa_list(sql->sa);
				append(l, stmt_project(be, u, a));
			}
			as = stmt_list(be, l);
		} else {
			/* count(*) may need the default group (relation) and
			   and/or an attribute to count */
			if (grp) {
				as = grp;
			} else if (left) {
				as = bin_find_smallest_column(be, left);
				as = exp_count_no_nil_arg(e, ext, NULL, as);
			} else {
				/* create dummy single value in a column */
				as = stmt_atom_lng(be, 0);
				as = const_column(be, as);
			}
		}
		s = stmt_aggr(be, as, grp, ext, a, 1, need_no_nil(e) /* ignore nil*/, !zero_if_empty(e) );
		if (find_prop(e->p, PROP_COUNT)) /* propagate count == 0 ipv NULL in outer joins */
			s->flag |= OUTER_ZERO;
	} 	break;
	case e_column: {
		if (right) /* check relation names */
			s = bin_find_column(be, right, e->l, e->r);
		if (!s && left)
			s = bin_find_column(be, left, e->l, e->r);
		if (s && grp)
			s = stmt_project(be, ext, s);
		if (!s && right) {
			TRC_CRITICAL(SQL_EXECUTION, "Could not find %s.%s\n", (char*)e->l, (char*)e->r);
			print_stmtlist(sql->sa, left);
			print_stmtlist(sql->sa, right);
			if (!s) {
				TRC_ERROR(SQL_EXECUTION, "Query: '%s'\n", be->client->query);
			}
			assert(s);
			return NULL;
		}
	}	break;
	case e_cmp: {
		stmt *l = NULL, *r = NULL, *r2 = NULL;
		int swapped = 0, is_select = 0, oldvtop, oldstop, oldvid;
		sql_exp *re = e->r, *re2 = e->f;

		/* general predicate, select and join */
		if (e->flag == cmp_filter) {
			list *args;
			list *ops;
			node *n;
			int first = 1;

			ops = sa_list(sql->sa);
			args = e->l;
			for( n = args->h; n; n = n->next ) {
				oldvtop = be->mb->vtop;
				oldstop = be->mb->stop;
				oldvid = be->mb->vid;
				s = NULL;
				if (!swapped)
					s = exp_bin(be, n->data, left, NULL, grp, ext, cnt, NULL, depth+1, 0, push);
				if (!s && (first || swapped)) {
					clean_mal_statements(be, oldstop, oldvtop, oldvid);
					s = exp_bin(be, n->data, right, NULL, grp, ext, cnt, NULL, depth+1, 0, push);
					swapped = 1;
				}
				if (!s)
					return s;
				if (s->nrcols == 0 && first)
					s = stmt_const(be, bin_find_smallest_column(be, swapped?right:left), s);
				list_append(ops, s);
				first = 0;
			}
			l = stmt_list(be, ops);
			ops = sa_list(sql->sa);
			args = e->r;
			for( n = args->h; n; n = n->next ) {
				s = exp_bin(be, n->data, (swapped || !right)?left:right, NULL, grp, ext, cnt, NULL, depth+1, 0, push);
				if (!s)
					return s;
				list_append(ops, s);
			}
			r = stmt_list(be, ops);

			if (left && right && (exps_card(e->r) != CARD_ATOM || !exps_are_atoms(e->r))) {
				sql_subfunc *f = e->f;
				for (node *n = l->op4.lval->h ; n ; n = n->next)
					n->data = column(be, n->data);
				for (node *n = r->op4.lval->h ; n ; n = n->next)
					n->data = column(be, n->data);
				return stmt_genjoin(be, l, r, f, is_anti(e), swapped);
			}
			assert(!swapped);
			s = stmt_genselect(be, l, r, e->f, sel, is_anti(e));
			return s;
		}
		if (e->flag == cmp_in || e->flag == cmp_notin) {
			return handle_in_exps(be, e->l, e->r, left, right, grp, ext, cnt, sel, (e->flag == cmp_in), 0, depth, reduce);
		}
		if (e->flag == cmp_or && (!right || right->nrcols == 1))
			return exp_bin_or(be, e, left, right, grp, ext, cnt, sel, depth, reduce, push);
		if (e->flag == cmp_or && right) {  /* join */
			assert(0);
		}

		/* mark use of join indices */
		if (right && find_prop(e->p, PROP_JOINIDX) != NULL)
			be->join_idx++;

		oldvtop = be->mb->vtop;
		oldstop = be->mb->stop;
		oldvid = be->mb->vid;
		if (!l) {
			l = exp_bin(be, e->l, left, (!reduce)?right:NULL, grp, ext, cnt, sel, depth+1, 0, push);
			swapped = 0;
		}
		if (!l && right) {
			clean_mal_statements(be, oldstop, oldvtop, oldvid);
 			l = exp_bin(be, e->l, right, NULL, grp, ext, cnt, sel, depth+1, 0, push);
			swapped = 1;
		}

		oldvtop = be->mb->vtop;
		oldstop = be->mb->stop;
		oldvid = be->mb->vid;
		if (swapped || !right || !reduce)
 			r = exp_bin(be, re, left, (!reduce)?right:NULL, grp, ext, cnt, sel, depth+1, 0, push);
		else
 			r = exp_bin(be, re, right, NULL, grp, ext, cnt, sel, depth+1, 0, push);
		if (!r && !swapped) {
			clean_mal_statements(be, oldstop, oldvtop, oldvid);
 			r = exp_bin(be, re, left, NULL, grp, ext, cnt, sel, depth+1, 0, push);
			is_select = 1;
		}
		if (!r && swapped) {
			clean_mal_statements(be, oldstop, oldvtop, oldvid);
 			r = exp_bin(be, re, right, NULL, grp, ext, cnt, sel, depth+1, 0, push);
			is_select = 1;
		}
		if (re2 && (swapped || !right || !reduce))
 			r2 = exp_bin(be, re2, left, (!reduce)?right:NULL, grp, ext, cnt, sel, depth+1, 0, push);
		else if (re2)
 			r2 = exp_bin(be, re2, right, NULL, grp, ext, cnt, sel, depth+1, 0, push);

		if (!l || !r || (re2 && !r2))
			return NULL;

		(void)is_select;
		if (reduce && left && right) {
			if (l->nrcols == 0)
				l = stmt_const(be, bin_find_smallest_column(be, swapped?right:left), l);
			if (r->nrcols == 0)
				r = stmt_const(be, bin_find_smallest_column(be, swapped?left:right), r);
			if (r2 && r2->nrcols == 0)
				r2 = stmt_const(be, bin_find_smallest_column(be, swapped?left:right), r2);
			if (r2) {
				s = stmt_join2(be, l, r, r2, (comp_type)e->flag, is_anti(e), swapped);
			} else if (swapped) {
				s = stmt_join(be, r, l, is_anti(e), swap_compare((comp_type)e->flag), 0, is_semantics(e), false);
			} else {
				s = stmt_join(be, l, r, is_anti(e), (comp_type)e->flag, 0, is_semantics(e), false);
			}
		} else {
			if (r2) { /* handle all cases in stmt_uselect, reducing, non reducing, scalar etc */
				if (l->nrcols == 0 && ((sel && sel->nrcols > 0) || r->nrcols > 0 || r2->nrcols > 0 || reduce))
					l = left ? stmt_const(be, bin_find_smallest_column(be, left), l) : column(be, l);
				s = stmt_uselect2(be, l, r, r2, (comp_type)e->flag, sel, is_anti(e), reduce);
			} else {
				/* value compare or select */
				if ((!reduce || (l->nrcols == 0 && r->nrcols == 0)) && (e->flag == mark_in || e->flag == mark_notin)) {
					int in_flag = e->flag==mark_in?1:0;
					if (e->anti)
						in_flag = !in_flag;
					sql_subfunc *f = sql_bind_func(sql, "sys", in_flag?"=":"<>", tail_type(l), tail_type(l), F_FUNC);
					assert(f);
					s = stmt_binop(be, l, r, sel, f);
					if (l->cand)
						s->cand = l->cand;
					if (r->cand)
						s->cand = r->cand;
				} else if (!reduce || (l->nrcols == 0 && r->nrcols == 0)) {
					sql_subfunc *f = sql_bind_func(sql, "sys", compare_func((comp_type)e->flag, is_anti(e)),
												   tail_type(l), tail_type(l), F_FUNC);
					assert(f);
					if (is_semantics(e)) {
						if (exp_is_null(e->l) && exp_is_null(e->r)) {
							s = stmt_bool(be, !is_anti(e));
						} else {
							list *args = sa_list(sql->sa);
							/* add nil semantics bit */
							list_append(args, l);
							list_append(args, r);
							list_append(args, stmt_bool(be, 1));
							s = stmt_Nop(be, stmt_list(be, args), sel, f);
						}
					} else {
						s = stmt_binop(be, l, r, sel, f);
					}
					if (l->cand)
						s->cand = l->cand;
					if (r->cand)
						s->cand = r->cand;
				} else {
					/* this can still be a join (as relational algebra and single value subquery results still means joins */
					s = stmt_uselect(be, l, r, (comp_type)e->flag, sel, is_anti(e), is_semantics(e));
				}
			}
		}
	 }	break;
	default:
		;
	}
	return s;
}

static stmt *
stmt_col( backend *be, sql_column *c, stmt *del, int part)
{
	stmt *sc = stmt_bat(be, c, RDONLY, part);

	if (isTable(c->t) && c->t->access != TABLE_READONLY &&
	   (!isNew(c) || !isNew(c->t) /* alter */) &&
	   (c->t->persistence == SQL_PERSIST || c->t->s) /*&& !c->t->commit_action*/) {
		stmt *u = stmt_bat(be, c, RD_UPD_ID, part);
		sc = stmt_project_delta(be, sc, u);
		if (del)
			sc = stmt_project(be, del, sc);
	} else if (del) { /* always handle the deletes */
		sc = stmt_project(be, del, sc);
	}
	return sc;
}

static stmt *
stmt_idx( backend *be, sql_idx *i, stmt *del, int part)
{
	stmt *sc = stmt_idxbat(be, i, RDONLY, part);

	if (isTable(i->t) && i->t->access != TABLE_READONLY &&
	   (!isNew(i) || !isNew(i->t) /* alter */) &&
	   (i->t->persistence == SQL_PERSIST || i->t->s) /*&& !i->t->commit_action*/) {
		stmt *u = stmt_idxbat(be, i, RD_UPD_ID, part);
		sc = stmt_project_delta(be, sc, u);
		if (del)
			sc = stmt_project(be, del, sc);
	} else if (del) { /* always handle the deletes */
		sc = stmt_project(be, del, sc);
	}
	return sc;
}

static int
stmt_set_type_param(mvc *sql, sql_subtype *type, stmt *param)
{
	if (!type || !param || param->type != st_var)
		return -1;

	if (set_type_param(sql, type, param->flag) == 0) {
		param->op4.typeval = *type;
		return 0;
	}
	return -1;
}

/* check_types tries to match the ct type with the type of s if they don't
 * match s is converted. Returns NULL on failure.
 */
static stmt *
check_types(backend *be, sql_subtype *ct, stmt *s, check_type tpe)
{
	mvc *sql = be->mvc;
	int c = 0;
	sql_subtype *t = NULL, *st = NULL;

 	st = tail_type(s);
	if ((!st || !st->type) && stmt_set_type_param(sql, ct, s) == 0) {
		return s;
	} else if (!st) {
		return sql_error(sql, 02, SQLSTATE(42000) "statement has no type information");
	}

	/* check if the types are the same */
	if (t && subtype_cmp(t, ct) != 0) {
		t = NULL;
	}

	if (!t) {	/* try to convert if needed */
		if (EC_INTERVAL(st->type->eclass) && (ct->type->eclass == EC_NUM || ct->type->eclass == EC_POS) && ct->digits < st->digits) {
			s = NULL; /* conversion from interval to num depends on the number of digits */
		} else {
			c = sql_type_convert(st->type->eclass, ct->type->eclass);
			if (!c || (c == 2 && tpe == type_set) || (c == 3 && tpe != type_cast)) {
				s = NULL;
			} else {
				s = stmt_convert(be, s, NULL, st, ct);
			}
		}
	}
	if (!s) {
		stmt *res = sql_error(
			sql, 03,
			SQLSTATE(42000) "types %s(%u,%u) (%s) and %s(%u,%u) (%s) are not equal",
			st->type->sqlname,
			st->digits,
			st->scale,
			st->type->base.name,
			ct->type->sqlname,
			ct->digits,
			ct->scale,
			ct->type->base.name
		);
		return res;
	}
	return s;
}

static stmt *
sql_Nop_(backend *be, const char *fname, stmt *a1, stmt *a2, stmt *a3, stmt *a4)
{
	mvc *sql = be->mvc;
	list *sl = sa_list(sql->sa);
	list *tl = sa_list(sql->sa);
	sql_subfunc *f = NULL;

	list_append(sl, a1);
	list_append(tl, tail_type(a1));
	list_append(sl, a2);
	list_append(tl, tail_type(a2));
	list_append(sl, a3);
	list_append(tl, tail_type(a3));
	if (a4) {
		list_append(sl, a4);
		list_append(tl, tail_type(a4));
	}

	if ((f = sql_bind_func_(sql, "sys", fname, tl, F_FUNC)))
		return stmt_Nop(be, stmt_list(be, sl), NULL, f);
	return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: no such operator '%s'", fname);
}

static stmt *
parse_value(backend *be, char *query, sql_subtype *tpe, char emode)
{
	sql_exp *e = rel_parse_val(be->mvc, query, tpe, emode, NULL);
	if (e)
		return exp_bin(be, e, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	return sql_error(be->mvc, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
}

static stmt *
stmt_rename(backend *be, sql_exp *exp, stmt *s )
{
	const char *name = exp_name(exp);
	const char *rname = exp_relname(exp);
	stmt *o = s;

	if (!name && exp_is_atom(exp))
		name = sa_strdup(be->mvc->sa, "single_value");
	assert(name);
	s = stmt_alias(be, s, rname, name);
	if (o->flag & OUTER_ZERO)
		s->flag |= OUTER_ZERO;
	return s;
}

static stmt *
rel2bin_sql_table(backend *be, sql_table *t, list *aliases)
{
	mvc *sql = be->mvc;
	list *l = sa_list(sql->sa);
	node *n;
	stmt *dels = stmt_tid(be, t, 0);

	if (aliases) {
		for (n = aliases->h; n; n = n->next) {
			sql_exp *e = n->data;
			if (e->type != e_column)
				continue;
			assert(e->type == e_column);
			char *name = e->r;
			if (name[0] == '%') {
				if (strcmp(name, TID)==0) {
					/* tid function  sql.tid(t) */
					const char *rnme = t->base.name;

					stmt *sc = dels?dels:stmt_tid(be, t, 0);
					sc = stmt_alias(be, sc, rnme, TID);
					list_append(l, sc);
				} else {
					node *m = ol_find_name(t->idxs, name+1);
					if (!m)
						assert(0);
					sql_idx *i = m->data;
					stmt *sc = stmt_idx(be, i, dels, dels->partition);
					const char *rnme = t->base.name;

					/* index names are prefixed, to make them independent */
					sc = stmt_alias(be, sc, rnme, sa_strconcat(sql->sa, "%", i->base.name));
					list_append(l, sc);
				}
			} else {
				node *m = ol_find_name(t->columns, name);
				if (!m)
					assert(0);
				sql_column *c = m->data;
				stmt *sc = stmt_col(be, c, dels, dels->partition);
				list_append(l, sc);
			}
		}
	} else {
		for (n = ol_first_node(t->columns); n; n = n->next) {
			sql_column *c = n->data;
			stmt *sc = stmt_col(be, c, dels, dels->partition);

			list_append(l, sc);
		}
		/* TID column */
		if (ol_first_node(t->columns)) {
			/* tid function  sql.tid(t) */
			const char *rnme = t->base.name;

			stmt *sc = dels?dels:stmt_tid(be, t, 0);
			sc = stmt_alias(be, sc, rnme, TID);
			list_append(l, sc);
		}
		if (t->idxs) {
			for (n = ol_first_node(t->idxs); n; n = n->next) {
				sql_idx *i = n->data;
				stmt *sc = stmt_idx(be, i, dels, dels->partition);
				const char *rnme = t->base.name;

				/* index names are prefixed, to make them independent */
				sc = stmt_alias(be, sc, rnme, sa_strconcat(sql->sa, "%", i->base.name));
				list_append(l, sc);
			}
		}
	}
	return stmt_list(be, l);
}

static stmt *
rel2bin_basetable(backend *be, sql_rel *rel)
{
	mvc *sql = be->mvc;
	sql_table *t = rel->l;
	sql_column *fcol = NULL;
	sql_idx *fi = NULL;
	list *l = sa_list(sql->sa);
	stmt *dels = stmt_tid(be, t, rel->flag == REL_PARTITION), *col = NULL;
	node *en;

	/* add aliases */
	assert(rel->exps);
	for( en = rel->exps->h; en && !col; en = en->next ) {
		sql_exp *exp = en->data;
		const char *oname = exp->r;

		if (is_func(exp->type) || (oname[0] == '%' && strcmp(oname, TID) == 0))
			continue;
		if (oname[0] == '%') {
			sql_idx *i = find_sql_idx(t, oname+1);

			/* do not include empty indices in the plan */
			if ((hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
				continue;
			fi = i;
			col = stmt_idx(be, i, NULL/*dels*/, dels->partition);
		} else {
			sql_column *c = find_sql_column(t, oname);

			fcol = c;
			col = stmt_col(be, c, NULL/*dels*/, dels->partition);
		}
	}
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		const char *rname = exp_relname(exp)?exp_relname(exp):exp->l;
		const char *oname = exp->r;
		stmt *s = NULL;

		assert(!is_func(exp->type));
		if (oname[0] == '%' && strcmp(oname, TID) == 0) {
			/* tid function  sql.tid(t) */
			const char *rnme = t->base.name;

			if (col)
				s = stmt_mirror(be, col);
			else {
				s = dels?dels:stmt_tid(be, t, 0);
				dels = NULL;
			}
			s = stmt_alias(be, s, rnme, TID);
		} else if (oname[0] == '%') {
			sql_idx *i = find_sql_idx(t, oname+1);

			/* do not include empty indices in the plan */
			if ((hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
				continue;
			s = (i == fi) ? col : stmt_idx(be, i, NULL/*dels*/, dels->partition);
		} else {
			sql_column *c = find_sql_column(t, oname);

			s = (c == fcol) ? col : stmt_col(be, c, NULL/*dels*/, dels->partition);
		}
		s->tname = rname;
		s->cname = exp_name(exp);
		list_append(l, s);
	}
	stmt *res = stmt_list(be, l);
	if (res && dels)
		res->cand = dels;
	return res;
}

static int
alias_cmp( stmt *s, const char *nme )
{
	return strcmp(s->cname, nme);
}

static list* exps2bin_args(backend *be, list *exps, list *args);

static list *
exp2bin_args(backend *be, sql_exp *e, list *args)
{
	mvc *sql = be->mvc;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!e || !args)
		return args;
	switch(e->type){
	case e_column:
	case e_psm:
		return args;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			args = exps2bin_args(be, e->l, args);
			args = exps2bin_args(be, e->r, args);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			args = exp2bin_args(be, e->l, args);
			args = exps2bin_args(be, e->r, args);
		} else {
			args = exp2bin_args(be, e->l, args);
			args = exp2bin_args(be, e->r, args);
			if (e->f)
				args = exp2bin_args(be, e->f, args);
		}
		return args;
	case e_convert:
		if (e->l)
			return exp2bin_args(be, e->l, args);
		break;
	case e_aggr:
	case e_func:
		if (e->l)
			return exps2bin_args(be, e->l, args);
		break;
	case e_atom:
		if (e->l) {
			return args;
		} else if (e->f) {
			return exps2bin_args(be, e->f, args);
		} else if (e->r) {
			char *nme;
			sql_var_name *vname = (sql_var_name*) e->r;

			if (vname->sname) { /* Global variable */
				nme = SA_NEW_ARRAY(be->mvc->sa, char, strlen(vname->sname) + strlen(vname->name) + 5);
				if (!nme)
					return NULL;
				stpcpy(stpcpy(stpcpy(stpcpy(nme, "A0%"), vname->sname), "%"), vname->name); /* mangle variable name */
			} else { /* Parameter or local variable */
				char levelstr[16];
				snprintf(levelstr, sizeof(levelstr), "%u", e->flag);
				nme = SA_NEW_ARRAY(be->mvc->sa, char, strlen(levelstr) + strlen(vname->name) + 3);
				if (!nme)
					return NULL;
				stpcpy(stpcpy(stpcpy(stpcpy(nme, "A"), levelstr), "%"), vname->name); /* mangle variable name */
			}
			if (!list_find(args, nme, (fcmp)&alias_cmp)) {
				stmt *s = stmt_var(be, vname->sname, vname->name, &e->tpe, 0, 0);

				s = stmt_alias(be, s, NULL, sa_strdup(sql->sa, nme));
				list_append(args, s);
			}
		}
	}
	return args;
}

static list *
exps2bin_args(backend *be, list *exps, list *args)
{
	node *n;

	if (!exps)
		return args;
	for (n = exps->h; n; n = n->next)
		args = exp2bin_args(be, n->data, args);
	return args;
}

static list *
rel2bin_args(backend *be, sql_rel *rel, list *args)
{
	if (THRhighwater())
		return sql_error(be->mvc, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!rel || !args)
		return args;
	switch(rel->op) {
	case op_basetable:
	case op_table:
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:

	case op_semi:
	case op_anti:

	case op_union:
	case op_inter:
	case op_except:
		args = rel2bin_args(be, rel->l, args);
		args = rel2bin_args(be, rel->r, args);
		break;
	case op_groupby:
		if (rel->r)
			args = exps2bin_args(be, rel->r, args);
		/* fall through */
	case op_project:
	case op_select:
	case op_topn:
	case op_sample:
		if (rel->exps)
			args = exps2bin_args(be, rel->exps, args);
		args = rel2bin_args(be, rel->l, args);
		break;
	case op_ddl:
		args = rel2bin_args(be, rel->l, args);
		if (rel->r)
			args = rel2bin_args(be, rel->r, args);
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
		args = rel2bin_args(be, rel->r, args);
		break;
	}
	return args;
}

typedef struct trigger_input {
	sql_table *t;
	stmt *tids;
	stmt **updates;
	int type; /* insert 1, update 2, delete 3, truncate 4 */
	const char *on;
	const char *nn;
} trigger_input;

static stmt *
rel2bin_table(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l;
	stmt *sub = NULL, *osub = NULL;
	node *en, *n;
	sql_exp *op = rel->r;

	if (rel->flag == TRIGGER_WRAPPER) {
		trigger_input *ti = rel->l;
		l = sa_list(sql->sa);

		for(n = ol_first_node(ti->t->columns); n; n = n->next) {
			sql_column *c = n->data;

			if (ti->type == 2) { /* updates */
				stmt *s = stmt_col(be, c, ti->tids, ti->tids->partition);
				append(l, stmt_alias(be, s, ti->on, c->base.name));
			}
			if (ti->updates && ti->updates[c->colnr]) {
				append(l, stmt_alias(be, ti->updates[c->colnr], ti->nn, c->base.name));
			} else {
				stmt *s = stmt_col(be, c, ti->tids, ti->tids->partition);
				append(l, stmt_alias(be, s, ti->nn, c->base.name));
				assert(ti->type != 1);
			}
		}
		sub = stmt_list(be, l);
		return sub;
	} else if (op) {
		int i;
		sql_subfunc *f = op->f;
		stmt *psub = NULL;
		list *ops = NULL;
		stmt *ids = NULL;

		if (rel->l) { /* first construct the sub relation */
			sql_rel *l = rel->l;
			if (l->op == op_ddl) {
				sql_table *t = rel_ddl_table_get(l);

				if (t)
					sub = rel2bin_sql_table(be, t, NULL);
			} else {
				sub = subrel_bin(be, rel->l, refs);
			}
			sub = subrel_project(be, sub, refs, rel->l);
			if (!sub)
				return NULL;
		}

		assert(f);
		if (f->func->res && list_length(f->func->res) + 1 == list_length(rel->exps) && !f->func->varres) {
			/* add inputs in correct order ie loop through args of f and pass column */
			list *exps = op->l;
			ops = sa_list(be->mvc->sa);
			if (exps) {
				for (node *en = exps->h; en; en = en->next) {
					sql_exp *e = en->data;

					/* find column */
					stmt *s = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
					if (!s)
						return NULL;
					if (en->next)
						append(ops, s);
					else /* last added exp is the ids (todo use name base lookup !!) */
						ids = s;
				}
			}
		} else {
			psub = exp_bin(be, op, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0); /* table function */
			if (!psub)
				return NULL;
		}
		l = sa_list(sql->sa);
		if (f->func->res) {
			if (f->func->varres) {
				for(i=0, en = rel->exps->h, n = f->res->h; en; en = en->next, n = n->next, i++ ) {
					sql_exp *exp = en->data;
					sql_subtype *st = n->data;
					const char *rnme = exp_relname(exp)?exp_relname(exp):exp->l;
					stmt *s = stmt_rs_column(be, psub, i, st);

					s = stmt_alias(be, s, rnme, exp_name(exp));
					list_append(l, s);
				}
			} else {
				node *m = rel->exps->h;
				int i = 0;

				/* correlated table returning function */
				if (list_length(f->func->res) + 1 == list_length(rel->exps)) {
					/* use a simple nested loop solution for this case, ie
					 * output a table of (input) row-ids, the output of the table producing function
					 */
					/* make sure the input for sql.unionfunc are bats */
					if (ids)
						ids = column(be, ids);
					if (ops)
						for (node *en = ops->h; en; en = en->next)
							en->data = column(be, (stmt *) en->data);

					int narg = 3 + list_length(rel->exps);
					if (ops)
						narg += list_length(ops);
					InstrPtr q = newStmtArgs(be->mb, sqlRef, "unionfunc", narg);
					/* Generate output rowid column and output of function f */
					for(i=0; m; m = m->next, i++) {
						sql_exp *e = m->data;
						int type = exp_subtype(e)->type->localtype;

						type = newBatType(type);
						if (i)
							q = pushReturn(be->mb, q, newTmpVariable(be->mb, type));
						else
							getArg(q, 0) = newTmpVariable(be->mb, type);
					}
					str mod = sql_func_mod(f->func);
					str fcn = sql_func_imp(f->func);
					q = pushStr(be->mb, q, mod);
					q = pushStr(be->mb, q, fcn);
					if (backend_create_func(be, f->func, NULL, ops) < 0)
		 				return NULL;
					psub = stmt_direct_func(be, q);

					if (ids) /* push input rowids column */
						q = pushArgument(be->mb, q, ids->nr);

					/* add inputs in correct order ie loop through args of f and pass column */
					if (ops) {
						for (node *en = ops->h; en; en = en->next) {
							stmt *op = en->data;

							q = pushArgument(be->mb, q, op->nr);
						}
					}

					/* name output of dependent columns, output of function is handled the same as without correlation */
					int len = list_length(rel->exps)-list_length(f->func->res);
					assert(len== 1);
					for(i=0, m=rel->exps->h; m && i<len; m = m->next, i++ ) {
						sql_exp *exp = m->data;
						stmt *s = stmt_rs_column(be, psub, i, exp_subtype(exp));

						s = stmt_alias(be, s, exp->l, exp->r);
						list_append(l, s);
					}
				}
				for(n = f->func->res->h; n && m; n = n->next, m = m->next, i++ ) {
					sql_arg *a = n->data;
					sql_exp *exp = m->data;
					stmt *s = stmt_rs_column(be, psub, i, &a->type);
					const char *rnme = exp_relname(exp)?exp_relname(exp):exp_find_rel_name(op);

					s = stmt_alias(be, s, rnme, a->name);
					list_append(l, s);
				}
#if 0
				if (list_length(f->res) == list_length(f->func->res) + 1) {
					assert(0);
					/* add missing %TID% column */
					sql_subtype *t = f->res->t->data;
					stmt *s = stmt_rs_column(be, psub, i, t);
					const char *rnme = exp_find_rel_name(op);

					s = stmt_alias(be, s, rnme, TID);
					list_append(l, s);
				}
#endif
			}
		}
		assert(rel->flag != TABLE_PROD_FUNC || !sub || !(sub->nrcols));
		sub = stmt_list(be, l);
	} else if (rel->l) { /* handle sub query via function */
		int i;
		char name[16], *nme;
		sql_rel *fr;

		nme = number2name(name, sizeof(name), ++be->remote);

		l = rel2bin_args(be, rel->l, sa_list(sql->sa));
		if (!l)
			return NULL;
		sub = stmt_list(be, l);
		if (!(sub = stmt_func(be, sub, sa_strdup(sql->sa, nme), rel->l, 0)))
			return NULL;
		fr = rel->l;
		l = sa_list(sql->sa);
		for(i = 0, n = rel->exps->h; n; n = n->next, i++ ) {
			sql_exp *c = n->data;
			stmt *s = stmt_rs_column(be, sub, i, exp_subtype(c));
			const char *nme = exp_name(c);
			const char *rnme = exp_relname(c);

			s = stmt_alias(be, s, rnme, nme);
			if (fr->card <= CARD_ATOM) /* single value, get result from bat */
				s = stmt_fetch(be, s);
			list_append(l, s);
		}
		sub = stmt_list(be, l);
	}
	if (!sub) {
		assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
		return NULL;
	}
	l = sa_list(sql->sa);
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		const char *rnme = exp_relname(exp)?exp_relname(exp):exp->l;
		stmt *s = bin_find_column(be, sub, exp->l, exp->r);

		if (!s) {
			assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
			return NULL;
		}
		if (sub && sub->nrcols >= 1 && s->nrcols == 0)
			s = stmt_const(be, bin_find_smallest_column(be, sub), s);
		s = stmt_alias(be, s, rnme, exp_name(exp));
		list_append(l, s);
	}
	if (osub && osub->nrcols)
		list_merge(l, osub->op4.lval, NULL);
	sub = stmt_list(be, l);
	return sub;
}

static stmt *
rel2bin_hash_lookup(backend *be, sql_rel *rel, stmt *left, stmt *right, sql_idx *i, node *en )
{
	mvc *sql = be->mvc;
	node *n;
	sql_subtype *it = sql_bind_localtype("int");
	sql_subtype *lng = sql_bind_localtype("lng");
	stmt *h = NULL;
	stmt *bits = stmt_atom_int(be, 1 + ((sizeof(lng)*8)-1)/(list_length(i->columns)+1));
	sql_exp *e = en->data;
	sql_exp *l = e->l;
	stmt *idx = bin_find_column(be, left, l->l, sa_strconcat(sql->sa, "%", i->base.name));
	int swap_exp = 0, swap_rel = 0, semantics = 0;

	if (!idx) {
		swap_exp = 1;
		l = e->r;
		idx = bin_find_column(be, left, l->l, sa_strconcat(sql->sa, "%", i->base.name));
	}
	if (!idx && right) {
		swap_exp = 0;
		swap_rel = 1;
		l = e->l;
		idx = bin_find_column(be, right, l->l, sa_strconcat(sql->sa, "%", i->base.name));
	}
	if (!idx && right) {
		swap_exp = 1;
		swap_rel = 1;
		l = e->r;
		idx = bin_find_column(be, right, l->l, sa_strconcat(sql->sa, "%", i->base.name));
	}
	if (!idx)
		return NULL;
	/* should be in key order! */
	for( en = rel->exps->h, n = i->columns->h; en && n; en = en->next, n = n->next ) {
		sql_exp *e = en->data;
		stmt *s = NULL;

		if (e->type == e_cmp && e->flag == cmp_equal) {
			sql_exp *ee = (swap_exp)?e->l:e->r;
			if (swap_rel)
				s = exp_bin(be, ee, left, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
			else
				s = exp_bin(be, ee, right, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		}

		if (!s)
			return NULL;
		if (h) {
			sql_subfunc *xor = sql_bind_func_result(sql, "sys", "rotate_xor_hash", F_FUNC, lng, 3, lng, it, tail_type(s));

			h = stmt_Nop(be, stmt_list(be, list_append( list_append(
				list_append(sa_list(sql->sa), h), bits), s)), NULL, xor);
			semantics = 1;
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, lng, 1, tail_type(s));

			h = stmt_unop(be, s, NULL, hf);
		}
	}
	if (h && h->nrcols) {
		if (!swap_rel) {
			return stmt_join(be, idx, h, 0, cmp_equal, 0, semantics, false);
		} else {
			return stmt_join(be, h, idx, 0, cmp_equal, 0, semantics, false);
		}
	} else {
		return stmt_uselect(be, idx, h, cmp_equal, NULL, 0, semantics);
	}
}

static stmt *
join_hash_key( backend *be, list *l )
{
	mvc *sql = be->mvc;
	node *m;
	sql_subtype *it, *lng;
	stmt *h = NULL;
	stmt *bits = stmt_atom_int(be, 1 + ((sizeof(lng)*8)-1)/(list_length(l)+1));

	it = sql_bind_localtype("int");
	lng = sql_bind_localtype("lng");
	for (m = l->h; m; m = m->next) {
		stmt *s = m->data;

		if (h) {
			sql_subfunc *xor = sql_bind_func_result(sql, "sys", "rotate_xor_hash", F_FUNC, lng, 3, lng, it, tail_type(s));

			h = stmt_Nop(be, stmt_list(be, list_append( list_append( list_append(sa_list(sql->sa), h), bits), s )), NULL, xor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, lng, 1, tail_type(s));
			h = stmt_unop(be, s, NULL, hf);
		}
	}
	return h;
}

static stmt *
releqjoin( backend *be, list *l1, list *l2, list *exps, int used_hash, int need_left, int is_semantics )
{
	mvc *sql = be->mvc;
	node *n1 = l1->h, *n2 = l2->h, *n3 = NULL;
	stmt *l, *r, *res;
	sql_exp *e;

	if (exps)
		n3 = exps->h;
	if (list_length(l1) <= 1) {
		l = l1->h->data;
		r = l2->h->data;
		if (!is_semantics && exps) {
			e = n3->data;
			is_semantics = is_semantics(e);
		}
		r =  stmt_join(be, l, r, 0, cmp_equal, need_left, is_semantics, false);
		return r;
	}
	if (used_hash) {
		l = n1->data;
		r = n2->data;
		n1 = n1->next;
		n2 = n2->next;
		n3 = n3?n3->next:NULL;
		res = stmt_join(be, l, r, 0, cmp_equal, need_left, 1, false);
	} else { /* need hash */
		l = join_hash_key(be, l1);
		r = join_hash_key(be, l2);
		res = stmt_join(be, l, r, 0, cmp_equal, need_left, 1, false);
	}
	l = stmt_result(be, res, 0);
	r = stmt_result(be, res, 1);
	for (; n1 && n2; n1 = n1->next, n2 = n2->next, n3 = n3?n3->next:NULL) {
		int semantics = is_semantics;
		stmt *ld = n1->data;
		stmt *rd = n2->data;
		stmt *le = stmt_project(be, l, ld );
		stmt *re = stmt_project(be, r, rd );
		/* intentional both tail_type's of le (as re sometimes is a find for bulk loading */
		sql_subfunc *f = NULL;
		stmt * cmp;
		list *ops;

		f = sql_bind_func(sql, "sys", "=", tail_type(le), tail_type(le), F_FUNC);
		assert(f);

		ops = sa_list(be->mvc->sa);
		list_append(ops, le);
		list_append(ops, re);
		if (!semantics && exps) {
			e = n3->data;
			semantics = is_semantics(e);
		}
		if (semantics)
			list_append(ops, stmt_bool(be, 1));
		cmp = stmt_Nop(be, stmt_list(be, ops), NULL, f);
		cmp = stmt_uselect(be, cmp, stmt_bool(be, 1), cmp_equal, NULL, 0, 0);
		l = stmt_project(be, cmp, l );
		r = stmt_project(be, cmp, r );
	}
	res = stmt_join(be, l, r, 0, cmp_joined, 0, 0, false);
	return res;
}

static void
split_join_exps(sql_rel *rel, list *joinable, list *not_joinable)
{
	if (!list_empty(rel->exps)) {
		for (node *n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			int left_reference = 0, right_reference = 0;

			/* we can handle thetajoins, rangejoins and filter joins (like) */
			/* ToDo how about atom expressions? */
			if (e->type == e_cmp) {
				int flag = e->flag & ~CMP_BETWEEN;
				/* check if its a select or join expression, ie use only expressions of one relation left and of the other right (than join) */
				if (flag < cmp_filter || flag == mark_in || flag == mark_notin) { /* theta and range joins */
					/* join or select ? */
					sql_exp *l = e->l, *r = e->r, *f = e->f;

					if (f) {
						int ll = rel_find_exp(rel->l, l) != NULL;
						int rl = rel_find_exp(rel->r, l) != NULL;
						int lr = rel_find_exp(rel->l, r) != NULL;
						int rr = rel_find_exp(rel->r, r) != NULL;
						int lf = rel_find_exp(rel->l, f) != NULL;
						int rf = rel_find_exp(rel->r, f) != NULL;
						int nrcr1 = 0, nrcr2 = 0, nrcl1 = 0, nrcl2 = 0;

						if ((ll && !rl &&
						   ((rr && !lr) || (nrcr1 = r->card == CARD_ATOM && exp_is_atom(r))) &&
						   ((rf && !lf) || (nrcr2 = f->card == CARD_ATOM && exp_is_atom(f))) && (nrcr1+nrcr2) <= 1) ||
						    (rl && !ll &&
						   ((lr && !rr) || (nrcl1 = r->card == CARD_ATOM && exp_is_atom(r))) &&
						   ((lf && !rf) || (nrcl2 = f->card == CARD_ATOM && exp_is_atom(f))) && (nrcl1+nrcl2) <= 1)) {
							left_reference = right_reference = 1;
						}
					} else {
						if (l->card != CARD_ATOM || !exp_is_atom(l)) {
							left_reference |= rel_find_exp(rel->l, l) != NULL;
							right_reference |= rel_find_exp(rel->r, l) != NULL;
						}
						if (r->card != CARD_ATOM || !exp_is_atom(r)) {
							left_reference |= rel_find_exp(rel->l, r) != NULL;
							right_reference |= rel_find_exp(rel->r, r) != NULL;
						}
					}
				} else if (flag == cmp_filter) {
					list *l = e->l, *r = e->r;
					int ll = 0, lr = 0, rl = 0, rr = 0;

					for (node *n = l->h ; n ; n = n->next) {
						sql_exp *ee = n->data;

						if (ee->card != CARD_ATOM || !exp_is_atom(ee)) {
							ll |= rel_find_exp(rel->l, ee) != NULL;
							rl |= rel_find_exp(rel->r, ee) != NULL;
						}
					}
					for (node *n = r->h ; n ; n = n->next) {
						sql_exp *ee = n->data;

						if (ee->card != CARD_ATOM || !exp_is_atom(ee)) {
							lr |= rel_find_exp(rel->l, ee) != NULL;
							rr |= rel_find_exp(rel->r, ee) != NULL;
						}
					}
					if ((ll && !lr && !rl && rr) || (!ll && lr && rl && !rr))
						right_reference = left_reference = 1;
				}
			}
			if (left_reference && right_reference) {
				append(joinable, e);
			} else {
				append(not_joinable, e);
			}
		}
	}
}

#define is_equi_exp(e) ((e)->flag == cmp_equal || (e)->flag == mark_in || (e)->flag == mark_notin)
#define is_equi_exp_(e) ((e)->flag == cmp_equal || (e)->flag == mark_in)

static list *
get_equi_joins_first(mvc *sql, list *exps, int *equality_only)
{
	list *new_exps = sa_list(sql->sa);

	for( node *n = exps->h; n; n = n->next ) {
		sql_exp *e = n->data;

		assert(e->type == e_cmp && e->flag != cmp_in && e->flag != cmp_notin && e->flag != cmp_or);
		if (is_equi_exp_(e))
			list_append(new_exps, e);
		else
			*equality_only = 0;
	}
	for( node *n = exps->h; n; n = n->next ) {
		sql_exp *e = n->data;

		if (!is_equi_exp_(e))
			list_append(new_exps, e);
	}
	return new_exps;
}

static stmt *
rel2bin_join(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l, *sexps = NULL;
	node *en = NULL, *n;
	stmt *left = NULL, *right = NULL, *join = NULL, *jl, *jr;
	stmt *ld = NULL, *rd = NULL;
	int need_left = (rel->flag == LEFT_JOIN);

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(be, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	left = subrel_project(be, left, refs, rel->l);
	right = subrel_project(be, right, refs, rel->r);
	if (!left || !right)
		return NULL;
	left = row2cols(be, left);
	right = row2cols(be, right);
	/*
 	 * split in 2 steps,
 	 * 	first cheap join(s) (equality or idx)
 	 * 	second selects/filters
	 */
	if (!list_empty(rel->exps)) {
		list *jexps = sa_list(sql->sa);
		sexps = sa_list(sql->sa);

		split_join_exps(rel, jexps, sexps);
		if (list_empty(jexps)) { /* cross product and continue after project */
			stmt *l = bin_find_smallest_column(be, left);
			stmt *r = bin_find_smallest_column(be, right);
			join = stmt_join(be, l, r, 0, cmp_all, 0, 0, false);
		}

		if (join) {
			en = jexps->h;
		} else {
			list *lje = sa_list(sql->sa), *rje = sa_list(sql->sa), *exps = sa_list(sql->sa);
			int used_hash = 0, idx = 0, equality_only = 1;

			(void) equality_only;
			jexps = get_equi_joins_first(sql, jexps, &equality_only);
			/* generate a relational join (releqjoin) which does a multi attribute (equi) join */
			for( en = jexps->h; en; en = en->next ) {
				int join_idx = be->join_idx;
				sql_exp *e = en->data;
				stmt *s = NULL;
				prop *p;

				/* stop search for equi joins on first non equi */
				if (list_length(lje) && (idx || e->type != e_cmp || e->flag != cmp_equal))
					break;

				/* handle possible index lookups, expressions are in index order! */
				if (!join &&
					(p=find_prop(e->p, PROP_HASHCOL)) != NULL) {
					sql_idx *i = p->value;

					join = s = rel2bin_hash_lookup(be, rel, left, right, i, en);
					if (s) {
						list_append(lje, s->op1);
						list_append(rje, s->op2);
						list_append(exps, NULL);
						used_hash = 1;
					}
				}

				s = exp_bin(be, e, left, right, NULL, NULL, NULL, NULL, 0, 1, 0);
				if (!s) {
					assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
					return NULL;
				}
				if (join_idx != be->join_idx)
					idx = 1;
				assert(s->type == st_join || s->type == st_join2 || s->type == st_joinN);
				if (!join)
					join = s;
				if (e->flag != cmp_equal) { /* only collect equi joins */
					en = en->next;
					break;
				}
				list_append(lje, s->op1);
				list_append(rje, s->op2);
				list_append(exps, e);
			}
			if (list_length(lje) > 1) {
				join = releqjoin(be, lje, rje, exps, used_hash, need_left, 0);
			} else if (!join || need_left) {
				sql_exp *e = exps->h->data;
				join = stmt_join(be, lje->h->data, rje->h->data, 0, cmp_equal, need_left, is_semantics(e), false);
			}
		}
	} else {
		stmt *l = bin_find_smallest_column(be, left);
		stmt *r = bin_find_smallest_column(be, right);
		join = stmt_join(be, l, r, 0, cmp_all, 0, 0, rel->single);
	}
	jl = stmt_result(be, join, 0);
	jr = stmt_result(be, join, 1);
	if (en || (sexps && list_length(sexps))) {
		stmt *sub, *sel = NULL;
		list *nl;

		/* construct relation */
		nl = sa_list(sql->sa);

		/* first project using equi-joins */
		for( n = left->op4.lval->h; n; n = n->next ) {
			stmt *c = n->data;
			const char *rnme = table_name(sql->sa, c);
			const char *nme = column_name(sql->sa, c);
			stmt *s = stmt_project(be, jl, column(be, c) );

			s = stmt_alias(be, s, rnme, nme);
			list_append(nl, s);
		}
		for( n = right->op4.lval->h; n; n = n->next ) {
			stmt *c = n->data;
			const char *rnme = table_name(sql->sa, c);
			const char *nme = column_name(sql->sa, c);
			stmt *s = stmt_project(be, jr, column(be, c) );

			s = stmt_alias(be, s, rnme, nme);
			list_append(nl, s);
		}
		sub = stmt_list(be, nl);

		/* continue with non equi-joins */
		while(sexps) {
			if (!en) {
				en = sexps->h;
				sexps = NULL;
			}
			for( ; en; en = en->next ) {
				stmt *s = exp_bin(be, en->data, sub, NULL, NULL, NULL, NULL, sel, 0, 1, 0);

				if (!s) {
					assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
					return NULL;
				}
				if (s->nrcols == 0) {
					stmt *l = bin_find_smallest_column(be, sub);
					s = stmt_uselect(be, stmt_const(be, l, stmt_bool(be, 1)), s, cmp_equal, sel, 0, 0);
				}
				sel = s;
			}
		}
		/* recreate join output */
		jl = stmt_project(be, sel, jl);
		jr = stmt_project(be, sel, jr);
	}

	/* construct relation */
	l = sa_list(sql->sa);

	if (rel->op == op_left || rel->op == op_full || is_single(rel)) {
		/* we need to add the missing oid's */
		stmt *l = ld = stmt_mirror(be, bin_find_smallest_column(be, left));
		if (rel->op == op_left || rel->op == op_full)
			ld = stmt_tdiff(be, ld, jl, NULL);
		if (rel->single && !list_empty(rel->exps)) {
			join = stmt_semijoin(be, l, jl, NULL, NULL, 0, true);
			jl = stmt_result(be, join, 0);
			jr = stmt_project(be, stmt_result(be, join, 1), jr);
		}
	}
	if (rel->op == op_right || rel->op == op_full) {
		/* we need to add the missing oid's */
		rd = stmt_mirror(be, bin_find_smallest_column(be, right));
		rd = stmt_tdiff(be, rd, jr, NULL);
	}

	for( n = left->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		const char *rnme = table_name(sql->sa, c);
		const char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(be, jl, column(be, c) );

		/* as append isn't save, we append to a new copy */
		if (rel->op == op_left || rel->op == op_full || rel->op == op_right)
			s = create_const_column(be, s);
		if (rel->op == op_left || rel->op == op_full)
			s = stmt_append(be, s, stmt_project(be, ld, c));
		if (rel->op == op_right || rel->op == op_full)
			s = stmt_append(be, s, stmt_const(be, rd, (c->flag&OUTER_ZERO)?stmt_atom_lng(be, 0):stmt_atom(be, atom_general(sql->sa, tail_type(c), NULL))));

		s = stmt_alias(be, s, rnme, nme);
		list_append(l, s);
	}
	for( n = right->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		const char *rnme = table_name(sql->sa, c);
		const char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(be, jr, column(be, c) );

		/* as append isn't save, we append to a new copy */
		if (rel->op == op_left || rel->op == op_full || rel->op == op_right)
			s = create_const_column(be, s);
		if (rel->op == op_left || rel->op == op_full)
			s = stmt_append(be, s, stmt_const(be, ld, (c->flag&OUTER_ZERO)?stmt_atom_lng(be, 0):stmt_atom(be, atom_general(sql->sa, tail_type(c), NULL))));
		if (rel->op == op_right || rel->op == op_full)
			s = stmt_append(be, s, stmt_project(be, rd, c));

		s = stmt_alias(be, s, rnme, nme);
		list_append(l, s);
	}
	return stmt_list(be, l);
}

static int
exp_is_mark(sql_exp *e)
{
	if (e->type == e_cmp && (e->flag == mark_in || e->flag == mark_notin))
		return 1;
	return 0;
}

static stmt *
rel2bin_antijoin(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l, *jexps = NULL, *mexps = NULL;
	node *en = NULL, *n;
	stmt *left = NULL, *right = NULL, *join = NULL;

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(be, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	left = subrel_project(be, left, refs, rel->l);
	right = subrel_project(be, right, refs, rel->r);
	if (!left || !right)
		return NULL;
	left = row2cols(be, left);
	right = row2cols(be, right);

	if (rel->exps) {
		jexps = sa_list(sql->sa);
		mexps = sa_list(sql->sa);

		for( en = rel->exps->h; en; en = en->next ) {
			sql_exp *e = en->data;

			assert(e->type == e_cmp);
			if (exp_is_mark(e))
				append(mexps, e);
			else
				append(jexps, e);
		}
	}
	/* handle join-ing conditions first */
	if (!list_empty(jexps)) {
		if (list_empty(mexps))
			mexps = jexps;
	}
	/* handle mark conditions second */
	if (!list_empty(mexps)) {
		assert(list_length(mexps) == 1);
		for( en = mexps->h; en; en = en->next ) {
			sql_exp *e = en->data;
			stmt *ls = exp_bin(be, e->l, left, right, NULL, NULL, NULL, NULL, 0, 0, 0), *rs;
			if (!ls)
				return NULL;

			if (!(rs = exp_bin(be, e->r, left, right, NULL, NULL, NULL, NULL, 0, 0, 0)))
				return NULL;

			if (ls->nrcols == 0)
				ls = stmt_const(be, bin_find_smallest_column(be, left), ls);
			if (rs->nrcols == 0)
				rs = stmt_const(be, bin_find_smallest_column(be, right), rs);
			join = stmt_tdiff2(be, ls, rs, NULL);
		}
	}

	/* construct relation */
	l = sa_list(sql->sa);

	/* project all the left columns */
	for( n = left->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		const char *rnme = table_name(sql->sa, c);
		const char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(be, join, column(be, c));

		s = stmt_alias(be, s, rnme, nme);
		list_append(l, s);
	}
	return stmt_list(be, l);
}

static stmt *
rel2bin_semijoin(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l, *sexps = NULL;
	node *en = NULL, *n;
	stmt *left = NULL, *right = NULL, *join = NULL, *jl, *jr, *c, *lcand = NULL;
	int semijoin_only = 0, l_is_base = 0;

	if (rel->op == op_anti && list_length(rel->exps) == 1 && ((sql_exp*)rel->exps->h->data)->flag == mark_notin)
		return rel2bin_antijoin(be, rel, refs);

	if (rel->l) { /* first construct the left sub relation */
		sql_rel *l = rel->l;
		l_is_base = is_basetable(l->op);
		left = subrel_bin(be, l, refs);
	}
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	if (!left || !right)
		return NULL;
	left = row2cols(be, left);
	right = row2cols(be, right);
	/*
 	 * split in 2 steps,
 	 * 	first cheap join(s) (equality or idx)
 	 * 	second selects/filters
	 */
	if (!list_empty(rel->exps)) {
		list *jexps = sa_list(sql->sa);
		sexps = sa_list(sql->sa);

		split_join_exps(rel, jexps, sexps);
		if (list_empty(jexps)) { /* cross product and continue after project */
			right = subrel_project(be, right, refs, rel->r);
			stmt *l = bin_find_smallest_column(be, left);
			stmt *r = bin_find_smallest_column(be, right);
			join = stmt_join(be, l, r, 0, cmp_all, 0, 0, false);
			lcand = left->cand;
		}

		if (join) {
			en = jexps->h;
		} else {
			list *lje = sa_list(sql->sa), *rje = sa_list(sql->sa), *exps = sa_list(sql->sa);
			int idx = 0, equality_only = 1;

			jexps = get_equi_joins_first(sql, jexps, &equality_only);
			if (!equality_only || list_length(jexps) > 1 || exp_has_func((sql_exp*)jexps->h->data))
				left = subrel_project(be, left, refs, rel->l);
			right = subrel_project(be, right, refs, rel->r);

			for( en = jexps->h; en; en = en->next ) {
				int join_idx = be->join_idx;
				sql_exp *e = en->data;
				stmt *s = NULL;

				/* only handle simple joins here */
				if ((exp_has_func(e) && e->flag != cmp_filter) || e->flag == cmp_or || (e->f && e->anti)) {
					if (!join && !list_length(lje)) {
						stmt *l = bin_find_smallest_column(be, left);
						stmt *r = bin_find_smallest_column(be, right);
						join = stmt_join(be, l, r, 0, cmp_all, 0, 0, false);
					}
					break;
				}
				if (list_length(lje) && (idx || e->type != e_cmp || (e->flag != cmp_equal && e->flag != cmp_filter) ||
				(join && e->flag == cmp_filter)))
					break;

				if (equality_only) {
					int oldvtop = be->mb->vtop, oldstop = be->mb->stop, oldvid = be->mb->vid, swap = 0;
					stmt *r, *l = exp_bin(be, e->l, left, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);

					if (!l) {
						swap = 1;
						clean_mal_statements(be, oldstop, oldvtop, oldvid);
						l = exp_bin(be, e->l, right, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
					}
					r = exp_bin(be, e->r, left, right, NULL, NULL, NULL, NULL, 0, 0, 0);

					if (swap) {
						stmt *t = l;
						l = r;
						r = t;
					}

					if (!l || !r)
						return NULL;
					if (be->no_mitosis && list_length(jexps) == 1 && list_empty(sexps) && rel->op == op_semi && !e->anti && is_equi_exp_(e)) {
						join = stmt_semijoin(be, column(be, l), column(be, r), left->cand, NULL/*right->cand*/, is_semantics(e), false);
						semijoin_only = 1;
						en = NULL;
						break;
					} else
						s = stmt_join_cand(be, column(be, l), column(be, r), left->cand, NULL/*right->cand*/, e->anti, (comp_type) e->flag, 0, is_semantics(e), false);
					lcand = left->cand;
				} else {
					s = exp_bin(be, e, left, right, NULL, NULL, NULL, NULL, 0, 1, 0);
				}
				if (!s) {
					assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
					return NULL;
				}
				if (join_idx != be->join_idx)
					idx = 1;
				/* stop on first non equality join */
				if (!join) {
					if (s->type != st_join && s->type != st_join2 && s->type != st_joinN) {
						if (!en->next && (s->type == st_uselect || s->type == st_uselect2))
							join = s;
						else
							break;
					}
					join = s;
				} else if (s->type != st_join && s->type != st_join2 && s->type != st_joinN) {
					/* handle select expressions */
					break;
				}
				if (s->type == st_join || s->type == st_join2 || s->type == st_joinN) {
					list_append(lje, s->op1);
					list_append(rje, s->op2);
					list_append(exps, e);
				}
			}
			if (list_length(lje) > 1) {
				join = releqjoin(be, lje, rje, exps, 0 /* use hash */, 0, 0);
			} else if (!join && list_length(lje) == list_length(rje) && list_length(lje)) {
				sql_exp *e = exps->h->data;
				join = stmt_join(be, lje->h->data, rje->h->data, 0, cmp_equal, 0, is_semantics(e), false);
			} else if (!join) {
				stmt *l = bin_find_smallest_column(be, left);
				stmt *r = bin_find_smallest_column(be, right);
				join = stmt_join(be, l, r, 0, cmp_all, 0, 0, false);
			}
		}
	} else {
		right = subrel_project(be, right, refs, rel->r);
		stmt *l = bin_find_smallest_column(be, left);
		stmt *r = bin_find_smallest_column(be, right);
		join = stmt_join(be, l, r, 0, cmp_all, 0, 0, false);
		lcand = left->cand;
	}
	jl = stmt_result(be, join, 0);
	if (en || (sexps && list_length(sexps))) {
		stmt *sub, *sel = NULL;
		list *nl;

		jr = stmt_result(be, join, 1);
		/* construct relation */
		nl = sa_list(sql->sa);

		/* first project after equi-joins */
		for( n = left->op4.lval->h; n; n = n->next ) {
			stmt *c = n->data;
			const char *rnme = table_name(sql->sa, c);
			const char *nme = column_name(sql->sa, c);
			stmt *s = stmt_project(be, jl, column(be, c) );

			s = stmt_alias(be, s, rnme, nme);
			list_append(nl, s);
		}
		for( n = right->op4.lval->h; n; n = n->next ) {
			stmt *c = n->data;
			const char *rnme = table_name(sql->sa, c);
			const char *nme = column_name(sql->sa, c);
			stmt *s = stmt_project(be, jr, column(be, c) );

			s = stmt_alias(be, s, rnme, nme);
			list_append(nl, s);
		}
		sub = stmt_list(be, nl);

		/* continue with non equi-joins */
		while(sexps) {
			if (!en) {
				en = sexps->h;
				sexps = NULL;
			}
			for( ; en; en = en->next ) {
				stmt *s = exp_bin(be, en->data, sub, NULL, NULL, NULL, NULL, sel, 0, 1, 0);

				if (!s) {
					assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
					return NULL;
				}
				if (s->nrcols == 0) {
					stmt *l = bin_find_smallest_column(be, sub);
					s = stmt_uselect(be, stmt_const(be, l, stmt_bool(be, 1)), s, cmp_equal, sel, 0, 0);
				}
				sel = s;
			}
		}
		/* recreate join output */
		jl = stmt_project(be, sel, jl);
	}

	/* construct relation */
	l = sa_list(sql->sa);

	/* We did a full join, thats too much.
	   Reduce this using difference and intersect */
	if (!semijoin_only) {
		c = stmt_mirror(be, bin_find_smallest_column(be, left));
		if (rel->op == op_anti) {
			join = stmt_tdiff(be, c, jl, lcand);
		} else {
			if (lcand)
				join = stmt_semijoin(be, c, jl, lcand, NULL/*right->cand*/, 0, false);
			else
				join = stmt_tinter(be, c, jl, false);
		}
	}

	/* project all the left columns */
	for( n = left->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data, *s;
		const char *rnme = table_name(sql->sa, c);
		const char *nme = column_name(sql->sa, c);

		if (semijoin_only && l_is_base && nme[0] == '%' && strcmp(nme, TID) == 0)
			s = join;
		else
			s = stmt_project(be, join, column(be, c));

		s = stmt_alias(be, s, rnme, nme);
		list_append(l, s);
	}
	return stmt_list(be, l);
}

static stmt *
rel2bin_distinct(backend *be, stmt *s, stmt **distinct)
{
	mvc *sql = be->mvc;
	node *n;
	stmt *g = NULL, *grp = NULL, *ext = NULL, *cnt = NULL;
	list *rl = sa_list(sql->sa), *tids;

	/* single values are unique */
	if (s->key && s->nrcols == 0)
		return s;

	/* Use 'all' tid columns */
	if (/* DISABLES CODE */ (0) && (tids = bin_find_columns(be, s, TID)) != NULL) {
		for (n = tids->h; n; n = n->next) {
			stmt *t = n->data;

			g = stmt_group(be, column(be, t), grp, ext, cnt, !n->next);
			grp = stmt_result(be, g, 0);
			ext = stmt_result(be, g, 1);
			cnt = stmt_result(be, g, 2);
		}
	} else {
		for (n = s->op4.lval->h; n; n = n->next) {
			stmt *t = n->data;

			g = stmt_group(be, column(be, t), grp, ext, cnt, !n->next);
			grp = stmt_result(be, g, 0);
			ext = stmt_result(be, g, 1);
			cnt = stmt_result(be, g, 2);
		}
	}
	if (!ext)
		return NULL;

	for (n = s->op4.lval->h; n; n = n->next) {
		stmt *t = n->data;

		list_append(rl, stmt_project(be, ext, t));
	}

	if (distinct)
		*distinct = ext;
	s = stmt_list(be, rl);
	return s;
}

static stmt *
rel2bin_single(backend *be, stmt *s)
{
	if (s->key && s->nrcols == 0)
		return s;

	mvc *sql = be->mvc;
	list *rl = sa_list(sql->sa);

	for (node *n = s->op4.lval->h; n; n = n->next) {
		stmt *t = n->data;
		const char *rnme = table_name(sql->sa, t);
		const char *nme = column_name(sql->sa, t);
		sql_subfunc *zero_or_one = sql_bind_func(sql, "sys", "zero_or_one", tail_type(t), NULL, F_AGGR);

		t = stmt_aggr(be, t, NULL, NULL, zero_or_one, 1, 0, 1);
		t = stmt_alias(be, t, rnme, nme);
		list_append(rl, t);
	}
	s = stmt_list(be, rl);
	return s;
}

static stmt *
rel_rename(backend *be, sql_rel *rel, stmt *sub)
{
	mvc *sql = be->mvc;

	(void) sql;
	if (rel->exps) {
		node *en, *n;
		list *l = sa_list(be->mvc->sa);

		for( en = rel->exps->h, n = sub->op4.lval->h; en && n; en = en->next, n = n->next ) {
			sql_exp *exp = en->data;
			stmt *s = n->data;

			if (!s) {
				assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
				return NULL;
			}
			s = stmt_rename(be, exp, s);
			list_append(l, s);
		}
		sub = stmt_list(be, l);
	}
	return sub;
}

static stmt *
rel2bin_union(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l;
	node *n, *m;
	stmt *left = NULL, *right = NULL, *sub;

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(be, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	left = subrel_project(be, left, refs, rel->l);
	right = subrel_project(be, right, refs, rel->r);
	if (!left || !right)
		return NULL;

	/* construct relation */
	l = sa_list(sql->sa);
	for( n = left->op4.lval->h, m = right->op4.lval->h; n && m;
		 n = n->next, m = m->next ) {
		stmt *c1 = n->data;
		stmt *c2 = m->data;
		const char *rnme = table_name(sql->sa, c1);
		const char *nme = column_name(sql->sa, c1);
		stmt *s;

		s = stmt_append(be, create_const_column(be, c1), c2);
		s = stmt_alias(be, s, rnme, nme);
		list_append(l, s);
	}
	sub = stmt_list(be, l);

	sub = rel_rename(be, rel, sub);
	if (need_distinct(rel))
		sub = rel2bin_distinct(be, sub, NULL);
	if (rel->single)
		sub = rel2bin_single(be, sub);
	return sub;
}

static stmt *
rel2bin_except(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	sql_subtype *lng = sql_bind_localtype("lng");
	list *stmts;
	node *n, *m;
	stmt *left = NULL, *right = NULL, *sub;
	sql_subfunc *min;

	stmt *lg = NULL, *rg = NULL;
	stmt *lgrp = NULL, *rgrp = NULL;
	stmt *lext = NULL, *rext = NULL, *next = NULL;
	stmt *lcnt = NULL, *rcnt = NULL, *ncnt = NULL, *zero = NULL;
	stmt *s, *lm, *rm;
	list *lje = sa_list(sql->sa);
	list *rje = sa_list(sql->sa);

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(be, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	if (!left || !right)
		return NULL;
	left = subrel_project(be, left, refs, rel->l);
	right = subrel_project(be, right, refs, rel->r);
	left = row2cols(be, left);
	right = row2cols(be, right);

	/*
	 * The multi column except is handled using group by's and
	 * group size counts on both sides of the intersect. We then
	 * return for each group of L with min(L.count,R.count),
	 * number of rows.
	 */
	for (n = left->op4.lval->h; n; n = n->next) {
		lg = stmt_group(be, column(be, n->data), lgrp, lext, lcnt, !n->next);
		lgrp = stmt_result(be, lg, 0);
		lext = stmt_result(be, lg, 1);
		lcnt = stmt_result(be, lg, 2);
	}
	for (n = right->op4.lval->h; n; n = n->next) {
		rg = stmt_group(be, column(be, n->data), rgrp, rext, rcnt, !n->next);
		rgrp = stmt_result(be, rg, 0);
		rext = stmt_result(be, rg, 1);
		rcnt = stmt_result(be, rg, 2);
	}

	if (!lg || !rg)
		return NULL;

	if (need_distinct(rel)) {
		lcnt = stmt_const(be, lcnt, stmt_atom_lng(be, 1));
		rcnt = stmt_const(be, rcnt, stmt_atom_lng(be, 1));
	}

	/* now find the matching groups */
	for (n = left->op4.lval->h, m = right->op4.lval->h; n && m; n = n->next, m = m->next) {
		stmt *l = column(be, n->data);
		stmt *r = column(be, m->data);

		l = stmt_project(be, lext, l);
		r = stmt_project(be, rext, r);
		list_append(lje, l);
		list_append(rje, r);
	}
	s = releqjoin(be, lje, rje, NULL, 0 /* use hash */, 0, 1 /*is_semantics*/);
	lm = stmt_result(be, s, 0);
	rm = stmt_result(be, s, 1);

	s = stmt_mirror(be, lext);
	s = stmt_tdiff(be, s, lm, NULL);

	/* first we find those missing in R */
	next = stmt_project(be, s, lext);
	ncnt = stmt_project(be, s, lcnt);
	zero = stmt_const(be, s, stmt_atom_lng(be, 0));

	/* ext, lcount, rcount */
	lext = stmt_project(be, lm, lext);
	lcnt = stmt_project(be, lm, lcnt);
	rcnt = stmt_project(be, rm, rcnt);

	/* append those missing in L */
	lext = stmt_append(be, lext, next);
	lcnt = stmt_append(be, lcnt, ncnt);
	rcnt = stmt_append(be, rcnt, zero);

 	min = sql_bind_func(sql, "sys", "sql_sub", lng, lng, F_FUNC);
	s = stmt_binop(be, lcnt, rcnt, NULL, min); /* use count */

	/* now we have gid,cnt, blowup to full groupsizes */
	s = stmt_gen_group(be, lext, s);

	/* project columns of left hand expression */
	stmts = sa_list(sql->sa);
	for (n = left->op4.lval->h; n; n = n->next) {
		stmt *c1 = column(be, n->data);
		const char *rnme = NULL;
		const char *nme = column_name(sql->sa, c1);

		/* retain name via the stmt_alias */
		c1 = stmt_project(be, s, c1);

		rnme = table_name(sql->sa, c1);
		c1 = stmt_alias(be, c1, rnme, nme);
		list_append(stmts, c1);
	}
	sub = stmt_list(be, stmts);
	return rel_rename(be, rel, sub);
}

static stmt *
rel2bin_inter(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	sql_subtype *lng = sql_bind_localtype("lng");
	list *stmts;
	node *n, *m;
	stmt *left = NULL, *right = NULL, *sub;
 	sql_subfunc *min;

	stmt *lg = NULL, *rg = NULL;
	stmt *lgrp = NULL, *rgrp = NULL;
	stmt *lext = NULL, *rext = NULL;
	stmt *lcnt = NULL, *rcnt = NULL;
	stmt *s, *lm, *rm;
	list *lje = sa_list(sql->sa);
	list *rje = sa_list(sql->sa);

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(be, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	left = subrel_project(be, left, refs, rel->l);
	right = subrel_project(be, right, refs, rel->r);
	if (!left || !right)
		return NULL;
	left = row2cols(be, left);

	/*
	 * The multi column intersect is handled using group by's and
	 * group size counts on both sides of the intersect. We then
	 * return for each group of L with min(L.count,R.count),
	 * number of rows.
	 */
	for (n = left->op4.lval->h; n; n = n->next) {
		lg = stmt_group(be, column(be, n->data), lgrp, lext, lcnt, !n->next);
		lgrp = stmt_result(be, lg, 0);
		lext = stmt_result(be, lg, 1);
		lcnt = stmt_result(be, lg, 2);
	}
	for (n = right->op4.lval->h; n; n = n->next) {
		rg = stmt_group(be, column(be, n->data), rgrp, rext, rcnt, !n->next);
		rgrp = stmt_result(be, rg, 0);
		rext = stmt_result(be, rg, 1);
		rcnt = stmt_result(be, rg, 2);
	}

	if (!lg || !rg)
		return NULL;

	if (need_distinct(rel)) {
		lcnt = stmt_const(be, lcnt, stmt_atom_lng(be, 1));
		rcnt = stmt_const(be, rcnt, stmt_atom_lng(be, 1));
	}

	/* now find the matching groups */
	for (n = left->op4.lval->h, m = right->op4.lval->h; n && m; n = n->next, m = m->next) {
		stmt *l = column(be, n->data);
		stmt *r = column(be, m->data);

		l = stmt_project(be, lext, l);
		r = stmt_project(be, rext, r);
		list_append(lje, l);
		list_append(rje, r);
	}
	s = releqjoin(be, lje, rje, NULL, 0 /* use hash */, 0, 1 /* is_semantics */);
	lm = stmt_result(be, s, 0);
	rm = stmt_result(be, s, 1);

	/* ext, lcount, rcount */
	lext = stmt_project(be, lm, lext);
	lcnt = stmt_project(be, lm, lcnt);
	rcnt = stmt_project(be, rm, rcnt);

 	min = sql_bind_func(sql, "sys", "sql_min", lng, lng, F_FUNC);
	s = stmt_binop(be, lcnt, rcnt, NULL, min);

	/* now we have gid,cnt, blowup to full groupsizes */
	s = stmt_gen_group(be, lext, s);

	/* project columns of left hand expression */
	stmts = sa_list(sql->sa);
	for (n = left->op4.lval->h; n; n = n->next) {
		stmt *c1 = column(be, n->data);
		const char *rnme = NULL;
		const char *nme = column_name(sql->sa, c1);

		/* retain name via the stmt_alias */
		c1 = stmt_project(be, s, c1);

		rnme = table_name(sql->sa, c1);
		c1 = stmt_alias(be, c1, rnme, nme);
		list_append(stmts, c1);
	}
	sub = stmt_list(be, stmts);
	return rel_rename(be, rel, sub);
}

static stmt *
sql_reorder(backend *be, stmt *order, stmt *s)
{
	list *l = sa_list(be->mvc->sa);
	node *n;

	for (n = s->op4.lval->h; n; n = n->next) {
		stmt *sc = n->data;
		const char *cname = column_name(be->mvc->sa, sc);
		const char *tname = table_name(be->mvc->sa, sc);

		sc = stmt_project(be, order, sc);
		sc = stmt_alias(be, sc, tname, cname );
		list_append(l, sc);
	}
	return stmt_list(be, l);
}

static sql_exp*
topn_limit(sql_rel *rel)
{
	if (rel->exps) {
		sql_exp *limit = rel->exps->h->data;
		if (exp_is_null(limit)) /* If the limit is NULL, ignore the value */
			return NULL;
		return limit;
	}
	return NULL;
}

static sql_exp*
topn_offset( sql_rel *rel )
{
	if (rel->exps && list_length(rel->exps) > 1) {
		sql_exp *offset = rel->exps->h->next->data;

		return offset;
	}
	return NULL;
}

static stmt *
rel2bin_project(backend *be, sql_rel *rel, list *refs, sql_rel *topn)
{
	mvc *sql = be->mvc;
	list *pl;
	node *en, *n;
	stmt *sub = NULL, *psub = NULL;
	stmt *l = NULL;

	if (topn) {
		sql_exp *le = topn_limit(topn);
		sql_exp *oe = topn_offset(topn);

		if (!le) { /* Don't push only offset */
			topn = NULL;
		} else {
			l = exp_bin(be, le, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
			if(!l)
				return NULL;
			if (oe) {
				sql_subtype *lng = sql_bind_localtype("lng");
				sql_subfunc *add = sql_bind_func_result(sql, "sys", "sql_add", F_FUNC, lng, 2, lng, lng);
				stmt *o = exp_bin(be, oe, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
				if(!o)
					return NULL;
				l = stmt_binop(be, l, o, NULL, add);
			}
		}
	}

	if (!rel->exps)
		return stmt_none(be);

	if (rel->l) { /* first construct the sub relation */
		sql_rel *l = rel->l;
		if (l->op == op_ddl) {
			sql_table *t = rel_ddl_table_get(l);

			if (t)
				sub = rel2bin_sql_table(be, t, rel->exps);
		} else {
			sub = subrel_bin(be, rel->l, refs);
		}
		sub = subrel_project(be, sub, refs, rel->l);
		if (!sub)
			return NULL;
	}

	pl = sa_list(sql->sa);
	if (sub)
		pl->expected_cnt = list_length(sub->op4.lval);
	psub = stmt_list(be, pl);
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		int oldvtop = be->mb->vtop, oldstop = be->mb->stop, oldvid = be->mb->vid;
		stmt *s = exp_bin(be, exp, sub, NULL /*psub*/, NULL, NULL, NULL, NULL, 0, 0, 0);

		if (!s) { /* try with own projection as well, but first clean leftover statements */
			clean_mal_statements(be, oldstop, oldvtop, oldvid);
			s = exp_bin(be, exp, sub, psub, NULL, NULL, NULL, NULL, 0, 0, 0);
		}
		if (!s) /* error */
			return NULL;
		/* single value with limit */
		if (topn && rel->r && sub && sub->nrcols == 0 && s->nrcols == 0)
			s = const_column(be, s);
		else if (sub && sub->nrcols >= 1 && s->nrcols == 0)
			s = stmt_const(be, bin_find_smallest_column(be, sub), s);

		if (!exp_name(exp))
			exp_label(sql->sa, exp, ++sql->label);
		s = stmt_rename(be, exp, s);
		column_name(sql->sa, s); /* save column name */
		list_append(pl, s);
	}
	stmt_set_nrcols(psub);

	/* In case of a topn
		if both order by and distinct: then get first order by col
		do topn on it. Project all again! Then rest
	*/
	if (topn && rel->r) {
		list *oexps = rel->r, *npl = sa_list(sql->sa);
		/* distinct, topn returns atleast N (unique groups) */
		int distinct = need_distinct(rel);
		stmt *limit = NULL, *lpiv = NULL, *lgid = NULL;

		for (n=oexps->h; n; n = n->next) {
			sql_exp *orderbycole = n->data;
 			int last = (n->next == NULL);

			stmt *orderbycolstmt = exp_bin(be, orderbycole, sub, psub, NULL, NULL, NULL, NULL, 0, 0, 0);

			if (!orderbycolstmt)
				return NULL;

			/* handle constants */
			if (orderbycolstmt->nrcols == 0 && !last) /* no need to sort on constant */
				continue;
			orderbycolstmt = column(be, orderbycolstmt);
			if (!limit) {	/* topn based on a single column */
				limit = stmt_limit(be, orderbycolstmt, NULL, NULL, stmt_atom_lng(be, 0), l, distinct, is_ascending(orderbycole), nulls_last(orderbycole), last, 1);
			} else { 	/* topn based on 2 columns */
				limit = stmt_limit(be, orderbycolstmt, lpiv, lgid, stmt_atom_lng(be, 0), l, distinct, is_ascending(orderbycole), nulls_last(orderbycole), last, 1);
			}
			if (!limit)
				return NULL;
			lpiv = limit;
			if (!last) {
				lpiv = stmt_result(be, limit, 0);
				lgid = stmt_result(be, limit, 1);
			}
		}

		limit = lpiv;
		for ( n=pl->h ; n; n = n->next)
			list_append(npl, stmt_project(be, limit, column(be, n->data)));
		psub = stmt_list(be, npl);

		/* also rebuild sub as multiple orderby expressions may use the sub table (ie aren't part of the result columns) */
		pl = sub->op4.lval;
		npl = sa_list(sql->sa);
		for ( n=pl->h ; n; n = n->next) {
			list_append(npl, stmt_project(be, limit, column(be, n->data)));
		}
		sub = stmt_list(be, npl);
	}
	if (need_distinct(rel)) {
		stmt *distinct = NULL;
		psub = rel2bin_distinct(be, psub, &distinct);
		/* also rebuild sub as multiple orderby expressions may use the sub table (ie aren't part of the result columns) */
		if (sub && distinct) {
			list *npl = sa_list(sql->sa);

			pl = sub->op4.lval;
			for ( n=pl->h ; n; n = n->next)
				list_append(npl, stmt_project(be, distinct, column(be, n->data)));
			sub = stmt_list(be, npl);
		}
	}
	if (/*(!topn || need_distinct(rel)) &&*/ rel->r) {
		list *oexps = rel->r;
		stmt *orderby_ids = NULL, *orderby_grp = NULL;

		for (en = oexps->h; en; en = en->next) {
			stmt *orderby = NULL;
			sql_exp *orderbycole = en->data;
			stmt *orderbycolstmt = exp_bin(be, orderbycole, sub, psub, NULL, NULL, NULL, NULL, 0, 0, 0);

			if (!orderbycolstmt) {
				assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
				return NULL;
			}
			/* single values don't need sorting */
			if (orderbycolstmt->nrcols == 0)
				continue;
			if (orderby_ids)
				orderby = stmt_reorder(be, orderbycolstmt, is_ascending(orderbycole), nulls_last(orderbycole), orderby_ids, orderby_grp);
			else
				orderby = stmt_order(be, orderbycolstmt, is_ascending(orderbycole), nulls_last(orderbycole));
			orderby_ids = stmt_result(be, orderby, 1);
			orderby_grp = stmt_result(be, orderby, 2);
		}
		if (orderby_ids)
			psub = sql_reorder(be, orderby_ids, psub);
	}
	return psub;
}

static stmt *
rel2bin_predicate(backend *be)
{
	return const_column(be, stmt_bool(be, 1));
}

static stmt *
rel2bin_select(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	node *en;
	stmt *sub = NULL, *sel = NULL;
	stmt *predicate = NULL;

	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(be, rel->l, refs);
		sel = sub->cand;
		if (!sub)
			return NULL;
		sub = row2cols(be, sub);
	}
	if (!sub && !predicate)
		predicate = rel2bin_predicate(be);
	if (!rel->exps || !rel->exps->h) {
		if (sub)
			return sub;
		if (predicate)
			return predicate;
		assert(0);
		return stmt_const(be, bin_find_smallest_column(be, sub), stmt_bool(be, 1));
	}
	if (!sub && predicate) {
		list *l = sa_list(sql->sa);
		assert(predicate);
		append(l, predicate);
		sub = stmt_list(be, l);
	}
	/* handle possible index lookups */
	/* expressions are in index order ! */
	if (sub && (en = rel->exps->h) != NULL) {
		sql_exp *e = en->data;
		prop *p;

		if ((p=find_prop(e->p, PROP_HASHCOL)) != NULL) {
			sql_idx *i = p->value;

			sel = rel2bin_hash_lookup(be, rel, sub, NULL, i, en);
		}
	}
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *e = en->data;
		stmt *s = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, sel, 0, 1, 0);

		if (!s) {
			assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
			return NULL;
		}
		if (s->nrcols == 0){
			if (!predicate && sub)
				predicate = stmt_const(be, bin_find_smallest_column(be, sub), stmt_bool(be, 1));
			if (e->type != e_cmp) {
				sql_subtype *bt = sql_bind_localtype("bit");

				s = stmt_convert(be, s, NULL, exp_subtype(e), bt);
			}
			sel = stmt_uselect(be, predicate, s, cmp_equal, sel, 0, 0);
		} else if (e->type != e_cmp) {
			sel = stmt_uselect(be, s, stmt_bool(be, 1), cmp_equal, sel, 0, 0);
		} else {
			sel = s;
		}
	}

	if (sub && sel) {
		sub = stmt_list(be, sub->op4.lval); /* protect against references */
		sub->cand = sel;
	}
	return sub;
}

static stmt *
rel2bin_groupby(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l, *aggrs, *gbexps = sa_list(sql->sa);
	node *n, *en;
	stmt *sub = NULL, *cursub;
	stmt *groupby = NULL, *grp = NULL, *ext = NULL, *cnt = NULL;

	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(be, rel->l, refs);
		sub = subrel_project(be, sub, refs, rel->l);
		if (!sub)
			return NULL;
	}

	if (sub && sub->type == st_list && sub->op4.lval->h && !((stmt*)sub->op4.lval->h->data)->nrcols) {
		list *newl = sa_list(sql->sa);
		node *n;

		for(n=sub->op4.lval->h; n; n = n->next) {
			const char *cname = column_name(sql->sa, n->data);
			const char *tname = table_name(sql->sa, n->data);
			stmt *s = column(be, n->data);

			s = stmt_alias(be, s, tname, cname );
			append(newl, s);
		}
		sub = stmt_list(be, newl);
	}

	/* groupby columns */

	/* Keep groupby columns, sub that they can be lookup in the aggr list */
	if (rel->r) {
		list *exps = rel->r;

		for( en = exps->h; en; en = en->next ) {
			sql_exp *e = en->data;
			stmt *gbcol = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);

			if (!gbcol) {
				assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
				return NULL;
			}
			if (!gbcol->nrcols)
				gbcol = stmt_const(be, bin_find_smallest_column(be, sub), gbcol);
			groupby = stmt_group(be, gbcol, grp, ext, cnt, !en->next);
			grp = stmt_result(be, groupby, 0);
			ext = stmt_result(be, groupby, 1);
			cnt = stmt_result(be, groupby, 2);
			gbcol = stmt_alias(be, gbcol, exp_find_rel_name(e), exp_name(e));
			list_append(gbexps, gbcol);
		}
	}
	/* now aggregate */
	l = sa_list(sql->sa);
	aggrs = rel->exps;
	cursub = stmt_list(be, l);
	for( n = aggrs->h; n; n = n->next ) {
		sql_exp *aggrexp = n->data;
		stmt *aggrstmt = NULL;
		int oldvtop, oldstop, oldvid;

		/* first look in the current aggr list (l) and group by column list */
		if (l && !aggrstmt && aggrexp->type == e_column)
			aggrstmt = list_find_column(be, l, aggrexp->l, aggrexp->r);
		if (gbexps && !aggrstmt && aggrexp->type == e_column) {
			aggrstmt = list_find_column(be, gbexps, aggrexp->l, aggrexp->r);
			if (aggrstmt && groupby) {
				aggrstmt = stmt_project(be, ext, aggrstmt);
				if (list_length(gbexps) == 1)
					aggrstmt->key = 1;
			}
		}

		oldvtop = be->mb->vtop;
		oldstop = be->mb->stop;
		oldvid = be->mb->vid;
		if (!aggrstmt)
			aggrstmt = exp_bin(be, aggrexp, sub, NULL, grp, ext, cnt, NULL, 0, 0, 0);
		/* maybe the aggr uses intermediate results of this group by,
		   therefore we pass the group by columns too
		 */
		if (!aggrstmt) {
			clean_mal_statements(be, oldstop, oldvtop, oldvid);
			aggrstmt = exp_bin(be, aggrexp, sub, cursub, grp, ext, cnt, NULL, 0, 0, 0);
		}
		if (!aggrstmt) {
			assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
			return NULL;
		}

		if (!aggrstmt->nrcols && ext && ext->nrcols)
			aggrstmt = stmt_const(be, ext, aggrstmt);

		aggrstmt = stmt_rename(be, aggrexp, aggrstmt);
		list_append(l, aggrstmt);
	}
	stmt_set_nrcols(cursub);
	return cursub;
}

static stmt *
rel2bin_topn(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	sql_exp *oe = NULL, *le = NULL;
	stmt *sub = NULL, *l = NULL, *o = NULL;
	node *n;

	if (rel->l) { /* first construct the sub relation */
		sql_rel *rl = rel->l;

		if (rl->op == op_project) {
			sub = rel2bin_project(be, rl, refs, rel);
		} else {
			sub = subrel_bin(be, rl, refs);
		}
		sub = subrel_project(be, sub, refs, rl);
	}
	if (!sub)
		return NULL;

	le = topn_limit(rel);
	oe = topn_offset(rel);

	n = sub->op4.lval->h;
	if (n) {
		stmt *limit = NULL, *sc = n->data;
		const char *cname = column_name(sql->sa, sc);
		const char *tname = table_name(sql->sa, sc);
		list *newl = sa_list(sql->sa);
		int oldvtop = be->mb->vtop, oldstop = be->mb->stop, oldvid = be->mb->vid;

		if (le)
			l = exp_bin(be, le, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		if (!l) {
			clean_mal_statements(be, oldstop, oldvtop, oldvid);
			l = stmt_atom_lng_nil(be);
		}

		oldvtop = be->mb->vtop;
		oldstop = be->mb->stop;
		oldvid = be->mb->vid;
		if (oe)
			o = exp_bin(be, oe, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		if (!o) {
			clean_mal_statements(be, oldstop, oldvtop, oldvid);
			o = stmt_atom_lng(be, 0);
		}
		if (!l || !o)
			return NULL;

		sc = column(be, sc);
		limit = stmt_limit(be, stmt_alias(be, sc, tname, cname), NULL, NULL, o, l, 0,0,0,0,0);

		for ( ; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(sql->sa, sc);
			const char *tname = table_name(sql->sa, sc);

			sc = column(be, sc);
			sc = stmt_project(be, limit, sc);
			list_append(newl, stmt_alias(be, sc, tname, cname));
		}
		sub = stmt_list(be, newl);
	}
	return sub;
}

static stmt *
rel2bin_sample(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *newl;
	stmt *sub = NULL, *sample_size = NULL, *sample = NULL, *seed = NULL;
	node *n;

	if (rel->l) /* first construct the sub relation */
		sub = subrel_bin(be, rel->l, refs);
	sub = subrel_project(be, sub, refs, rel->l);
	if (!sub)
		return NULL;

	n = sub->op4.lval->h;
	newl = sa_list(sql->sa);

	if (n) {
		stmt *sc = n->data;
		const char *cname = column_name(sql->sa, sc);
		const char *tname = table_name(sql->sa, sc);

		 if (!(sample_size = exp_bin(be, rel->exps->h->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0)))
			return NULL;

		if (rel->exps->cnt == 2) {
			seed = exp_bin(be, rel->exps->h->next->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
			if (!seed)
				return NULL;
		}

		sc = column(be, sc);
		sample = stmt_sample(be, stmt_alias(be, sc, tname, cname),sample_size, seed);

		for ( ; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(sql->sa, sc);
			const char *tname = table_name(sql->sa, sc);

			sc = column(be, sc);
			sc = stmt_project(be, sample, sc);
			list_append(newl, stmt_alias(be, sc, tname, cname));
		}
	}
	sub = stmt_list(be, newl);
	return sub;
}

static stmt *
sql_parse(backend *be, sql_schema *s, const char *query, char mode)
{
	sql_rel *rel = rel_parse(be->mvc, s, query, mode);
	stmt *sq = NULL;

	if ((rel = sql_processrelation(be->mvc, rel, 1, 1)))
		sq = rel_bin(be, rel);
	return sq;
}

static stmt *
insert_check_ukey(backend *be, list *inserts, sql_key *k, stmt *idx_inserts)
{
	mvc *sql = be->mvc;
/* pkey's cannot have NULLs, ukeys however can
   current implementation switches on 'NOT NULL' on primary key columns */

	char *msg = NULL;
	stmt *res;

	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subfunc *cnt = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);
	sql_subtype *bt = sql_bind_localtype("bit");
	stmt *dels = stmt_tid(be, k->t, 0);
	sql_subfunc *ne = sql_bind_func_result(sql, "sys", "<>", F_FUNC, bt, 2, lng, lng);

	if (list_length(k->columns) > 1) {
		node *m;
		stmt *s = list_fetch(inserts, 0), *ins = s;
		sql_subfunc *sum;
		stmt *ssum = NULL;
		stmt *col = NULL;

		s = ins;
		/* 1st stage: find out if original contains same values */
		if (/*s->key &&*/ s->nrcols == 0) {
			s = NULL;
			if (k->idx && hash_index(k->idx->type))
				s = stmt_uselect(be, stmt_idx(be, k->idx, dels, dels->partition), idx_inserts, cmp_equal, s, 0, 1 /* is_semantics*/);
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *cs = list_fetch(inserts, c->c->colnr);

				col = stmt_col(be, c->c, dels, dels->partition);
				if ((k->type == ukey) && stmt_has_null(col)) {
					stmt *nn = stmt_selectnonil(be, col, s);
					s = stmt_uselect( be, col, cs, cmp_equal, nn, 0, 0);
				} else {
					s = stmt_uselect( be, col, cs, cmp_equal, s, 0, 0);
				}
			}
		} else {
			list *lje = sa_list(sql->sa);
			list *rje = sa_list(sql->sa);
			if (k->idx && hash_index(k->idx->type)) {
				list_append(lje, stmt_idx(be, k->idx, dels, dels->partition));
				list_append(rje, idx_inserts);
			}
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *cs = list_fetch(inserts, c->c->colnr);

				col = stmt_col(be, c->c, dels, dels->partition);
				list_append(lje, col);
				list_append(rje, cs);
			}
			s = releqjoin(be, lje, rje, NULL, 1 /* hash used */, 0, 0);
			s = stmt_result(be, s, 0);
		}
		s = stmt_binop(be, stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1), stmt_atom_lng(be, 0), NULL, ne);

		/* 2nd stage: find out if inserted are unique */
		if ((!idx_inserts && ins->nrcols) || (idx_inserts && idx_inserts->nrcols)) {	/* insert columns not atoms */
			sql_subfunc *or = sql_bind_func_result(sql, "sys", "or", F_FUNC, bt, 2, bt, bt);
			stmt *orderby_ids = NULL, *orderby_grp = NULL;
			stmt *sel = NULL;

			/* remove any nils as in stmt_order NULL = NULL, instead of NULL != NULL */
			if (k->type == ukey) {
				for (m = k->columns->h; m; m = m->next) {
					sql_kc *c = m->data;
					stmt *cs = list_fetch(inserts, c->c->colnr);

				   	if (stmt_has_null(cs))
						sel = stmt_selectnonil(be, cs, sel);
				}
			}
			/* implementation uses sort key check */
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *orderby;
				stmt *cs = list_fetch(inserts, c->c->colnr);

				if (sel)
					cs = stmt_project(be, sel, cs);
				if (orderby_grp)
					orderby = stmt_reorder(be, cs, 1, 0, orderby_ids, orderby_grp);
				else
					orderby = stmt_order(be, cs, 1, 0);
				orderby_ids = stmt_result(be, orderby, 1);
				orderby_grp = stmt_result(be, orderby, 2);
			}

			if (!orderby_grp || !orderby_ids)
				return NULL;

			sum = sql_bind_func(sql, "sys", "not_unique", tail_type(orderby_grp), NULL, F_AGGR);
			ssum = stmt_aggr(be, orderby_grp, NULL, NULL, sum, 1, 0, 1);
			/* combine results */
			s = stmt_binop(be, s, ssum, NULL, or);
		}

		if (k->type == pkey) {
			msg = sa_message(sql->sa, SQLSTATE(40002) "INSERT INTO: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, SQLSTATE(40002) "INSERT INTO: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(be, s, msg, 00001);
	} else {		/* single column key */
		sql_kc *c = k->columns->h->data;
		stmt *s = list_fetch(inserts, c->c->colnr), *h = s;

		s = stmt_col(be, c->c, dels, dels->partition);
		if ((k->type == ukey) && stmt_has_null(s)) {
			stmt *nn = stmt_selectnonil(be, s, NULL);
			s = stmt_project(be, nn, s);
		}
		if (h->nrcols) {
			s = stmt_join(be, s, h, 0, cmp_equal, 0, 0, false);
			/* s should be empty */
			s = stmt_result(be, s, 0);
			s = stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1);
		} else {
			s = stmt_uselect(be, s, h, cmp_equal, NULL, 0, 0);
			/* s should be empty */
			s = stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1);
		}
		/* s should be empty */
		s = stmt_binop(be, s, stmt_atom_lng(be, 0), NULL, ne);

		/* 2e stage: find out if inserts are unique */
		if (h->nrcols) {	/* insert multiple atoms */
			sql_subfunc *sum;
			stmt *count_sum = NULL;
			sql_subfunc *or = sql_bind_func_result(sql, "sys", "or", F_FUNC, bt, 2, bt, bt);
			stmt *ssum, *ss;

			stmt *g = list_fetch(inserts, c->c->colnr), *ins = g;

			/* inserted vaules may be null */
			if ((k->type == ukey) && stmt_has_null(ins)) {
				stmt *nn = stmt_selectnonil(be, ins, NULL);
				ins = stmt_project(be, nn, ins);
			}

			g = stmt_group(be, ins, NULL, NULL, NULL, 1);
			ss = stmt_result(be, g, 2); /* use count */
			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_func(sql, "sys", "sum", lng, NULL, F_AGGR);
			ssum = stmt_aggr(be, ss, NULL, NULL, sum, 1, 0, 1);
			ssum = sql_Nop_(be, "ifthenelse", sql_unop_(be, "isnull", ssum), stmt_atom_lng(be, 0), ssum, NULL);
			count_sum = stmt_binop(be, check_types(be, tail_type(ssum), stmt_aggr(be, ss, NULL, NULL, cnt, 1, 0, 1), type_equal), ssum, NULL, ne);

			/* combine results */
			s = stmt_binop(be, s, count_sum, NULL, or);
		}
		if (k->type == pkey) {
			msg = sa_message( sql->sa, SQLSTATE(40002) "INSERT INTO: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, SQLSTATE(40002) "INSERT INTO: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(be, s, msg, 00001);
	}
	return res;
}

static stmt *
insert_check_fkey(backend *be, list *inserts, sql_key *k, stmt *idx_inserts, stmt *pin)
{
	mvc *sql = be->mvc;
	char *msg = NULL;
	stmt *cs = list_fetch(inserts, 0), *s = cs;
	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subfunc *cnt = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *ne = sql_bind_func_result(sql, "sys", "<>", F_FUNC, bt, 2, lng, lng);

	if (pin && list_length(pin->op4.lval))
		s = pin->op4.lval->h->data;
	if (s->key && s->nrcols == 0) {
		s = stmt_binop(be, stmt_aggr(be, idx_inserts, NULL, NULL, cnt, 1, 0, 1), stmt_atom_lng(be, 1), NULL, ne);
	} else {
		/* releqjoin.count <> inserts[col1].count */
		s = stmt_binop(be, stmt_aggr(be, idx_inserts, NULL, NULL, cnt, 1, 0, 1), stmt_aggr(be, column(be, s), NULL, NULL, cnt, 1, 0, 1), NULL, ne);
	}

	/* s should be empty */
	msg = sa_message(sql->sa, SQLSTATE(40002) "INSERT INTO: FOREIGN KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
	return stmt_exception(be, s, msg, 00001);
}

static stmt *
sql_insert_key(backend *be, list *inserts, sql_key *k, stmt *idx_inserts, stmt *pin)
{
	/* int insert = 1;
	 * while insert and has u/pkey and not defered then
	 *      if u/pkey values exist then
	 *              insert = 0
	 * while insert and has fkey and not defered then
	 *      find id of corresponding u/pkey
	 *      if (!found)
	 *              insert = 0
	 * if insert
	 *      insert values
	 *      insert fkey/pkey index
	 */
	if (k->type == pkey || k->type == ukey) {
		return insert_check_ukey(be, inserts, k, idx_inserts );
	} else {		/* foreign keys */
		return insert_check_fkey(be, inserts, k, idx_inserts, pin );
	}
}

static int
sql_stack_add_inserted( mvc *sql, const char *name, sql_table *t, stmt **updates)
{
	/* Put single relation of updates and old values on to the stack */
	sql_rel *r = NULL;
	node *n;
	list *exps = sa_list(sql->sa);
	trigger_input *ti = SA_NEW(sql->sa, trigger_input);

	ti->t = t;
	ti->tids = NULL;
	ti->updates = updates;
	ti->type = 1;
	ti->nn = name;
	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;
		sql_exp *ne = exp_column(sql->sa, name, c->base.name, &c->type, CARD_MULTI, c->null, 0);

		append(exps, ne);
	}
	r = rel_table_func(sql->sa, NULL, NULL, exps, TRIGGER_WRAPPER);
	r->l = ti;

	return stack_push_rel_view(sql, name, r) ? 1 : 0;
}

static int
sql_insert_triggers(backend *be, sql_table *t, stmt **updates, int time)
{
	mvc *sql = be->mvc;
	node *n;
	int res = 1;

	if (!ol_length(t->triggers))
		return res;

	for (n = ol_first_node(t->triggers); n; n = n->next) {
		sql_trigger *trigger = n->data;

		if (!stack_push_frame(sql, "%OLD-NEW"))
			return 0;
		if (trigger->event == 0 && trigger->time == time) {
			const char *n = trigger->new_name;

			/* add name for the 'inserted' to the stack */
			if (!n) n = "new";

			if(!sql_stack_add_inserted(sql, n, t, updates)) {
				stack_pop_frame(sql);
				return 0;
			}
			if (!sql_parse(be, trigger->t->s, trigger->statement, m_instantiate)) {
				stack_pop_frame(sql);
				return 0;
			}
		}
		stack_pop_frame(sql);
	}
	return res;
}

static void
sql_insert_check_null(backend *be, sql_table *t, list *inserts)
{
	mvc *sql = be->mvc;
	node *m, *n;
	sql_subfunc *cnt = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);

	for (n = ol_first_node(t->columns), m = inserts->h; n && m;
		n = n->next, m = m->next) {
		stmt *i = m->data;
		sql_column *c = n->data;

		if (!c->null) {
			stmt *s = i;
			char *msg = NULL;

			if (!(s->key && s->nrcols == 0)) {
				s = stmt_selectnil(be, column(be, i));
				s = stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1);
			} else {
				sql_subfunc *isnil = sql_bind_func(sql, "sys", "isnull", &c->type, NULL, F_FUNC);

				s = stmt_unop(be, i, NULL, isnil);
			}
			msg = sa_message(sql->sa, SQLSTATE(40002) "INSERT INTO: NOT NULL constraint violated for column %s.%s", c->t->base.name, c->base.name);
			(void)stmt_exception(be, s, msg, 00001);
		}
	}
}

static stmt **
table_update_stmts(mvc *sql, sql_table *t, int *Len)
{
	*Len = ol_length(t->columns);
	return SA_ZNEW_ARRAY(sql->sa, stmt *, *Len);
}

static stmt *
rel2bin_insert(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l;
	stmt *inserts = NULL, *insert = NULL, *ddl = NULL, *pin = NULL, **updates, *ret = NULL, *cnt = NULL, *pos = NULL;
	int idx_ins = 0, constraint = 1, len = 0;
	node *n, *m, *idx_m = NULL;
	sql_rel *tr = rel->l, *prel = rel->r;
	sql_table *t = NULL;

	if ((rel->flag&UPD_NO_CONSTRAINT))
		constraint = 0;
	if ((rel->flag&UPD_COMP)) {  /* special case ! */
		idx_ins = 1;
		prel = rel->l;
		rel = rel->r;
		tr = rel->l;
	}

	if (tr->op == op_basetable) {
		t = tr->l;
	} else {
		ddl = subrel_bin(be, tr, refs);
		ddl = subrel_project(be, ddl, refs, NULL);
		if (!ddl)
			return NULL;
		t = rel_ddl_table_get(tr);
	}

	if (rel->r) /* first construct the inserts relation */
		inserts = subrel_bin(be, rel->r, refs);
	inserts = subrel_project(be, inserts, refs, rel->r);

	if (!inserts)
		return NULL;

	if (idx_ins)
		pin = refs_find_rel(refs, prel);

	if (constraint && !be->first_statement_generated)
		sql_insert_check_null(be, /*(be->cur_append && t->p) ? t->p :*/ t, inserts->op4.lval);

	l = sa_list(sql->sa);

	updates = table_update_stmts(sql, t, &len);
	for (n = ol_first_node(t->columns), m = inserts->op4.lval->h; n && m; n = n->next, m = m->next) {
		sql_column *c = n->data;

		updates[c->colnr] = m->data;
	}

/* before */
#if 0
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_insert_triggers(be, up, updates, 0))
				return sql_error(sql, 02, SQLSTATE(27000) "INSERT INTO: triggers failed for table '%s'", up->base.name);
		}
	}
#endif
	if (!sql_insert_triggers(be, t, updates, 0))
		return sql_error(sql, 02, SQLSTATE(27000) "INSERT INTO: triggers failed for table '%s'", t->base.name);

	insert = inserts->op4.lval->h->data;
	if (insert->nrcols == 0) {
		cnt = stmt_atom_lng(be, 1);
	} else {
		cnt = stmt_aggr(be, insert, NULL, NULL, sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR), 1, 0, 1);
	}
	insert = NULL;

	if (t->idxs) {
		idx_m = m;
		for (n = ol_first_node(t->idxs); n && m; n = n->next) {
			stmt *is = m->data;
			sql_idx *i = n->data;

		    if (non_updatable_index(i->type)) /* Some indexes don't hold delta structures */
				continue;
			if (hash_index(i->type) && list_length(i->columns) <= 1)
				is = NULL;
			if (i->key && constraint) {
				stmt *ckeys = sql_insert_key(be, inserts->op4.lval, i->key, is, pin);

				list_append(l, ckeys);
			}
			if (!insert)
				insert = is;
			/* If the index doesn't hold delta structures, don't update the 'm' variable */
			m = m->next;
		}
	}

	if (t->s) /* only not declared tables, need this */
		pos = stmt_claim(be, t, cnt);

	if (t->idxs)
	for (n = ol_first_node(t->idxs), m = idx_m; n && m; n = n->next) {
		stmt *is = m->data;
		sql_idx *i = n->data;

		if (non_updatable_index(i->type)) /* Some indexes don't hold delta structures */
			continue;
		if (hash_index(i->type) && list_length(i->columns) <= 1)
			is = NULL;
		if (is)
			is = stmt_append_idx(be, i, pos, is);
		/* If the index doesn't hold delta structures, don't update the 'm' variable */
		m = m->next;
	}

	for (n = ol_first_node(t->columns), m = inserts->op4.lval->h; n && m; n = n->next, m = m->next) {

		stmt *ins = m->data;
		sql_column *c = n->data;

		insert = stmt_append_col(be, c, pos, ins, rel->flag);
		append(l,insert);
	}
	if (!insert)
		return NULL;

#if 0
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_insert_triggers(be, up, updates, 1))
				return sql_error(sql, 02, SQLSTATE(27000) "INSERT INTO: triggers failed for table '%s'", up->base.name);
		}
	}
#endif
	if (!sql_insert_triggers(be, t, updates, 1))
		return sql_error(sql, 02, SQLSTATE(27000) "INSERT INTO: triggers failed for table '%s'", t->base.name);
	if (ddl) {
		ret = ddl;
		list_prepend(l, ddl);
	} else {
		ret = cnt;
	}

	if (be->cur_append) /* building the total number of rows affected across all tables */
		ret->nr = add_to_merge_partitions_accumulator(be, ret->nr);

	if (ddl)
		return stmt_list(be, l);
	else
		return ret;
}

static int
is_idx_updated(sql_idx * i, stmt **updates)
{
	int update = 0;
	node *m;

	for (m = i->columns->h; m; m = m->next) {
		sql_kc *ic = m->data;

		if (updates[ic->c->colnr]) {
			update = 1;
			break;
		}
	}
	return update;
}

static int
first_updated_col(stmt **updates, int cnt)
{
	int i;

	for (i = 0; i < cnt; i++) {
		if (updates[i])
			return i;
	}
	return -1;
}

static stmt *
update_check_ukey(backend *be, stmt **updates, sql_key *k, stmt *u_tids, stmt *idx_updates, int updcol)
{
	mvc *sql = be->mvc;
	char *msg = NULL;
	stmt *res = NULL;

	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subfunc *cnt = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *ne;

	ne = sql_bind_func_result(sql, "sys", "<>", F_FUNC, bt, 2, lng, lng);
	if (list_length(k->columns) > 1) {
		stmt *dels = stmt_tid(be, k->t, 0);
		node *m;
		stmt *s = NULL;

		/* 1st stage: find out if original (without the updated)
			do not contain the same values as the updated values.
			This is done using a relation join and a count (which
			should be zero)
	 	*/
		if (!isNew(k)) {
			stmt *nu_tids = stmt_tdiff(be, dels, u_tids, NULL); /* not updated ids */
			list *lje = sa_list(sql->sa);
			list *rje = sa_list(sql->sa);

			if (k->idx && hash_index(k->idx->type)) {
				list_append(lje, stmt_idx(be, k->idx, nu_tids, nu_tids->partition));
				list_append(rje, idx_updates);
			}
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *upd;

				assert(updates);
				if (updates[c->c->colnr]) {
					upd = updates[c->c->colnr];
				} else {
					upd = stmt_col(be, c->c, u_tids, u_tids->partition);
				}
				list_append(lje, stmt_col(be, c->c, nu_tids, nu_tids->partition));
				list_append(rje, upd);
			}
			s = releqjoin(be, lje, rje, NULL, 1 /* hash used */, 0, 0);
			s = stmt_result(be, s, 0);
			s = stmt_binop(be, stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1), stmt_atom_lng(be, 0), NULL, ne);
		}

		/* 2e stage: find out if the updated are unique */
		if (!updates || updates[updcol]->nrcols) {	/* update columns not atoms */
			sql_subfunc *sum;
			stmt *count_sum = NULL, *ssum;
			stmt *g = NULL, *grp = NULL, *ext = NULL, *Cnt = NULL;
			stmt *cand = NULL;
			stmt *ss;
			sql_subfunc *or = sql_bind_func_result(sql, "sys", "or", F_FUNC, bt, 2, bt, bt);

			/* also take the hopefully unique hash keys, to reduce
			   (re)group costs */
			if (k->idx && hash_index(k->idx->type)) {
				g = stmt_group(be, idx_updates, grp, ext, Cnt, 0);
				grp = stmt_result(be, g, 0);
				ext = stmt_result(be, g, 1);
				Cnt = stmt_result(be, g, 2);

				/* continue only with groups with a cnt > 1 */
				cand = stmt_uselect(be, Cnt, stmt_atom_lng(be, 1), cmp_gt, NULL, 0, 0);
				/* project cand on ext and Cnt */
				Cnt = stmt_project(be, cand, Cnt);
				ext = stmt_project(be, cand, ext);

				/* join groups with extend to retrieve all oid's of the original
				 * bat that belong to a group with Cnt >1 */
				g = stmt_join(be, grp, ext, 0, cmp_equal, 0, 0, false);
				cand = stmt_result(be, g, 0);
				grp = stmt_project(be, cand, grp);
			}

			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *upd;

				if (updates && updates[c->c->colnr]) {
					upd = updates[c->c->colnr];
				} else {
					upd = stmt_col(be, c->c, dels, dels->partition);
				}

				/* apply cand list first */
				if (cand)
					upd = stmt_project(be, cand, upd);

				/* remove nulls */
				if ((k->type == ukey) && stmt_has_null(upd)) {
					stmt *nn = stmt_selectnonil(be, upd, NULL);
					upd = stmt_project(be, nn, upd);
					if (grp)
						grp = stmt_project(be, nn, grp);
					if (cand)
						cand = stmt_project(be, nn, cand);
				}

				/* apply group by on groups with Cnt > 1 */
				g = stmt_group(be, upd, grp, ext, Cnt, !m->next);
				grp = stmt_result(be, g, 0);
				ext = stmt_result(be, g, 1);
				Cnt = stmt_result(be, g, 2);
			}
			ss = Cnt; /* use count */
			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_func(sql, "sys", "sum", lng, NULL, F_AGGR);
			ssum = stmt_aggr(be, ss, NULL, NULL, sum, 1, 0, 1);
			ssum = sql_Nop_(be, "ifthenelse", sql_unop_(be, "isnull", ssum), stmt_atom_lng(be, 0), ssum, NULL);
			count_sum = stmt_binop(be, stmt_aggr(be, ss, NULL, NULL, cnt, 1, 0, 1), check_types(be, lng, ssum, type_equal), NULL, ne);

			/* combine results */
			if (s)
				s = stmt_binop(be, s, count_sum, NULL, or);
			else
				s = count_sum;
		}

		if (k->type == pkey) {
			msg = sa_message(sql->sa, SQLSTATE(40002) "UPDATE: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, SQLSTATE(40002) "UPDATE: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(be, s, msg, 00001);
	} else {		/* single column key */
		stmt *dels = stmt_tid(be, k->t, 0);
		sql_kc *c = k->columns->h->data;
		stmt *s = NULL, *h = NULL, *o;

		/* s should be empty */
		if (!isNew(k)) {
			stmt *nu_tids = stmt_tdiff(be, dels, u_tids, NULL); /* not updated ids */
			assert (updates);

			h = updates[c->c->colnr];
			o = stmt_col(be, c->c, nu_tids, nu_tids->partition);
			s = stmt_join(be, o, h, 0, cmp_equal, 0, 0, false);
			s = stmt_result(be, s, 0);
			s = stmt_binop(be, stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1), stmt_atom_lng(be, 0), NULL, ne);
		}

		/* 2e stage: find out if updated are unique */
		if (!h || h->nrcols) {	/* update columns not atoms */
			sql_subfunc *sum;
			stmt *count_sum = NULL;
			sql_subfunc *or = sql_bind_func_result(sql, "sys", "or", F_FUNC, bt, 2, bt, bt);
			stmt *ssum, *ss;
			stmt *upd;
			stmt *g;

			if (updates) {
 				upd = updates[c->c->colnr];
			} else {
 				upd = stmt_col(be, c->c, dels, dels->partition);
			}

			/* remove nulls */
			if ((k->type == ukey) && stmt_has_null(upd)) {
				stmt *nn = stmt_selectnonil(be, upd, NULL);
				upd = stmt_project(be, nn, upd);
			}

			g = stmt_group(be, upd, NULL, NULL, NULL, 1);
			ss = stmt_result(be, g, 2); /* use count */

			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_func(sql, "sys", "sum", lng, NULL, F_AGGR);
			ssum = stmt_aggr(be, ss, NULL, NULL, sum, 1, 0, 1);
			ssum = sql_Nop_(be, "ifthenelse", sql_unop_(be, "isnull", ssum), stmt_atom_lng(be, 0), ssum, NULL);
			count_sum = stmt_binop(be, check_types(be, tail_type(ssum), stmt_aggr(be, ss, NULL, NULL, cnt, 1, 0, 1), type_equal), ssum, NULL, ne);

			/* combine results */
			if (s)
				s = stmt_binop(be, s, count_sum, NULL, or);
			else
				s = count_sum;
		}

		if (k->type == pkey) {
			msg = sa_message(sql->sa, SQLSTATE(40002) "UPDATE: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, SQLSTATE(40002) "UPDATE: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(be, s, msg, 00001);
	}
	return res;
}

/*
         A referential constraint is satisfied if one of the following con-
         ditions is true, depending on the <match option> specified in the
         <referential constraint definition>:

         -  If no <match type> was specified then, for each row R1 of the
            referencing table, either at least one of the values of the
            referencing columns in R1 shall be a null value, or the value of
            each referencing column in R1 shall be equal to the value of the
            corresponding referenced column in some row of the referenced
            table.

         -  If MATCH FULL was specified then, for each row R1 of the refer-
            encing table, either the value of every referencing column in R1
            shall be a null value, or the value of every referencing column
            in R1 shall not be null and there shall be some row R2 of the
            referenced table such that the value of each referencing col-
            umn in R1 is equal to the value of the corresponding referenced
            column in R2.

         -  If MATCH PARTIAL was specified then, for each row R1 of the
            referencing table, there shall be some row R2 of the refer-
            enced table such that the value of each referencing column in
            R1 is either null or is equal to the value of the corresponding
            referenced column in R2.
*/

static stmt *
update_check_fkey(backend *be, stmt **updates, sql_key *k, stmt *tids, stmt *idx_updates, int updcol, stmt *pup)
{
	mvc *sql = be->mvc;
	char *msg = NULL;
	stmt *s, *cur, *null = NULL, *cntnulls;
	sql_subtype *lng = sql_bind_localtype("lng"), *bt = sql_bind_localtype("bit");
	sql_subfunc *cnt = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);
	sql_subfunc *ne = sql_bind_func_result(sql, "sys", "<>", F_FUNC, bt, 2, lng, lng);
	sql_subfunc *or = sql_bind_func_result(sql, "sys", "or", F_FUNC, bt, 2, bt, bt);
	node *m;

	if (!idx_updates)
		return NULL;
	/* releqjoin.count <> updates[updcol].count */
	if (pup && list_length(pup->op4.lval)) {
		cur = pup->op4.lval->h->data;
	} else if (updates) {
		cur = updates[updcol];
	} else {
		sql_kc *c = k->columns->h->data;
		stmt *dels = stmt_tid(be, k->t, 0);
		assert(0);
		cur = stmt_col(be, c->c, dels, dels->partition);
	}
	s = stmt_binop(be, stmt_aggr(be, idx_updates, NULL, NULL, cnt, 1, 0, 1), stmt_aggr(be, cur, NULL, NULL, cnt, 1, 0, 1), NULL, ne);

	for (m = k->columns->h; m; m = m->next) {
		sql_kc *c = m->data;

		/* FOR MATCH FULL/SIMPLE/PARTIAL see above */
		/* Currently only the default MATCH SIMPLE is supported */
		if (c->c->null) {
			stmt *upd, *nn;

			if (updates && updates[c->c->colnr]) {
				upd = updates[c->c->colnr];
			} else if (updates && updcol >= 0) {
				assert(0);
				upd = stmt_col(be, c->c, tids, tids->partition);
			} else { /* created idx/key using alter */
				upd = stmt_col(be, c->c, tids, tids->partition);
			}
			nn = stmt_selectnil(be, upd);
			if (null)
				null = stmt_tunion(be, null, nn);
			else
				null = nn;
		}
	}
	if (null) {
		cntnulls = stmt_aggr(be, null, NULL, NULL, cnt, 1, 0, 1);
	} else {
		cntnulls = stmt_atom_lng(be, 0);
	}
	s = stmt_binop(be, s,
		stmt_binop(be, stmt_aggr(be, stmt_selectnil(be, idx_updates), NULL, NULL, cnt, 1, 0, 1), cntnulls, NULL, ne), NULL, or);

	/* s should be empty */
	msg = sa_message(sql->sa, SQLSTATE(40002) "UPDATE: FOREIGN KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
	return stmt_exception(be, s, msg, 00001);
}

static stmt *
join_updated_pkey(backend *be, sql_key * k, stmt *tids, stmt **updates)
{
	mvc *sql = be->mvc;
	sql_trans *tr = sql->session->tr;
	char *msg = NULL;
	int nulls = 0;
	node *m, *o;
	sql_key *rk = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)k)->rkey);
	stmt *s = NULL, *dels = stmt_tid(be, rk->t, 0), *fdels, *cnteqjoin;
	stmt *null = NULL, *rows;
	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *cnt = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);
	sql_subfunc *ne = sql_bind_func_result(sql, "sys", "<>", F_FUNC, bt, 2, lng, lng);
	list *lje = sa_list(sql->sa);
	list *rje = sa_list(sql->sa);

	fdels = stmt_tid(be, k->idx->t, 0);
	rows = stmt_idx(be, k->idx, fdels, fdels->partition);

	rows = stmt_join(be, rows, tids, 0, cmp_equal, 0, 0, false); /* join over the join index */
	rows = stmt_result(be, rows, 0);

	for (m = k->idx->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *fc = m->data;
		sql_kc *c = o->data;
		stmt *upd, *col;

		if (updates[c->c->colnr]) {
			upd = updates[c->c->colnr];
		} else {
			upd = stmt_project(be, tids, stmt_col(be, c->c, dels, dels->partition));
		}
		if (c->c->null) {	/* new nulls (MATCH SIMPLE) */
			stmt *nn = stmt_selectnil(be, upd);
			if (null)
				null = stmt_tunion(be, null, nn);
			else
				null = nn;
			nulls = 1;
		}
		col = stmt_col(be, fc->c, rows, rows->partition);
		if (!upd || (upd = check_types(be, &fc->c->type, upd, type_equal)) == NULL)
			return NULL;
		list_append(lje, upd);
		list_append(rje, col);
	}
	s = releqjoin(be, lje, rje, NULL, 1 /* hash used */, 0, 0);
	s = stmt_result(be, s, 0);

	/* add missing nulls */
	cnteqjoin = stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1);
	if (nulls) {
		sql_subfunc *add = sql_bind_func_result(sql, "sys", "sql_add", F_FUNC, lng, 2, lng, lng);
		cnteqjoin = stmt_binop(be, cnteqjoin, stmt_aggr(be, null, NULL, NULL, cnt, 1, 0, 1), NULL, add);
	}

	/* releqjoin.count <> updates[updcol].count */
	s = stmt_binop(be, cnteqjoin, stmt_aggr(be, rows, NULL, NULL, cnt, 1, 0, 1), NULL, ne);

	/* s should be empty */
	msg = sa_message(sql->sa, SQLSTATE(40002) "UPDATE: FOREIGN KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
	return stmt_exception(be, s, msg, 00001);
}

static list * sql_update(backend *be, sql_table *t, stmt *rows, stmt **updates);

static stmt*
sql_delete_set_Fkeys(backend *be, sql_key *k, stmt *ftids /* to be updated rows of fkey table */, int action)
{
	mvc *sql = be->mvc;
	sql_trans *tr = sql->session->tr;
	list *l = NULL;
	int len = 0;
	node *m, *o;
	sql_key *rk = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)k)->rkey);
	stmt **new_updates;
	sql_table *t = mvc_bind_table(sql, k->t->s, k->t->base.name);

	new_updates = table_update_stmts(sql, t, &len);
	for (m = k->idx->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *fc = m->data;
		stmt *upd = NULL;

		if (action == ACT_SET_DEFAULT) {
			if (fc->c->def) {
				stmt *sq = parse_value(be, fc->c->def, &fc->c->type, sql->emode);
				if (!sq)
					return NULL;
				upd = sq;
			} else {
				upd = stmt_atom(be, atom_general(sql->sa, &fc->c->type, NULL));
			}
		} else {
			upd = stmt_atom(be, atom_general(sql->sa, &fc->c->type, NULL));
		}

		if (!upd || (upd = check_types(be, &fc->c->type, upd, type_equal)) == NULL)
			return NULL;

		if (upd->nrcols <= 0)
			upd = stmt_const(be, ftids, upd);

		new_updates[fc->c->colnr] = upd;
	}
	if ((l = sql_update(be, t, ftids, new_updates)) == NULL)
		return NULL;
	return stmt_list(be, l);
}

static stmt*
sql_update_cascade_Fkeys(backend *be, sql_key *k, stmt *utids, stmt **updates, int action)
{
	mvc *sql = be->mvc;
	sql_trans *tr = sql->session->tr;
	list *l = NULL;
	int len = 0;
	node *m, *o;
	sql_key *rk = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)k)->rkey);
	stmt **new_updates;
	stmt *rows;
	sql_table *t = mvc_bind_table(sql, k->t->s, k->t->base.name);
	stmt *ftids, *upd_ids;

	ftids = stmt_tid(be, k->idx->t, 0);
	rows = stmt_idx(be, k->idx, ftids, ftids->partition);

	rows = stmt_join(be, rows, utids, 0, cmp_equal, 0, 0, false); /* join over the join index */
	upd_ids = stmt_result(be, rows, 1);
	rows = stmt_result(be, rows, 0);
	rows = stmt_project(be, rows, ftids);

	new_updates = table_update_stmts(sql, t, &len);
	for (m = k->idx->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *fc = m->data;
		sql_kc *c = o->data;
		stmt *upd = NULL;

		if (!updates[c->c->colnr]) {
			continue;
		} else if (action == ACT_CASCADE) {
			upd = updates[c->c->colnr];
		} else if (action == ACT_SET_DEFAULT) {
			if (fc->c->def) {
				stmt *sq = parse_value(be, fc->c->def, &fc->c->type, sql->emode);
				if (!sq)
					return NULL;
				upd = sq;
			} else {
				upd = stmt_atom(be, atom_general(sql->sa, &fc->c->type, NULL));
			}
		} else if (action == ACT_SET_NULL) {
			upd = stmt_atom(be, atom_general(sql->sa, &fc->c->type, NULL));
		}

		if (!upd || (upd = check_types(be, &fc->c->type, upd, type_equal)) == NULL)
			return NULL;

		if (upd->nrcols <= 0)
			upd = stmt_const(be, upd_ids, upd);
		else
			upd = stmt_project(be, upd_ids, upd);

		new_updates[fc->c->colnr] = upd;
	}

	if ((l = sql_update(be, t, rows, new_updates)) == NULL)
		return NULL;
	return stmt_list(be, l);
}

static int
cascade_ukey(backend *be, stmt **updates, sql_key *k, stmt *tids)
{
	/* now iterate over all keys */
	sql_trans *tr = be->mvc->session->tr;
	list *keys = sql_trans_get_dependencies(tr, k->base.id, FKEY_DEPENDENCY, NULL);
	if (keys) {
		for (node *n = keys->h; n; n = n->next->next) {
			sqlid fkey_id = *(sqlid*)n->data;
			sql_base *b = os_find_id(tr->cat->objects, tr, fkey_id);
			sql_key *fk = (sql_key*)b;
			sql_fkey *rk = (sql_fkey*)b;

			if (fk->type != fkey || rk->rkey != k->base.id)
				continue;

			/* All rows of the foreign key table which are
			   affected by the primary key update should all
			   match one of the updated primary keys again.
			 */
			switch (((sql_fkey*)fk)->on_update) {
			case ACT_NO_ACTION:
				break;
			case ACT_SET_NULL:
			case ACT_SET_DEFAULT:
			case ACT_CASCADE:
				if (!sql_update_cascade_Fkeys(be, fk, tids, updates, ((sql_fkey*)fk)->on_update)) {
					list_destroy(keys);
					return -1;
				}
				break;
			default:	/*RESTRICT*/
				if (!join_updated_pkey(be, fk, tids, updates)) {
					list_destroy(keys);
					return -1;
				}
			}
		}
		list_destroy(keys);
	}
	return 0;
}

static void
sql_update_check_key(backend *be, stmt **updates, sql_key *k, stmt *tids, stmt *idx_updates, int updcol, list *l, stmt *pup)
{
	stmt *ckeys;

	if (k->type == pkey || k->type == ukey) {
		ckeys = update_check_ukey(be, updates, k, tids, idx_updates, updcol);
	} else { /* foreign keys */
		ckeys = update_check_fkey(be, updates, k, tids, idx_updates, updcol, pup);
	}
	list_append(l, ckeys);
}

static stmt *
hash_update(backend *be, sql_idx * i, stmt *rows, stmt **updates, int updcol)
{
	mvc *sql = be->mvc;
	/* calculate new value */
	node *m;
	sql_subtype *it, *lng;
	int bits = 1 + ((sizeof(lng)*8)-1)/(list_length(i->columns)+1);
	stmt *h = NULL, *tids;

	if (list_length(i->columns) <= 1)
		return NULL;

	tids = stmt_tid(be, i->t, 0);
	it = sql_bind_localtype("int");
	lng = sql_bind_localtype("lng");
	for (m = i->columns->h; m; m = m->next ) {
		sql_kc *c = m->data;
		stmt *upd;

		if (updates && updates[c->c->colnr]) {
			upd = updates[c->c->colnr];
		} else if (updates && updcol >= 0) {
			assert(0);
			upd = stmt_col(be, c->c, rows, rows->partition);
		} else { /* created idx/key using alter */
			upd = stmt_col(be, c->c, tids, tids->partition);
		}

		if (h && i->type == hash_idx)  {
			sql_subfunc *xor = sql_bind_func_result(sql, "sys", "rotate_xor_hash", F_FUNC, lng, 3, lng, it, &c->c->type);

			h = stmt_Nop(be, stmt_list( be, list_append( list_append(
				list_append(sa_list(sql->sa), h),
				stmt_atom_int(be, bits)),  upd)), NULL,
				xor);
		} else if (h)  {
			stmt *h2;
			sql_subfunc *lsh = sql_bind_func_result(sql, "sys", "left_shift", F_FUNC, lng, 2, lng, it);
			sql_subfunc *lor = sql_bind_func_result(sql, "sys", "bit_or", F_FUNC, lng, 2, lng, lng);
			sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, lng, 1, &c->c->type);

			h = stmt_binop(be, h, stmt_atom_int(be, bits), NULL, lsh);
			h2 = stmt_unop(be, upd, NULL, hf);
			h = stmt_binop(be, h, h2, NULL, lor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, lng, 1, &c->c->type);
			h = stmt_unop(be, upd, NULL, hf);
			if (i->type == oph_idx)
				break;
		}
	}
	return h;
}

static stmt *
join_idx_update(backend *be, sql_idx * i, stmt *ftids, stmt **updates, int updcol)
{
	mvc *sql = be->mvc;
	sql_trans *tr = sql->session->tr;
	node *m, *o;
	sql_key *rk = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)i->key)->rkey);
	stmt *s = NULL, *ptids = stmt_tid(be, rk->t, 0), *l, *r;
	list *lje = sa_list(sql->sa);
	list *rje = sa_list(sql->sa);

	for (m = i->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *c = m->data;
		sql_kc *rc = o->data;
		stmt *upd;

		if (updates && updates[c->c->colnr]) {
			upd = updates[c->c->colnr];
		} else if (updates && updcol >= 0) {
			assert(0);
			upd = stmt_col(be, c->c, ftids, ftids->partition);
		} else { /* created idx/key using alter */
			upd = stmt_col(be, c->c, ftids, ftids->partition);
		}

		list_append(lje, check_types(be, &rc->c->type, upd, type_equal));
		list_append(rje, stmt_col(be, rc->c, ptids, ptids->partition));
	}
	s = releqjoin(be, lje, rje, NULL, 0 /* use hash */, 0, 0);
	l = stmt_result(be, s, 0);
	r = stmt_result(be, s, 1);
	r = stmt_project(be, r, ptids);
	return stmt_left_project(be, ftids, l, r);
}

static int
cascade_updates(backend *be, sql_table *t, stmt *rows, stmt **updates)
{
	mvc *sql = be->mvc;
	node *n;

	if (!ol_length(t->idxs))
		return 0;

	for (n = ol_first_node(t->idxs); n; n = n->next) {
		sql_idx *i = n->data;

		/* check if update is needed,
		 * ie atleast on of the idx columns is updated
		 */
		if (is_idx_updated(i, updates) == 0)
			continue;

		if (i->key) {
			if (!(sql->cascade_action && list_find_id(sql->cascade_action, i->key->base.id))) {
				sql_key *k = i->key;
				sqlid *local_id = SA_NEW(sql->sa, sqlid);
				if (!sql->cascade_action)
					sql->cascade_action = sa_list(sql->sa);
				*local_id = i->key->base.id;
				list_append(sql->cascade_action, local_id);
				if (k->type == pkey || k->type == ukey) {
					if (cascade_ukey(be, updates, k, rows))
						return -1;
				}
			}
		}
	}
	return 0;
}

static list *
update_idxs_and_check_keys(backend *be, sql_table *t, stmt *rows, stmt **updates, list *l, stmt *pup)
{
	mvc *sql = be->mvc;
	node *n;
	int updcol;
	list *idx_updates = sa_list(sql->sa);

	if (!ol_length(t->idxs))
		return idx_updates;

	updcol = first_updated_col(updates, ol_length(t->columns));
	for (n = ol_first_node(t->idxs); n; n = n->next) {
		sql_idx *i = n->data;
		stmt *is = NULL;

		/* check if update is needed,
		 * ie atleast on of the idx columns is updated
		 */
		if (is_idx_updated(i, updates) == 0)
			continue;

		if (hash_index(i->type)) {
			is = hash_update(be, i, rows, updates, updcol);
		} else if (i->type == join_idx) {
			if (updcol < 0)
				return NULL;
			is = join_idx_update(be, i, rows, updates, updcol);
		}
		if (i->key)
			sql_update_check_key(be, updates, i->key, rows, is, updcol, l, pup);
		if (is)
			list_append(idx_updates, stmt_update_idx(be, i, rows, is));
	}
	return idx_updates;
}

static int
sql_stack_add_updated(mvc *sql, const char *on, const char *nn, sql_table *t, stmt *tids, stmt **updates)
{
	/* Put single relation of updates and old values on to the stack */
	sql_rel *r = NULL;
	node *n;
	list *exps = sa_list(sql->sa);
	trigger_input *ti = SA_NEW(sql->sa, trigger_input);

	ti->t = t;
	ti->tids = tids;
	ti->updates = updates;
	ti->type = 2;
	ti->on = on;
	ti->nn = nn;
	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;

		if (updates[c->colnr]) {
			sql_exp *oe = exp_column(sql->sa, on, c->base.name, &c->type, CARD_MULTI, c->null, 0);
			sql_exp *ne = exp_column(sql->sa, nn, c->base.name, &c->type, CARD_MULTI, c->null, 0);

			append(exps, oe);
			append(exps, ne);
		} else {
			sql_exp *oe = exp_column(sql->sa, on, c->base.name, &c->type, CARD_MULTI, c->null, 0);
			sql_exp *ne = exp_column(sql->sa, nn, c->base.name, &c->type, CARD_MULTI, c->null, 0);

			append(exps, oe);
			append(exps, ne);
		}
	}
	r = rel_table_func(sql->sa, NULL, NULL, exps, TRIGGER_WRAPPER);
	r->l = ti;

	/* put single table into the stack with 2 names, needed for the psm code */
	if (!stack_push_rel_view(sql, on, r) || !stack_push_rel_view(sql, nn, rel_dup(r)))
		return 0;
	return 1;
}

static int
sql_update_triggers(backend *be, sql_table *t, stmt *tids, stmt **updates, int time )
{
	mvc *sql = be->mvc;
	node *n;
	int res = 1;

	if (!ol_length(t->triggers))
		return res;

	for (n = ol_first_node(t->triggers); n; n = n->next) {
		sql_trigger *trigger = n->data;

		if (!stack_push_frame(sql, "%OLD-NEW"))
			return 0;
		if (trigger->event == 2 && trigger->time == time) {
			/* add name for the 'inserted' to the stack */
			const char *n = trigger->new_name;
			const char *o = trigger->old_name;

			if (!n) n = "new";
			if (!o) o = "old";

			if(!sql_stack_add_updated(sql, o, n, t, tids, updates)) {
				stack_pop_frame(sql);
				return 0;
			}

			if (!sql_parse(be, trigger->t->s, trigger->statement, m_instantiate)) {
				stack_pop_frame(sql);
				return 0;
			}
		}
		stack_pop_frame(sql);
	}
	return res;
}

static void
sql_update_check_null(backend *be, sql_table *t, stmt **updates)
{
	mvc *sql = be->mvc;
	node *n;
	sql_subfunc *cnt = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);

	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;

		if (updates[c->colnr] && !c->null) {
			stmt *s = updates[c->colnr];
			char *msg = NULL;

			if (!(s->key && s->nrcols == 0)) {
				s = stmt_selectnil(be, updates[c->colnr]);
				s = stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1);
			} else {
				sql_subfunc *isnil = sql_bind_func(sql, "sys", "isnull", &c->type, NULL, F_FUNC);

				s = stmt_unop(be, updates[c->colnr], NULL, isnil);
			}
			msg = sa_message(sql->sa, SQLSTATE(40002) "UPDATE: NOT NULL constraint violated for column '%s.%s'", c->t->base.name, c->base.name);
			(void)stmt_exception(be, s, msg, 00001);
		}
	}
}

/* updates: an array of table width, per column holds the values for the to be updated rows  */
static list *
sql_update(backend *be, sql_table *t, stmt *rows, stmt **updates)
{
	mvc *sql = be->mvc;
	list *idx_updates = NULL;
	int i, nr_cols = ol_length(t->columns);
	list *l = sa_list(sql->sa);
	node *n;

	if (!be->first_statement_generated)
		sql_update_check_null(be, /*(be->cur_append && t->p) ? t->p :*/ t, updates);

	/* check keys + get idx */
	idx_updates = update_idxs_and_check_keys(be, t, rows, updates, l, NULL);
	if (!idx_updates) {
		assert(0);
		return sql_error(sql, 02, SQLSTATE(42000) "UPDATE: failed to update indexes for table '%s'", t->base.name);
	}

/* before */
#if 0
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_update_triggers(be, up, rows, updates, 0))
				return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", up->base.name);
		}
	}
#endif
	if (!sql_update_triggers(be, t, rows, updates, 0))
		return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", t->base.name);

/* apply updates */
	for (i = 0, n = ol_first_node(t->columns); i < nr_cols && n; i++, n = n->next) {
		sql_column *c = n->data;

		if (updates[i])
	       		append(l, stmt_update_col(be, c, rows, updates[i]));
	}
	if (cascade_updates(be, t, rows, updates))
		return sql_error(sql, 02, SQLSTATE(42000) "UPDATE: cascade failed for table '%s'", t->base.name);

/* after */
#if 0
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_update_triggers(be, up, rows, updates, 1))
				return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", up->base.name);
		}
	}
#endif
	if (!sql_update_triggers(be, t, rows, updates, 1))
		return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", t->base.name);

/* cascade ?? */
	return l;
}

/* updates with empty list is alter with create idx or keys */
static stmt *
rel2bin_update(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	stmt *update = NULL, **updates = NULL, *tids, *s, *ddl = NULL, *pup = NULL, *cnt;
	list *l = sa_list(sql->sa);
	int nr_cols, updcol, idx_ups = 0;
	node *m;
	sql_rel *tr = rel->l, *prel = rel->r;
	sql_table *t = NULL;

	if ((rel->flag&UPD_COMP)) {  /* special case ! */
		idx_ups = 1;
		prel = rel->l;
		rel = rel->r;
		tr = rel->l;
	}
	if (tr->op == op_basetable) {
		t = tr->l;
	} else {
		ddl = subrel_bin(be, tr, refs);
		ddl = subrel_project(be, ddl, refs, NULL);
		if (!ddl)
			return NULL;
		t = rel_ddl_table_get(tr);

		/* no columns to update (probably an new pkey!) */
		if (!rel->exps)
			return ddl;
	}

	if (rel->r) /* first construct the update relation */
		update = subrel_bin(be, rel->r, refs);
	update = subrel_project(be, update, refs, rel->r);

	if (!update)
		return NULL;

	if (idx_ups)
		pup = refs_find_rel(refs, prel);

	updates = table_update_stmts(sql, t, &nr_cols);
	tids = update->op4.lval->h->data;

	/* lookup the updates */
	for (m = rel->exps->h; m; m = m->next) {
		sql_exp *ce = m->data;
		sql_column *c = find_sql_column(t, exp_name(ce));

		if (c)
			updates[c->colnr] = bin_find_column(be, update, ce->l, ce->r);
	}
	if (!be->first_statement_generated)
		sql_update_check_null(be, /*(be->cur_append && t->p) ? t->p :*/ t, updates);

	/* check keys + get idx */
	updcol = first_updated_col(updates, ol_length(t->columns));
	for (m = rel->exps->h; m; m = m->next) {
		sql_exp *ce = m->data;
		sql_idx *i = find_sql_idx(t, exp_name(ce)+1);
		stmt *update_idx, *is = NULL;

		if (i) {
			if (non_updatable_index(i->type)) /* Some indexes don't hold delta structures */
				continue;

			update_idx = bin_find_column(be, update, ce->l, ce->r);
			if (update_idx)
				is = update_idx;
			if (hash_index(i->type) && list_length(i->columns) <= 1) {
				is = NULL;
				update_idx = NULL;
			}
			if (i->key)
				sql_update_check_key(be, (updcol>=0)?updates:NULL, i->key, tids, update_idx, updcol, l, pup);
			if (is)
				list_append(l, stmt_update_idx(be,  i, tids, is));
		}
	}

/* before */
#if 0
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_update_triggers(be, up, tids, updates, 0))
				return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", up->base.name);
		}
	}
#endif
	if (!sql_update_triggers(be, t, tids, updates, 0))
		return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", t->base.name);

/* apply the update */
	for (m = rel->exps->h; m; m = m->next) {
		sql_exp *ce = m->data;
		sql_column *c = find_sql_column(t, exp_name(ce));

		if (c)
			append(l, stmt_update_col(be,  c, tids, updates[c->colnr]));
	}

	if (cascade_updates(be, t, tids, updates))
		return sql_error(sql, 02, SQLSTATE(42000) "UPDATE: cascade failed for table '%s'", t->base.name);

/* after */
#if 0
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_update_triggers(be, up, tids, updates, 1))
				return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", up->base.name);
		}
	}
#endif
	if (!sql_update_triggers(be, t, tids, updates, 1))
		return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", t->base.name);

	if (ddl) {
		list_prepend(l, ddl);
		cnt = stmt_list(be, l);
	} else {
		s = stmt_aggr(be, tids, NULL, NULL, sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR), 1, 0, 1);
		cnt = s;
	}

	if (be->cur_append) /* building the total number of rows affected across all tables */
		cnt->nr = add_to_merge_partitions_accumulator(be, cnt->nr);

	if (sql->cascade_action)
		sql->cascade_action = NULL;
	return cnt;
}

static int
sql_stack_add_deleted(mvc *sql, const char *name, sql_table *t, stmt *tids, stmt **deleted_cols, int type)
{
	/* Put single relation of updates and old values on to the stack */
	sql_rel *r = NULL;
	node *n;
	list *exps = sa_list(sql->sa);
	trigger_input *ti = SA_NEW(sql->sa, trigger_input);

	ti->t = t;
	ti->tids = tids;
	ti->updates = deleted_cols;
	ti->type = type;
	ti->nn = name;
	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;
		sql_exp *ne = exp_column(sql->sa, name, c->base.name, &c->type, CARD_MULTI, c->null, 0);

		append(exps, ne);
	}
	r = rel_table_func(sql->sa, NULL, NULL, exps, TRIGGER_WRAPPER);
	r->l = ti;

	return stack_push_rel_view(sql, name, r) ? 1 : 0;
}

static int
sql_delete_triggers(backend *be, sql_table *t, stmt *tids, stmt **deleted_cols, int time, int firing_type, int internal_type)
{
	mvc *sql = be->mvc;
	node *n;
	int res = 1;

	if (!ol_length(t->triggers))
		return res;

	for (n = ol_first_node(t->triggers); n; n = n->next) {
		sql_trigger *trigger = n->data;

		if (!stack_push_frame(sql, "%OLD-NEW"))
			return 0;
		if (trigger->event == firing_type && trigger->time == time) {
			/* add name for the 'deleted' to the stack */
			const char *o = trigger->old_name;

			if (!o) o = "old";

			if(!sql_stack_add_deleted(sql, o, t, tids, deleted_cols, internal_type)) {
				stack_pop_frame(sql);
				return 0;
			}

			if (!sql_parse(be, trigger->t->s, trigger->statement, m_instantiate)) {
				stack_pop_frame(sql);
				return 0;
			}
		}
		stack_pop_frame(sql);
	}
	return res;
}

static stmt * sql_delete(backend *be, sql_table *t, stmt *rows);

static stmt *
sql_delete_cascade_Fkeys(backend *be, sql_key *fk, stmt *ftids)
{
	sql_table *t = mvc_bind_table(be->mvc, fk->t->s, fk->t->base.name);
	return sql_delete(be, t, ftids);
}

static void
sql_delete_ukey(backend *be, stmt *utids /* deleted tids from ukey table */, sql_key *k, list *l, char* which, int cascade)
{
	mvc *sql = be->mvc;
	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_trans *tr = be->mvc->session->tr;
	list *keys = sql_trans_get_dependencies(tr, k->base.id, FKEY_DEPENDENCY, NULL);

	if (keys) {
		for (node *n = keys->h; n; n = n->next->next) {
			sqlid fkey_id = *(sqlid*)n->data;
			sql_base *b = os_find_id(tr->cat->objects, tr, fkey_id);
			sql_key *fk = (sql_key*)b;
			sql_fkey *rk = (sql_fkey*)b;

			if (fk->type != fkey || rk->rkey != k->base.id)
				continue;
			char *msg = NULL;
			sql_subfunc *cnt = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);
			sql_subfunc *ne = sql_bind_func_result(sql, "sys", "<>", F_FUNC, bt, 2, lng, lng);
			stmt *s, *tids;

			tids = stmt_tid(be, fk->idx->t, 0);
			s = stmt_idx(be, fk->idx, tids, tids->partition);
			s = stmt_join(be, s, utids, 0, cmp_equal, 0, 0, false); /* join over the join index */
			s = stmt_result(be, s, 0);
			tids = stmt_project(be, s, tids);
			if(cascade) { /* for truncate statements with the cascade option */
				s = sql_delete_cascade_Fkeys(be, fk, tids);
				list_prepend(l, s);
			} else {
				switch (((sql_fkey*)fk)->on_delete) {
					case ACT_NO_ACTION:
						break;
					case ACT_SET_NULL:
					case ACT_SET_DEFAULT:
						s = sql_delete_set_Fkeys(be, fk, tids, ((sql_fkey*)fk)->on_delete);
						list_prepend(l, s);
						break;
					case ACT_CASCADE:
						s = sql_delete_cascade_Fkeys(be, fk, tids);
						list_prepend(l, s);
						break;
					default:	/*RESTRICT*/
						/* The overlap between deleted primaries and foreign should be empty */
						s = stmt_binop(be, stmt_aggr(be, tids, NULL, NULL, cnt, 1, 0, 1), stmt_atom_lng(be, 0), NULL, ne);
						msg = sa_message(sql->sa, SQLSTATE(40002) "%s: FOREIGN KEY constraint '%s.%s' violated", which, fk->t->base.name, fk->base.name);
						s = stmt_exception(be, s, msg, 00001);
						list_prepend(l, s);
				}
			}
		}
		list_destroy(keys);
	}
}

static int
sql_delete_keys(backend *be, sql_table *t, stmt *rows, list *l, char* which, int cascade)
{
	mvc *sql = be->mvc;
	int res = 1;
	node *n;

	if (!ol_length(t->keys))
		return res;

	for (n = ol_first_node(t->keys); n; n = n->next) {
		sql_key *k = n->data;

		if (k->type == pkey || k->type == ukey) {
			if (!(sql->cascade_action && list_find_id(sql->cascade_action, k->base.id))) {
				sqlid *local_id = SA_NEW(sql->sa, sqlid);
				if (!sql->cascade_action)
					sql->cascade_action = sa_list(sql->sa);

				*local_id = k->base.id;
				list_append(sql->cascade_action, local_id);
				sql_delete_ukey(be, rows, k, l, which, cascade);
			}
		}
	}
	return res;
}

static stmt *
sql_delete(backend *be, sql_table *t, stmt *rows)
{
	mvc *sql = be->mvc;
	stmt *v = NULL, *s = NULL;
	list *l = sa_list(sql->sa);
	stmt **deleted_cols = NULL;

	if (rows) {
		v = rows;
	} else { /* delete all */
		v = stmt_tid(be, t, 0);
	}

	/*  project all columns */
	if (ol_length(t->triggers) || partition_find_part(sql->session->tr, t, NULL)) {
		int nr = 0;
		deleted_cols = table_update_stmts(sql, t, &nr);
		int i = 0;
		for (node *n = ol_first_node(t->columns); n; n = n->next, i++) {
			sql_column *c = n->data;
			stmt *s = stmt_col(be, c, v, v->partition);

			deleted_cols[i] = s;
			list_append(l, s);
		}
	}

/* before */
#if 0
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_delete_triggers(be, up, v, deleted_cols, 0, 1, 3))
				return sql_error(sql, 02, SQLSTATE(27000) "DELETE: triggers failed for table '%s'", up->base.name);
		}
	}
#endif
	if (!sql_delete_triggers(be, t, v, deleted_cols, 0, 1, 3))
		return sql_error(sql, 02, SQLSTATE(27000) "DELETE: triggers failed for table '%s'", t->base.name);

	if (!sql_delete_keys(be, t, v, l, "DELETE", 0))
		return sql_error(sql, 02, SQLSTATE(42000) "DELETE: failed to delete indexes for table '%s'", t->base.name);

	if (rows) {
		list_append(l, stmt_delete(be, t, rows));
	} else { /* delete all */
		/* first column */
		s = stmt_table_clear(be, t);
		list_append(l, s);
	}

/* after */
#if 0
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_delete_triggers(be, up, v, deleted_cols, 1, 1, 3))
				return sql_error(sql, 02, SQLSTATE(27000) "DELETE: triggers failed for table '%s'", up->base.name);
		}
	}
#endif
	if (!sql_delete_triggers(be, t, v, deleted_cols, 1, 1, 3))
		return sql_error(sql, 02, SQLSTATE(27000) "DELETE: triggers failed for table '%s'", t->base.name);
	if (rows)
		s = stmt_aggr(be, rows, NULL, NULL, sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR), 1, 0, 1);
	if (be->cur_append) /* building the total number of rows affected across all tables */
		s->nr = add_to_merge_partitions_accumulator(be, s->nr);
	return s;
}

static stmt *
rel2bin_delete(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	stmt *rows = NULL, *stdelete = NULL;
	sql_rel *tr = rel->l;
	sql_table *t = NULL;

	if (tr->op == op_basetable)
		t = tr->l;
	else
		assert(0/*ddl statement*/);

	if (rel->r) { /* first construct the deletes relation */
		rows = subrel_bin(be, rel->r, refs);
		rows = subrel_project(be, rows, refs, rel->r);
		if (!rows)
			return NULL;
	}
	if (rows && rows->type == st_list) {
		stmt *s = rows;
		rows = s->op4.lval->h->data;
	}
	stdelete = sql_delete(be, t, rows);
	if (sql->cascade_action)
		sql->cascade_action = NULL;
	return stdelete;
}

struct tablelist {
	sql_table *table;
	struct tablelist* next;
};

static void /* inspect the other tables recursively for foreign key dependencies */
check_for_foreign_key_references(mvc *sql, struct tablelist* tlist, struct tablelist* next_append, sql_table *t, int cascade, int *error)
{
	node *n;
	int found;
	struct tablelist* new_node, *node_check;
	sql_trans *tr = sql->session->tr;

	if (THRhighwater()) {
		sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		*error = 1;
		return;
	}

	if (*error)
		return;

	sqlstore *store = sql->session->tr->store;
	if (t->keys) { /* Check for foreign key references */
		for (n = ol_first_node(t->keys); n; n = n->next) {
			sql_key *k = n->data;

			if (k->type == ukey || k->type == pkey) {
				list *keys = sql_trans_get_dependencies(tr, k->base.id, FKEY_DEPENDENCY, NULL);

				if (keys) {
					for (node *n = keys->h; n; n = n->next->next) {
						sqlid fkey_id = *(sqlid*)n->data;
						sql_base *b = os_find_id(tr->cat->objects, tr, fkey_id);
						sql_key *fk = (sql_key*)b;
						sql_fkey *rk = (sql_fkey*)b;

						if (fk->type != fkey || rk->rkey != k->base.id)
							continue;
						k = fk;
						/* make sure it is not a self referencing key */
						if (k->t != t && !cascade) {
							node *n = ol_first_node(t->columns);
							sql_column *c = n->data;
							size_t n_rows = store->storage_api.count_col(sql->session->tr, c, 0);
							size_t n_deletes = store->storage_api.count_del(sql->session->tr, c->t, 0);
							assert (n_rows >= n_deletes);
							if (n_rows - n_deletes > 0) {
								list_destroy(keys);
								sql_error(sql, 02, SQLSTATE(23000) "TRUNCATE: FOREIGN KEY %s.%s depends on %s", k->t->base.name, k->base.name, t->base.name);
								*error = 1;
								return;
							}
						} else if (k->t != t) {
							found = 0;
							for (node_check = tlist; node_check; node_check = node_check->next) {
								if (node_check->table == k->t)
									found = 1;
							}
							if (!found) {
								if ((new_node = SA_NEW(sql->ta, struct tablelist)) == NULL) {
									list_destroy(keys);
									sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
									*error = 1;
									return;
								}
								new_node->table = k->t;
								new_node->next = NULL;
								next_append->next = new_node;
								check_for_foreign_key_references(sql, tlist, new_node, k->t, cascade, error);
							}
						}
					}
					list_destroy(keys);
				}
			}
		}
	}
}

static stmt *
sql_truncate(backend *be, sql_table *t, int restart_sequences, int cascade)
{
	mvc *sql = be->mvc;
	list *l = sa_list(sql->sa);
	stmt *v, *ret = NULL, *other = NULL;
	const char *next_value_for = "next value for ";
	sql_column *col = NULL;
	sql_schema *sche = NULL;
	sql_table *next = NULL;
	sql_trans *tr = sql->session->tr;
	int error = 0;
	struct tablelist* new_list = SA_NEW(sql->ta, struct tablelist), *list_node;
	stmt **deleted_cols = NULL;

	if (!new_list) {
		sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		error = 1;
		goto finalize;
	}

	new_list->table = t;
	new_list->next = NULL;
	check_for_foreign_key_references(sql, new_list, new_list, t, cascade, &error);
	if (error)
		goto finalize;

	for (list_node = new_list; list_node; list_node = list_node->next) {
		next = list_node->table;
		sche = next->s;

		if (restart_sequences) { /* restart the sequences if it's the case */
			for (node *n = ol_first_node(next->columns); n; n = n->next) {
				col = n->data;

				if (col->def && !strncmp(col->def, next_value_for, strlen(next_value_for))) {
					sql_schema *s = NULL;
					sql_sequence *seq = NULL;
					char *schema = NULL, *seq_name = NULL;

					extract_schema_and_sequence_name(sql->ta, col->def + strlen(next_value_for), &schema, &seq_name);
					if (!schema || !seq_name || !(s = find_sql_schema(tr, schema)))
						continue;

					assert(s->base.id == sche->base.id);
					if ((seq = find_sql_sequence(tr, s, seq_name))) {
						if (!sql_trans_sequence_restart(tr, seq, seq->start)) {
							sql_error(sql, 02, SQLSTATE(HY005) "Could not restart sequence %s.%s", sche->base.name, seq_name);
							error = 1;
							goto finalize;
						}
					}
				}
			}
		}

		v = stmt_tid(be, next, 0);

		/*  project all columns */
		if (ol_length(t->triggers) || partition_find_part(sql->session->tr, t, NULL)) {
			int nr = 0;
			deleted_cols = table_update_stmts(sql, t, &nr);
			int i = 0;
			for (node *n = ol_first_node(t->columns); n; n = n->next, i++) {
				sql_column *c = n->data;
				stmt *s = stmt_col(be, c, v, v->partition);

				deleted_cols[i] = s;
				list_append(l, s);
			}
		}

		/* before */
#if 0
		if (be->cur_append && !be->first_statement_generated) {
			for (sql_table *up = t->p ; up ; up = up->p) {
				if (!sql_delete_triggers(be, up, v, deleted_cols, 0, 3, 4)) {
					sql_error(sql, 02, SQLSTATE(27000) "TRUNCATE: triggers failed for table '%s'", up->base.name);
					error = 1;
					goto finalize;
				}
			}
		}
#endif
		if (!sql_delete_triggers(be, next, v, deleted_cols, 0, 3, 4)) {
			sql_error(sql, 02, SQLSTATE(27000) "TRUNCATE: triggers failed for table '%s'", next->base.name);
			error = 1;
			goto finalize;
		}

		if (!sql_delete_keys(be, next, v, l, "TRUNCATE", cascade)) {
			sql_error(sql, 02, SQLSTATE(42000) "TRUNCATE: failed to delete indexes for table '%s'", next->base.name);
			error = 1;
			goto finalize;
		}

		other = stmt_table_clear(be, next);
		list_append(l, other);
		if (next == t)
			ret = other;

		/* after */
#if 0
		if (be->cur_append && !be->first_statement_generated) {
			for (sql_table *up = t->p ; up ; up = up->p) {
				if (!sql_delete_triggers(be, up, v, deleted_cols, 1, 3, 4)) {
					sql_error(sql, 02, SQLSTATE(27000) "TRUNCATE: triggers failed for table '%s'", up->base.name);
					error = 1;
					goto finalize;
				}
			}
		}
#endif
		if (!sql_delete_triggers(be, next, v, deleted_cols, 1, 3, 4)) {
			sql_error(sql, 02, SQLSTATE(27000) "TRUNCATE: triggers failed for table '%s'", next->base.name);
			error = 1;
			goto finalize;
		}

		if (be->cur_append) /* building the total number of rows affected across all tables */
			other->nr = add_to_merge_partitions_accumulator(be, other->nr);
	}

finalize:
	sa_reset(sql->ta);
	if (error)
		return NULL;
	return ret;
}

#define E_ATOM_INT(e) ((atom*)((sql_exp*)e)->l)->data.val.ival
#define E_ATOM_STRING(e) ((atom*)((sql_exp*)e)->l)->data.val.sval

static stmt *
rel2bin_truncate(backend *be, sql_rel *rel)
{
	mvc *sql = be->mvc;
	stmt *truncate = NULL;
	sql_rel *tr = rel->l;
	sql_table *t = NULL;
	node *n = NULL;
	int restart_sequences, cascade;

	if (tr->op == op_basetable)
		t = tr->l;
	else
		assert(0/*ddl statement*/);

	n = rel->exps->h;
	restart_sequences = E_ATOM_INT(n->data);
	cascade = E_ATOM_INT(n->next->data);

	truncate = sql_truncate(be, t, restart_sequences, cascade);
	if (sql->cascade_action)
		sql->cascade_action = NULL;
	return truncate;
}

static stmt *
rel2bin_output(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	node *n;
	const char *tsep, *rsep, *ssep, *ns;
	const char *fn   = NULL;
	int onclient = 0;
	stmt *s = NULL, *fns = NULL;
	list *slist = sa_list(sql->sa);

	if (rel->l)  /* first construct the sub relation */
		s = subrel_bin(be, rel->l, refs);
	s = subrel_project(be, s, refs, rel->l);
	if (!s)
		return NULL;

	if (!rel->exps)
		return s;
	n = rel->exps->h;
	tsep = sa_strdup(sql->sa, E_ATOM_STRING(n->data));
	rsep = sa_strdup(sql->sa, E_ATOM_STRING(n->next->data));
	ssep = sa_strdup(sql->sa, E_ATOM_STRING(n->next->next->data));
	ns   = sa_strdup(sql->sa, E_ATOM_STRING(n->next->next->next->data));

	if (n->next->next->next->next) {
		fn = E_ATOM_STRING(n->next->next->next->next->data);
		fns = stmt_atom_string(be, sa_strdup(sql->sa, fn));
		onclient = E_ATOM_INT(n->next->next->next->next->next->data);
	}
	list_append(slist, stmt_export(be, s, tsep, rsep, ssep, ns, onclient, fns));
	if (s->type == st_list && ((stmt*)s->op4.lval->h->data)->nrcols != 0) {
		stmt *cnt = stmt_aggr(be, s->op4.lval->h->data, NULL, NULL, sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR), 1, 0, 1);
		return cnt;
	} else {
		return stmt_atom_lng(be, 1);
	}
}

static stmt *
rel2bin_list(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	stmt *l = NULL, *r = NULL;
	list *slist = sa_list(sql->sa);

	(void)refs;

	if (find_prop(rel->p, PROP_DISTRIBUTE) && be->cur_append == 0) /* create affected rows accumulator */
		create_merge_partitions_accumulator(be);

	if (rel->l)  /* first construct the sub relation */
		l = subrel_bin(be, rel->l, refs);
	if (rel->r)  /* first construct the sub relation */
		r = subrel_bin(be, rel->r, refs);
	l = subrel_project(be, l, refs, rel->l);
	r = subrel_project(be, r, refs, rel->r);
	if (!l || !r)
		return NULL;
	list_append(slist, l);
	list_append(slist, r);
	return stmt_list(be, slist);
}

static stmt *
rel2bin_psm(backend *be, sql_rel *rel)
{
	mvc *sql = be->mvc;
	node *n;
	list *l = sa_list(sql->sa);
	stmt *sub = NULL;

	for (n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *s = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		if (!s)
			return NULL;

		if (s && s->type == st_table) /* relational statement */
			sub = s->op1;
		else
			append(l, s);
	}
	return stmt_list(be, l);
}

static stmt *
rel2bin_partition_limits(backend *be, sql_rel *rel, list *refs)
{
	stmt *l = NULL, *r = NULL;
	node *n = NULL;
	list *slist = sa_list(be->mvc->sa);

	if (rel->l)  /* first construct the sub relation */
		l = subrel_bin(be, rel->l, refs);
	if (rel->r)  /* first construct the sub relation */
		r = subrel_bin(be, rel->r, refs);
	l = subrel_project(be, l, refs, rel->l);
	r = subrel_project(be, r, refs, rel->r);
	if ((rel->l && !l) || (rel->r && !r))
		return NULL;

	assert(rel->exps);
	assert(rel->flag == ddl_alter_table_add_range_partition || rel->flag == ddl_alter_table_add_list_partition);

	if (rel->exps) {
		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			stmt *s = exp_bin(be, e, l, r, NULL, NULL, NULL, NULL, 0, 0, 0);
			if (!s)
				return NULL;
			append(slist, s);
		}
	}
	return stmt_catalog(be, rel->flag, stmt_list(be, slist));
}

static stmt *
rel2bin_exception(backend *be, sql_rel *rel, list *refs)
{
	stmt *l = NULL, *r = NULL;
	node *n = NULL;
	list *slist = sa_list(be->mvc->sa);

	if (find_prop(rel->p, PROP_DISTRIBUTE) && be->cur_append == 0) /* create affected rows accumulator */
		create_merge_partitions_accumulator(be);

	if (rel->l)  /* first construct the sub relation */
		l = subrel_bin(be, rel->l, refs);
	if (rel->r)  /* first construct the sub relation */
		r = subrel_bin(be, rel->r, refs);
	l = subrel_project(be, l, refs, rel->l);
	r = subrel_project(be, r, refs, rel->r);
	if ((rel->l && !l) || (rel->r && !r))
		return NULL;

	if (rel->exps) {
		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			stmt *s = exp_bin(be, e, l, r, NULL, NULL, NULL, NULL, 0, 0, 0);
			if (!s)
				return NULL;
			append(slist, s);
		}
	} else { /* if there is no exception condition, just generate a statement list */
		list_append(slist, l);
		list_append(slist, r);
	}
	return stmt_list(be, slist);
}

static stmt *
rel2bin_seq(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	node *en = rel->exps->h;
	stmt *restart, *sname, *seq, *seqname, *sl = NULL;
	list *l = sa_list(sql->sa);

	if (rel->l) { /* first construct the sub relation */
		sl = subrel_bin(be, rel->l, refs);
		sl = subrel_project(be, sl, refs, rel->l);
		if (!sl)
			return NULL;
	}

	restart = exp_bin(be, en->data, sl, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	sname = exp_bin(be, en->next->data, sl, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	seqname = exp_bin(be, en->next->next->data, sl, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	seq = exp_bin(be, en->next->next->next->data, sl, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	if (!restart || !sname || !seqname || !seq)
		return NULL;

	(void)refs;
	append(l, sname);
	append(l, seqname);
	append(l, seq);
	append(l, restart);
	return stmt_catalog(be, rel->flag, stmt_list(be, l));
}

static stmt *
rel2bin_trans(backend *be, sql_rel *rel, list *refs)
{
	node *en = rel->exps->h;
	stmt *chain = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	stmt *name = NULL;

	if (!chain)
		return NULL;

	(void)refs;
	if (en->next) {
		name = exp_bin(be, en->next->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		if (!name)
			return NULL;
	}
	return stmt_trans(be, rel->flag, chain, name);
}

static stmt *
rel2bin_catalog_schema(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	node *en = rel->exps->h;
	stmt *action = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	stmt *sname = NULL, *name = NULL, *ifexists = NULL;
	list *l = sa_list(sql->sa);

	if (!action)
		return NULL;

	(void)refs;
	en = en->next;
	sname = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	if (!sname)
		return NULL;
	append(l, sname);
	en = en->next;
	if (rel->flag == ddl_create_schema) {
		if (en) {
			name = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
			if (!name)
				return NULL;
		} else {
			name = stmt_atom_string_nil(be);
		}
		append(l, name);
	} else {
		assert(rel->flag == ddl_drop_schema);
		ifexists = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		if (!ifexists)
			return NULL;
		append(l, ifexists);
	}
	append(l, action);
	return stmt_catalog(be, rel->flag, stmt_list(be, l));
}

static stmt *
rel2bin_catalog_table(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	node *en = rel->exps->h;
	stmt *action = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	stmt *table = NULL, *sname, *tname = NULL, *kname = NULL, *ifexists = NULL;
	list *l = sa_list(sql->sa);

	if (!action)
		return NULL;

	(void)refs;
	en = en->next;
	sname = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	if (!sname)
		return NULL;
	en = en->next;
	if (en) {
		tname = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		if (!tname)
			return NULL;
		en = en->next;
	}
	append(l, sname);
	assert(tname);
	append(l, tname);
	if (rel->flag == ddl_drop_constraint) { /* needs extra string parameter for constraint name */
		if (en) {
			kname = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
			if (!kname)
				return NULL;
			en = en->next;
		}
		append(l, kname);
	}
	if (rel->flag != ddl_drop_table && rel->flag != ddl_drop_view && rel->flag != ddl_drop_constraint) {
		if (en) {
			table = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
			if (!table)
				return NULL;
		}
		append(l, table);
	} else {
		if (en) {
			ifexists = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
			if (!ifexists)
				return NULL;
		} else {
			ifexists = stmt_atom_int(be, 0);
		}
		append(l, ifexists);
	}
	append(l, action);
	return stmt_catalog(be, rel->flag, stmt_list(be, l));
}

static stmt *
rel2bin_catalog2(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	node *en;
	list *l = sa_list(sql->sa);

	(void)refs;
	for (en = rel->exps->h; en; en = en->next) {
		stmt *es = NULL;

		if (en->data) {
			es = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
			if (!es)
				return NULL;
		} else {
			es = stmt_atom_string_nil(be);
		}
		append(l,es);
	}
	return stmt_catalog(be, rel->flag, stmt_list(be, l));
}

static stmt *
rel2bin_ddl(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	stmt *s = NULL;

	switch (rel->flag) {
		case ddl_output:
			s = rel2bin_output(be, rel, refs);
			sql->type = Q_TABLE;
			break;
		case ddl_list:
			s = rel2bin_list(be, rel, refs);
			break;
		case ddl_psm:
			s = rel2bin_psm(be, rel);
			break;
		case ddl_exception:
			s = rel2bin_exception(be, rel, refs);
			break;
		case ddl_create_seq:
		case ddl_alter_seq:
			s = rel2bin_seq(be, rel, refs);
			sql->type = Q_SCHEMA;
			break;
		case ddl_alter_table_add_range_partition:
		case ddl_alter_table_add_list_partition:
			s = rel2bin_partition_limits(be, rel, refs);
			sql->type = Q_SCHEMA;
			break;
		case ddl_release:
		case ddl_commit:
		case ddl_rollback:
		case ddl_trans:
			s = rel2bin_trans(be, rel, refs);
			sql->type = Q_TRANS;
			break;
		case ddl_create_schema:
		case ddl_drop_schema:
			s = rel2bin_catalog_schema(be, rel, refs);
			sql->type = Q_SCHEMA;
			break;
		case ddl_create_table:
		case ddl_drop_table:
		case ddl_create_view:
		case ddl_drop_view:
		case ddl_drop_constraint:
		case ddl_alter_table:
			s = rel2bin_catalog_table(be, rel, refs);
			sql->type = Q_SCHEMA;
			break;
		case ddl_drop_seq:
		case ddl_create_type:
		case ddl_drop_type:
		case ddl_drop_index:
		case ddl_create_function:
		case ddl_drop_function:
		case ddl_create_trigger:
		case ddl_drop_trigger:
		case ddl_grant_roles:
		case ddl_revoke_roles:
		case ddl_grant:
		case ddl_revoke:
		case ddl_grant_func:
		case ddl_revoke_func:
		case ddl_create_user:
		case ddl_drop_user:
		case ddl_alter_user:
		case ddl_rename_user:
		case ddl_create_role:
		case ddl_drop_role:
		case ddl_alter_table_add_table:
		case ddl_alter_table_del_table:
		case ddl_alter_table_set_access:
		case ddl_comment_on:
		case ddl_rename_schema:
		case ddl_rename_table:
		case ddl_rename_column:
			s = rel2bin_catalog2(be, rel, refs);
			sql->type = Q_SCHEMA;
			break;
		default:
			assert(0);
	}
	return s;
}

static stmt *
subrel_bin(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	stmt *s = NULL;

	if (THRhighwater())
		return sql_error(be->mvc, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!rel)
		return s;
	if (rel_is_ref(rel)) {
		s = refs_find_rel(refs, rel);
		/* needs a proper fix!! */
		if (s)
			return s;
	}
	switch (rel->op) {
	case op_basetable:
		s = rel2bin_basetable(be, rel);
		sql->type = Q_TABLE;
		break;
	case op_table:
		s = rel2bin_table(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		s = rel2bin_join(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_semi:
	case op_anti:
		s = rel2bin_semijoin(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_union:
		s = rel2bin_union(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_except:
		s = rel2bin_except(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_inter:
		s = rel2bin_inter(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_project:
		s = rel2bin_project(be, rel, refs, NULL);
		sql->type = Q_TABLE;
		break;
	case op_select:
		s = rel2bin_select(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_groupby:
		s = rel2bin_groupby(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_topn:
		s = rel2bin_topn(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_sample:
		s = rel2bin_sample(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_insert:
		s = rel2bin_insert(be, rel, refs);
		if (sql->type == Q_TABLE)
			sql->type = Q_UPDATE;
		break;
	case op_update:
		s = rel2bin_update(be, rel, refs);
		if (sql->type == Q_TABLE)
			sql->type = Q_UPDATE;
		break;
	case op_delete:
		s = rel2bin_delete(be, rel, refs);
		if (sql->type == Q_TABLE)
			sql->type = Q_UPDATE;
		break;
	case op_truncate:
		s = rel2bin_truncate(be, rel);
		if (sql->type == Q_TABLE)
			sql->type = Q_UPDATE;
		break;
	case op_ddl:
		s = rel2bin_ddl(be, rel, refs);
		break;
	}
	if (s && rel_is_ref(rel)) {
		list_append(refs, rel);
		list_append(refs, s);
	}
	return s;
}

stmt *
rel_bin(backend *be, sql_rel *rel)
{
	mvc *sql = be->mvc;
	list *refs = sa_list(sql->sa);
	mapi_query_t sqltype = sql->type;
	stmt *s = subrel_bin(be, rel, refs);

	s = subrel_project(be, s, refs, rel);
	if (sqltype == Q_SCHEMA)
		sql->type = sqltype;  /* reset */

	return s;
}

stmt *
output_rel_bin(backend *be, sql_rel *rel )
{
	mvc *sql = be->mvc;
	list *refs = sa_list(sql->sa);
	mapi_query_t sqltype = sql->type;
	stmt *s;

	be->join_idx = 0;
	if (refs == NULL)
		return NULL;
	s = subrel_bin(be, rel, refs);
	s = subrel_project(be, s, refs, rel);
	if (sqltype == Q_SCHEMA)
		sql->type = sqltype;  /* reset */

	if (!is_ddl(rel->op) && s && s->type != st_none && sql->type == Q_TABLE)
		s = stmt_output(be, s);
	if (sqltype == Q_UPDATE && s && (s->type != st_list || be->cur_append)) {
		if (be->cur_append) { /* finish the output bat */
			s->nr = be->cur_append;
			be->cur_append = 0;
			be->first_statement_generated = false;
		}
		s = stmt_affected_rows(be, s);
	}
	return s;
}
