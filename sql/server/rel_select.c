/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "rel_select.h"
#include "sql_tokens.h"
#include "sql_privileges.h"
#include "sql_env.h"
#include "sql_decimal.h"
#include "sql_qc.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_xml.h"
#include "rel_dump.h"
#include "rel_prop.h"
#include "rel_psm.h"
#include "rel_schema.h"
#include "rel_unnest.h"
#include "rel_sequence.h"
#include "rel_file_loader.h"
#include "rel_optimizer_private.h"

#define VALUE_FUNC(f) (f->func->type == F_FUNC || f->func->type == F_FILT)
#define check_card(card,f) ((card == card_none && !f->res) || (CARD_VALUE(card) && f->res && VALUE_FUNC(f)) || card == card_loader || (card == card_relation && f->func->type == F_UNION))

/* return all expressions, with table name == tname */
static list *
rel_table_projections( mvc *sql, sql_rel *rel, char *tname, int level )
{
	list *exps;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!rel)
		return NULL;

	if (!tname)
		return _rel_projections(sql, rel, NULL, 1, 0, 1);

	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		exps = rel_table_projections( sql, rel->l, tname, level+1);
		if (exps)
			return exps;
		return rel_table_projections( sql, rel->r, tname, level+1);
	case op_semi:
	case op_anti:
	case op_select:
		return rel_table_projections( sql, rel->l, tname, level+1);

	case op_topn:
	case op_sample:
	case op_groupby:
	case op_union:
	case op_except:
	case op_inter:
	case op_project:
		if (!is_processed(rel) && level == 0)
			return rel_table_projections( sql, rel->l, tname, level+1);
		/* fall through */
	case op_munion:
		if (!is_processed(rel) && level == 0) {
			node *n = ((list*)rel->l)->h;
			if (n)
				return rel_table_projections(sql, n->data, tname, level+1);
		}
	/* fall through */
	case op_table:
	case op_basetable:
		if (is_basetable(rel->op) && !rel->exps)
			return rel_base_project_all(sql, rel, tname);
		if (rel->exps) {
			int rename = 0;
			node *en;

			/* first check alias */
			if (!is_base(rel->op) && !level) {
				list *exps = sa_list(sql->sa);

				for (en = rel->exps->h; en && !rename; en = en->next) {
					sql_exp *e = en->data;;

					if ((is_basecol(e) && exp_relname(e) && strcmp(exp_relname(e), tname) == 0) ||
					    (is_basecol(e) && !exp_relname(e) && e->l && strcmp(e->l, tname) == 0)) {
						if (exp_name(e) && exps_bind_column2(exps, tname, exp_name(e), NULL))
							rename = 1;
						else
							append(exps, e);
					}
				}
			}

			exps = new_exp_list(sql->sa);
			for (en = rel->exps->h; en; en = en->next) {
				sql_exp *e = en->data;
				if (is_basecol(e) && exp_relname(e) && strcmp(exp_relname(e), tname) == 0) {
					if (rename)
						append(exps, exp_alias_ref(sql, e));
					else {
						sql_exp *ne = exp_ref(sql, e);
						exp_setname(sql, ne, tname, exp_name(e));
						append(exps, ne);
					}
				}
				if (is_basecol(e) && !exp_relname(e) && e->l && strcmp(e->l, tname) == 0) {
					if (rename)
						append(exps, exp_alias_ref(sql, e));
					else {
						sql_exp *ne = exp_ref(sql, e);
						exp_setname(sql, ne, tname, exp_name(e));
						append(exps, ne);
					}
				}

			}
			if (exps && list_length(exps))
				return exps;
		}
		/* fall through */
	default:
		return NULL;
	}
}

static sql_exp *
rel_lastexp(mvc *sql, sql_rel *rel )
{
	sql_exp *e;

	if (!is_processed(rel) || is_topn(rel->op) || is_sample(rel->op))
		rel = rel_parent(rel);
	assert(list_length(rel->exps));
	if (rel->op == op_project) {
		list_hash_clear(rel->exps);
		return exp_ref(sql, rel->exps->t->data);
	}
	assert(is_project(rel->op));
	e = rel->exps->t->data;
	return exp_ref(sql, e);
}

static sql_rel *
rel_orderby(mvc *sql, sql_rel *l)
{
	sql_rel *rel = rel_create(sql->sa);
	if (!rel)
		return NULL;

	assert(l->op == op_project && !l->r);
	rel->l = l;
	rel->r = NULL;
	rel->op = op_project;
	rel->exps = rel_projections(sql, l, NULL, 1, 0);
	rel->card = l->card;
	rel->nrcols = l->nrcols;
	return rel;
}

/* forward refs */
static sql_rel * rel_setquery(sql_query *query, symbol *sq);
static sql_rel * rel_joinquery(sql_query *query, symbol *sq, list *refs);

static sql_rel *
rel_table_optname(mvc *sql, sql_rel *sq, symbol *optname, list *refs)
{
	sql_rel *osq = sq;
	node *ne;

	if (optname && optname->token == SQL_NAME) {
		dlist *columnrefs = NULL;
		char *tname = optname->data.lval->h->data.sval;
		list *l = sa_list(sql->sa);

		columnrefs = optname->data.lval->h->next->data.lval;
		if (is_topn(sq->op) || is_sample(sq->op) || ((is_simple_project(sq->op) || is_groupby(sq->op)) && sq->r) || is_base(sq->op)) {
			sq = rel_project(sql->sa, sq, rel_projections(sql, sq, NULL, 1, 0));
			osq = sq;
		}
		if (columnrefs && dlist_length(columnrefs) != list_length(sq->exps))
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: The number of aliases don't match the number of columns (%d != %d)", dlist_length(columnrefs), sq->nrcols);
		if (columnrefs && sq->exps) {
			dnode *d = columnrefs->h;

			ne = sq->exps->h;
			list_hash_clear(sq->exps);
			for (; d && ne; d = d->next, ne = ne->next) {
				sql_exp *e = ne->data;

				if (exps_bind_column2(l, tname, d->data.sval, NULL))
					return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: Duplicate column name '%s.%s'", tname, d->data.sval);
				exp_setname(sql, e, tname, d->data.sval );
				if (!is_intern(e))
					set_basecol(e);
				append(l, e);
			}
		}
		if (!columnrefs && sq->exps) {
			ne = sq->exps->h;
			list_hash_clear(sq->exps);
			for (; ne; ne = ne->next) {
				sql_exp *e = ne->data;
				char *name = NULL;

				if (!is_intern(e)) {
					if (!exp_name(e))
						name = make_label(sql->sa, ++sql->label);
					noninternexp_setname(sql, e, tname, name);
					set_basecol(e);
				}
			}
		}
		if (refs) { /* if this relation is under a FROM clause, check for duplicate names */
			if (list_find(refs, tname, (fcmp) &strcmp))
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: relation name \"%s\" specified more than once", tname);
			assert(tname);
			list_append(refs, tname);
		}
	} else {
		if (!is_project(sq->op) || is_topn(sq->op) || is_sample(sq->op) || ((is_simple_project(sq->op) || is_groupby(sq->op)) && sq->r)) {
			sq = rel_project(sql->sa, sq, rel_projections(sql, sq, NULL, 1, 1));
			osq = sq;
		}
		for (ne = osq->exps->h; ne; ne = ne->next) {
			sql_exp *e = ne->data;

			if (!is_intern(e))
				set_basecol(e);
		}
	}
	return osq;
}

static sql_rel *
rel_subquery_optname(sql_query *query, symbol *ast, list *refs)
{
	mvc *sql = query->sql;
	SelectNode *sn = (SelectNode *) ast;
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_rel *sq = rel_subquery(query, ast, ek);

	assert(ast->token == SQL_SELECT);
	if (!sq)
		return NULL;

	return rel_table_optname(sql, sq, sn->name, refs);
}

static void
rel_rename(mvc *sql, sql_rel *nrel, char *rname, sql_rel *brel)
{
	assert(is_project(nrel->op));
	if (brel) {
		if (is_project(nrel->op) && nrel->exps) {
			for (node *ne = nrel->exps->h, *be = brel->exps->h; ne && be; ne = ne->next, be = be->next) {
				sql_exp *e = ne->data;
				sql_exp *b = be->data;
				char *name = NULL;

				if (!is_intern(e)) {
					if (!exp_name(e))
						name = make_label(sql->sa, ++sql->label);
					noninternexp_setname(sql, e, rname, name);
					set_basecol(e);
					e->alias.label = b->alias.label;
				}
			}
		}
		list_hash_clear(nrel->exps);
	} else if (is_project(nrel->op) && nrel->exps) {
		node *ne = nrel->exps->h;

		for (; ne; ne = ne->next) {
			sql_exp *e = ne->data;
			char *name = NULL;

			if (!is_intern(e)) {
				if (!exp_name(e))
					name = make_label(sql->sa, ++sql->label);
				noninternexp_setname(sql, e, rname, name);
				set_basecol(e);
			}
		}
		list_hash_clear(nrel->exps);
	}
}

sql_rel *
rel_with_query(sql_query *query, symbol *q )
{
	mvc *sql = query->sql;
	dnode *d = q->data.lval->h;
	symbol *next = d->next->data.sym;
	bool recursive = d->next->next->data.i_val;
	sql_rel *rel;

	if (!stack_push_frame(sql, NULL))
		return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	/* first handle all with's (ie inlined views) */
	for (d = d->data.lval->h; d; d = d->next) {
		symbol *sym = d->data.sym;
		dnode *dn = sym->data.lval->h->next;
		char *rname = qname_schema_object(dn->data.lval);
		sql_rel *nrel, *base_rel = NULL;
		symbol *recursive_part = NULL;
		sql_rel_view *recursive_union = NULL;
		int recursive_distinct = 0;

		if (frame_find_rel_view(sql, rname)) {
			stack_pop_frame(sql);
			return sql_error(sql, 01, SQLSTATE(42000) "View '%s' already declared", rname);
		}
		if (recursive) {
			symbol *union_stmt = dn->next->next->data.sym;
			if (union_stmt->token == SQL_UNION) { /* split in base and recursive part */
				dnode *n = union_stmt->data.lval->h;
				symbol *base = n->data.sym;
				recursive_distinct = n->next->data.i_val;
				dlist *corresponding = n->next->next->data.lval;
				recursive_part = n->next->next->next->data.sym;
				if (corresponding)
					return sql_error(sql, 01, SQLSTATE(42000) "Recursive with corresponding is not supported");
				dn->next->next->data.sym = base;
			}
		}
		nrel = rel_semantic(query, sym);
		if (!nrel) {
			stack_pop_frame(sql);
			return NULL;
		}
		if (!(recursive_union = stack_push_rel_view(sql, rname, nrel))) {
			stack_pop_frame(sql);
			return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if (recursive && recursive_part) {
			base_rel = nrel;
			rel_rename(sql, base_rel, rname, base_rel);
			dn->next->next->data.sym = recursive_part;
			set_processed(nrel);
			nrel = rel_semantic(query, sym);
			if (!nrel) {
				stack_pop_frame(sql);
				return NULL;
			}
			list *ls = rel_projections(sql, base_rel, NULL, 0, 1);
			list *rs = rel_projections(sql, nrel, NULL, 0, 1);

			if (!rel_is_ref(base_rel)) { /* not recursive */
				nrel = rel_setop_n_ary_check_types(sql, base_rel, nrel, ls, rs, op_munion);
			} else {
				base_rel->used |= statistics_gathered;
				prop *p = base_rel->p = prop_create(sql->sa, PROP_COUNT, base_rel->p);
				p->value.lval = 1000000; /* random ? */

				/* down cast the recursive side (on errors users should add casts on the base side) */
				list *nrs = new_exp_list(sql->sa);
				if(!nrs)
					return NULL;

				for (node *n = ls->h, *m = rs->h; n && m; n = n->next, m = m->next) {
					sql_subtype *t = exp_subtype(n->data);
					append(nrs, exp_check_type(sql, t, nrel, m->data, type_equal));
				}
				nrel = rel_project(sql->sa, nrel, nrs);
				nrel = rel_setop_n_ary(sql->sa, append(append(sa_list(sql->sa), base_rel), nrel), op_munion);
				set_recursive(nrel);
			}
			if (recursive_distinct)
				set_distinct(nrel);
			rel_setop_n_ary_set_exps(sql, nrel, rel_projections(sql, nrel, NULL, 0, 1), false);
			set_processed(nrel);
			recursive_union->rel_view = nrel;
		}
		if (!is_project(nrel->op)) {
			if (is_topn(nrel->op) || is_sample(nrel->op)) {
				nrel = rel_project(sql->sa, nrel, rel_projections(sql, nrel, NULL, 1, 1));
			} else {
				stack_pop_frame(sql);
				return NULL;
			}
		}
		rel_rename(sql, nrel, rname, base_rel);
	}
	rel = rel_semantic(query, next);
	stack_pop_frame(sql);
	return rel;
}

static sql_rel *
query_exp_optname(sql_query *query, symbol *q, list *refs)
{
	mvc *sql = query->sql;
	switch (q->token) {
	case SQL_WITH:
	{
		sql_rel *tq = rel_with_query(query, q);

		if (!tq)
			return NULL;
		if (q->data.lval->t->type == type_symbol)
			return rel_table_optname(sql, tq, q->data.lval->t->data.sym, refs);
		return tq;
	}
	case SQL_JOIN:
	{
		sql_rel *tq = rel_joinquery(query, q, refs);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym, NULL);
	}
	default:
		(void) sql_error(sql, 02, SQLSTATE(42000) "case %d %s", (int) q->token, token2string(q->token));
	}
	return NULL;
}

static sql_subfunc *
bind_func_(mvc *sql, char *sname, char *fname, list *ops, sql_ftype type, bool private, bool *found, bool exact)
{
	sql_subfunc *sf = NULL;

	if (sql->forward && strcmp(fname, sql->forward->base.name) == 0 &&
	    list_cmp(sql->forward->ops, ops, (fcmp)&arg_subtype_cmp) == 0 &&
	    execute_priv(sql, sql->forward) && type == sql->forward->type)
		return sql_dup_subfunc(sql->sa, sql->forward, NULL, NULL);
	sf = sql_bind_func_(sql, sname, fname, ops, type, private, exact);
	if (found)
		*found |= sf != NULL;
	if (sf && execute_priv(sql, sf->func))
		return sf;
	return NULL;
}

static sql_subfunc *
bind_func(mvc *sql, char *sname, char *fname, sql_subtype *t1, sql_subtype *t2, int nr, sql_ftype type, bool private, bool *found, bool exact)
{
	list *tl = sa_list(sql->sa);
	assert(nr >= 1 && nr <= 2);
	append(tl, t1);
	if (nr == 2)
		append(tl, t2);
	sql_subfunc *sf = NULL;

	if (sql->forward) {
		if (execute_priv(sql, sql->forward) &&
		    strcmp(fname, sql->forward->base.name) == 0 &&
		   ((!t1 && list_length(sql->forward->ops) == 0) ||
		    (!t2 && list_length(sql->forward->ops) == 1 && arg_subtype_cmp(sql->forward->ops->h->data, t1) == 0) ||
		    (list_length(sql->forward->ops) == 2 &&
			arg_subtype_cmp(sql->forward->ops->h->data, t1) == 0 &&
			arg_subtype_cmp(sql->forward->ops->h->next->data, t2) == 0)) && type == sql->forward->type) {
			return sql_dup_subfunc(sql->sa, sql->forward, NULL, NULL);
		}
	}
	sf = sql_bind_func_(sql, sname, fname, tl, type, private, exact);
	if (found)
		*found |= sf != NULL;
	if (sf && execute_priv(sql, sf->func))
		return sf;
	return NULL;
}

static sql_subfunc *
find_func(mvc *sql, char *sname, char *fname, int len, sql_ftype type, bool private, sql_subfunc *prev, bool *found)
{
	sql_subfunc *sf = NULL;

	if (sql->forward && strcmp(fname, sql->forward->base.name) == 0 && list_length(sql->forward->ops) == len && execute_priv(sql, sql->forward) && type == sql->forward->type)
		return sql_dup_subfunc(sql->sa, sql->forward, NULL, NULL);
	sf = sql_find_func(sql, sname, fname, len, type, private, prev);
	if (found)
		*found |= sf != NULL;
	if (sf && execute_priv(sql, sf->func))
		return sf;
	return NULL;
}

static sql_exp *
exp_fix_scale(mvc *sql, sql_subtype *ct, sql_exp *e)
{
	sql_subtype *et = exp_subtype(e);

	if (ct->type->scale == SCALE_FIX && et->type->scale == SCALE_FIX) {
		int scale_diff = ((int) ct->scale - (int) et->scale);

		if (scale_diff) {
			if (scale_diff < 0)
				return e;
			sql_subtype st;
			int scale = ct->scale;
			int digits = ((et->type->eclass == EC_NUM)?bits2digits(et->digits):et->digits)-et->scale+scale;
			(void)sql_find_subtype(&st, ct->type->base.name, digits, scale);
			return exp_convert(sql, e, et, &st);
		}
	}
	return e;
}

static lng
rel_get_count(sql_rel *rel)
{
	if (!rel)
		return 0;
	prop *p = NULL;
	if (rel->p && (p = find_prop(rel->p, PROP_COUNT)) != NULL)
		return p->value.lval;
	else if(is_munion(rel->op)) {
		lng cnt = 0;
		list *l = rel->l;
		for (node *n = l->h; n; n = n->next) {
			lng lcnt = rel_get_count(n->data);
			if (lcnt == BUN_MAX)
				return BUN_MAX;
			cnt += lcnt;
		}
		return cnt;
	} else if(rel->l) {
		if (is_select(rel->op) || is_project(rel->op))
			return rel_get_count(rel->l);
	}
	return 0;
}

#define is_sum_aggr(f) (f->type == F_AGGR && strcmp(f->base.name, "sum") == 0)

list *
check_arguments_and_find_largest_any_type(mvc *sql, sql_rel *rel, list *exps, sql_subfunc *sf, int maybe_zero_or_one, bool internal)
{
	list *nexps = new_exp_list(sql->sa);
	sql_subtype *atp = NULL, super, *res = !list_empty(sf->res) ? sf->res->h->data: NULL;
	unsigned int digits = 0, scale = 0;

	/* find largest any type argument */
	for (node *n = exps->h, *m = sf->func->ops->h; n && m; n = n->next, m = m->next) {
		sql_arg *a = m->data;
		sql_exp *e = n->data;
		sql_subtype *t = exp_subtype(e);

		if (a->type.type->eclass == EC_ANY) {
			if (t && atp) {
				supertype(&super, t, atp);
				atp = &super;
			} else if (t) {
				atp = t;
			}
		}
		if (t && sf->func->fix_scale == SCALE_FIX && t->type->eclass == EC_DEC) {
			if (digits < t->digits)
				digits = t->digits;
			if (scale < t->scale)
				scale = t->scale;
		}
	}
	if (!atp && !list_empty(exps))
		atp = exp_subtype(exps->h->data);

	if ((atp && atp->type->localtype == TYPE_void) || !atp) /* NULL */
		atp = sql_bind_localtype("str");

	node *n, *m;
	sql_arg *last = NULL;
	for (n = exps->h, m = sf->func->ops->h; n && ((sf->func->vararg && last) || m); n = n->next, m = m?m->next:NULL) {
		sql_arg *a = m?m->data:last;
		sql_exp *e = n->data;
		sql_subtype *ntp = &a->type, *t = exp_subtype(e);

		last = a;
		if (!t) {
			if (a->type.type->eclass == EC_ANY && atp)
				ntp = sql_create_subtype(sql->sa, atp->type, atp->digits, atp->scale);
			rel_set_type_param(sql, ntp, rel, e, sf->func->fix_scale != INOUT && !UDF_LANG(sf->func->lang));
		} else if (a->type.type->eclass == EC_ANY && atp) {
			ntp = sql_create_subtype(sql->sa, atp->type, atp->digits, atp->scale);
		} else if (t && ntp->digits == 0 && EC_VARCHAR(a->type.type->eclass)) {
			ntp = sql_create_subtype(sql->sa, a->type.type, type_digits_to_char_digits(t), 0);
		} else if (t && ntp->digits > 0 && a->type.type->eclass == EC_NUM && t->type->eclass == EC_NUM) {
			ntp = sql_create_subtype(sql->sa, a->type.type, t->digits, 0);
		} else if (t && ntp->scale == 0 && ntp->type->eclass == EC_DEC && EC_VARCHAR(t->type->eclass)) {
			sql_subtype *res = SA_NEW(sql->sa, sql_subtype);
			int digits = t->digits?t->digits+3:ntp->digits;
			(void)sql_find_subtype(res, a->type.type->base.name, digits, 3);
			ntp = res;
		} else if (t && ntp->scale == 0 && ntp->type->eclass == EC_DEC) {
			ntp = sql_create_subtype(sql->sa, a->type.type, t->type->eclass == EC_NUM?bits2digits(t->digits):t->digits, t->scale);
		} else if (t->type == ntp->type) {
			ntp = t;
		}
		if (!(e = exp_check_type(sql, ntp, rel, e, type_equal)))
			return NULL;
		if (sf->func->fix_scale == SCALE_FIX) {
			ntp = sql_create_subtype(sql->sa, a->type.type->localtype?a->type.type:t?t->type:atp->type, digits, scale);
			e = exp_fix_scale(sql, ntp, e);
		} else if (sf->func->fix_scale == SCALE_EQ) {
			e = exp_fix_scale(sql, &a->type, e);
		}
		if (maybe_zero_or_one && e->card > CARD_ATOM) {
			sql_subfunc *zero_or_one = sql_bind_func(sql, "sys", "zero_or_one", exp_subtype(e), NULL, F_AGGR, true, false);
			e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, has_nil(e));
		}
		append(nexps, e);
	}
	/* handle any extra arguments for rel_dump/analytic funcs */
	for ( ; n; n = n->next)
		append(nexps, n->data);
	if (sf->func->fix_scale == SCALE_FIX || IS_ANALYTIC(sf->func)) {
		exps_scale_fix(sf, nexps, atp);
	} else if (sf->func->fix_scale == MAX_BITS) {
		exps_max_bits(sf, nexps);
	} else if (sf->func->fix_scale == SCALE_MUL) {
		exps_sum_scales(sf, nexps);
	} else if (!internal && sf->func->fix_scale == SCALE_DIV) {
		if (!exps_scale_algebra(sql, sf, rel, nexps))
			return NULL;
	} else if (sf->func->fix_scale == DIGITS_ADD) {
		exps_digits_add(sf, nexps);
	} else if (sf->func->fix_scale == INOUT) {
		exps_inout(sf, nexps);
	} else if (is_sum_aggr(sf->func))
		exps_largest_int(sf, nexps, rel_get_count(rel));

	/* dirty hack */
	if (sf->func->type != F_PROC && sf->func->type != F_UNION && sf->func->type != F_LOADER && res) {
		if (res->type->eclass == EC_ANY && atp)
			sf->res->h->data = sql_create_subtype(sql->sa, atp->type, atp->digits, atp->scale);
	}
	return nexps;
}

static char *
nary_function_arg_types_2str(mvc *sql, list* types, int N)
{
	char *arg_list = NULL;
	int i = 0;

	for (node *n = types->h; n && i < N; n = n->next) {
		sql_subtype *t = (sql_subtype *) n->data;
		char *tpe = t ? sql_subtype_string(sql->ta, t) : "?";

		if (arg_list) {
			arg_list = sa_message(sql->ta, "%s, %s", arg_list, tpe);
		} else {
			arg_list = tpe;
		}
		i++;
	}
	return arg_list;
}

static char *
file_loader_add_table_column_types(mvc *sql, sql_subfunc *f, list *exps, list *res_exps, char *tname)
{
	sql_exp *file = exps->h->data;
	if (!exp_is_atom(file))
		return "Filename missing";

	atom *a = file->l;
	if (a->data.vtype != TYPE_str || !a->data.val.sval)
		return "Filename missing";

	char *filename = a->data.val.sval;
	if (strcmp(filename, "") == 0)
		return "Filename missing";

	char *ext = strrchr(filename, '.'), *ep = ext;

	if (ext) {
		ext = ext + 1;
		ext = mkLower(sa_strdup(sql->sa, ext));
	}
	if (!ext)
		return "Filename extension missing";

	file_loader_t *fl = fl_find(ext);
	if (!fl) {
		/* maybe compressed */
		char *p = ep - 1;
		while (p > filename && *p != '.')
			p--;
		if (p != filename) {
			ext = p + 1;
			ext = sa_strdup(sql->sa, ext);
			char *d = strchr(ext, '.');
			assert(d);
			*d = 0;
			fl = fl_find(ext);
		}
		if (!fl) /* fallback */
			fl = fl_find("csv");
		if (!fl) /* not expected */
			return sa_message(sql->ta, "Filename extension '%s' missing", ext?ext:"");
	}
	str err = fl->add_types(sql, f, filename, res_exps, tname);
	if (err)
		return err;
	sql_subtype *st = sql_bind_localtype("str");
	sql_exp *ext_exp = exp_atom(sql->sa, atom_string(sql->sa, st, ext));
	if (!ext_exp)
		return MAL_MALLOC_FAIL;
	append(exps, ext_exp);
	return NULL;
}

static sql_rel *
rel_file_loader(mvc *sql, list *exps, list *tl, char *tname)
{
	sql_subfunc *f = NULL;
	bool found = false;

	if ((f = bind_func_(sql, NULL, "file_loader", tl, F_UNION, true, &found, false))) {
		list *nexps = exps;
		if (list_empty(tl) || (nexps = check_arguments_and_find_largest_any_type(sql, NULL, exps, f, 1, false))) {
			list *res_exps = sa_list(sql->sa);
			if (list_length(exps) == 1 && f && f->func->varres && strlen(f->func->mod) == 0 && strlen(f->func->imp) == 0) {
				char *err = file_loader_add_table_column_types(sql, f, nexps, res_exps, tname);
				if (err)
					return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: file_loader function failed '%s'", err);
			}
			sql_exp *e = exp_op(sql->sa, nexps, f);
			sql_rel *rel = rel_table_func(sql->sa, NULL, e, res_exps, TABLE_PROD_FUNC);
			if (rel)
				rel = rel_project(sql->sa, rel, exps_alias(sql, res_exps));
			return rel;
		}
	}
	return NULL;
}

sql_exp *
find_table_function(mvc *sql, char *sname, char *fname, list *exps, list *tl, sql_ftype type)
{
	bool found = false;
	sql_subfunc *f = NULL;

	assert(type == F_UNION || type == F_LOADER);
	if ((f = bind_func_(sql, sname, fname, tl, type, false, &found, false))) {
		list *nexps = exps;
		if (list_empty(tl) || (nexps = check_arguments_and_find_largest_any_type(sql, NULL, exps, f, 1, false)))
			return exp_op(sql->sa, nexps, f);
		found = false;
	}
	char *arg_list = list_length(tl) ? nary_function_arg_types_2str(sql, tl, list_length(tl)) : NULL;
	return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: %s %s function %s%s%s'%s'(%s)",
					 found ? "insufficient privileges for" : "no such", type == F_UNION ? "table returning" : "loader", sname ? "'":"", sname ? sname : "",
					 sname ? "'.":"", fname, arg_list ? arg_list : "");
}

static sql_rel *
rel_named_table_function(sql_query *query, sql_rel *rel, symbol *ast, int lateral, list *refs)
{
	mvc *sql = query->sql;
	list *exps = NULL, *tl;
	node *m;
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_rel *sq = NULL, *outer = NULL;
	sql_exp *e = NULL;
	sql_subfunc *sf = NULL;
	symbol *sym = ast->data.lval->h->data.sym, *subquery = NULL;
	dnode *l = sym->data.lval->h, *n;
	char *tname = NULL;
	char *fname = qname_schema_object(l->data.lval);
	char *sname = qname_schema(l->data.lval);

	tl = sa_list(sql->sa);
	exps = sa_list(sql->sa);
	if (l->next)
		l = l->next; /* skip distinct */
	if (l->next) { /* table call with subquery */
		int is_value = 1;
		if (l->next->type == type_symbol || l->next->type == type_list) {
			exp_kind iek = {type_value, card_set, TRUE};
			int count = 0;

			if (l->next->type == type_symbol)
				n = l->next;
			else
				n = l->next->data.lval?l->next->data.lval->h:NULL;

			for (dnode *m = n; m; m = m->next) {
				if (m->type == type_symbol && m->data.sym->token == SQL_SELECT)
					subquery = m->data.sym;
				count++;
			}
			if (subquery && count > 1)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: The input for the table returning function %s%s%s'%s' must be either a single sub query, or a list of values",
								 sname ? "'":"", sname ? sname : "", sname ? "'.":"", fname);

			if (subquery) {
				if (!(sq = rel_subquery(query, subquery, ek)))
					return NULL;
				is_value = 0;
			} else {
				for ( ; n; n = n->next) {
					sql_exp *e = rel_value_exp(query, &outer, n->data.sym, sql_sel | sql_from, iek);

					if (!e)
						return NULL;
					append(exps, e);
					is_value &= exp_is_atom(e);
				}
				if (!is_value || (lateral && outer))
					sq = rel_project(sql->sa, NULL, exps);
				if (lateral && outer) {
					sq = rel_crossproduct(sql->sa, sq, outer, op_join);
					set_dependent(sq);
					set_processed(sq);
				}
			}
		}
		if (!is_value && (!sq || (!lateral && outer)))
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: no such table returning function %s%s%s'%s'", sname ? "'":"", sname ? sname : "", sname ? "'.":"", fname);
		if (!is_value) {
			if (list_length(exps))
				exps = sa_list(sql->sa);
			for (node *en = sq->exps->h; en; en = en->next) {
				sql_exp *e = en->data;

				if (!e->alias.label)
					exp_label(sql->sa, e, ++sql->label);
				sql_exp *ne = exp_ref(sql, e);
				/* allow for table functions with table input */
				ne->card = CARD_ATOM;
				exp_setname(sql, ne, tname, exp_name(e));
				append(exps, ne);
				append(tl, exp_subtype(e));
			}
		} else {
			for (node *en = exps->h; en; en = en->next)
				append(tl, exp_subtype(en->data));
		}
	}

	rel = NULL;
	if (ast->data.lval->t->type == type_symbol && ast->data.lval->t->data.sym)
		tname = ast->data.lval->t->data.sym->data.lval->h->data.sval;
	else
		tname = make_label(sql->sa, ++sql->label);

	if (!sname && strcmp(fname, "file_loader") == 0) {
		rel = rel_file_loader(sql, exps, tl, tname);
		if (!rel)
			return NULL;
	} else if (!(e = find_table_function(sql, sname, fname, list_empty(exps) ? NULL : exps, tl, F_UNION)))
		return NULL;

	if (!rel) {
		rel = sq;

		/* column or table function */
		sf = e->f;
		if (e->type != e_func || sf->func->type != F_UNION)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: '%s' does not return a table", exp_func_name(e));

		if (sq) {
			for (node *n = sq->exps->h, *m = sf->func->ops->h ; n && m ; n = n->next, m = m->next) {
				sql_exp *e = (sql_exp*) n->data;
				sql_arg *a = (sql_arg*) m->data;
				if (!exp_subtype(e) && rel_set_type_param(sql, &(a->type), sq, e, 0) < 0)
					return NULL;
			}
		}

		/* for each column add table.column name */
		exps = new_exp_list(sql->sa);
		for (m = sf->func->res->h; m; m = m->next) {
			sql_arg *a = m->data;
			sql_exp *e = exp_column(sql->sa, tname, a->name, &a->type, CARD_MULTI, 1, 0, 0);
			e->alias.label = -(sql->nid++);

			set_basecol(e);
			append(exps, e);
		}
		rel = rel_table_func(sql->sa, rel, e, exps, (sq)?TABLE_FROM_RELATION:TABLE_PROD_FUNC);
	}
	if (ast->data.lval->t->type == type_symbol && ast->data.lval->t->data.sym && ast->data.lval->t->data.sym->data.lval->h->next->data.lval) {
		rel = rel_table_optname(sql, rel, ast->data.lval->t->data.sym, refs);
	} else if (refs) { /* if this relation is under a FROM clause, check for duplicate names */
		if (list_find(refs, tname, (fcmp) &strcmp))
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: relation name \"%s\" specified more than once", tname);
		list_append(refs, tname);
	}
	return rel;
}

static sql_exp*
exp_tuples_set_supertype(mvc *sql, list *tuple_values, sql_exp *tuples)
{
	assert(is_values(tuples));
	list *vals = exp_get_values(tuples);
	if (!vals || !vals->h)
		return NULL;

	int tuple_width = list_length(tuple_values), i;
	sql_subtype *types = SA_NEW_ARRAY(sql->sa, sql_subtype, tuple_width);
	bool *has_type = SA_NEW_ARRAY(sql->sa, bool, tuple_width);
	node *n;

	memset(has_type, 0, sizeof(bool)*tuple_width);
	for(n = tuple_values->h, i = 0; n; n = n->next, i++) {
		sql_exp *e = n->data;
		if (exp_subtype(e)) {
			types[i] = *exp_subtype(e);
			has_type[i] = 1;
		}
	}

	for (node *m = vals->h; m; m = m->next) {
		sql_exp *tuple = m->data;
		if (is_values(tuple)) {
			list *exps = tuple->f;
			for(n = exps->h, i = 0; n; n = n->next, i++) {
				sql_subtype *tpe;
				sql_exp *e = n->data;

				if (has_type[i] && e->type == e_atom && !e->l && !e->r && !e->f && !e->tpe.type) {
					if (set_type_param(sql, types+i, e->flag) == 0)
						e->tpe = types[i];
					else
						return NULL;
				}
				tpe = exp_subtype(e);
				if (!tpe)
					return NULL;
				if (has_type[i] && tpe) {
					supertype(types+i, types+i, tpe);
				} else {
					has_type[i] = 1;
					types[i] = *tpe;
				}
			}
		} else {
			sql_rel *tuple_relation = exp_rel_get_rel(sql->sa, tuple);

			for(n = tuple_relation->exps->h, i = 0; n; n = n->next, i++) {
				sql_subtype *tpe;
				sql_exp *e = n->data;

				if (has_type[i] && e->type == e_atom && !e->l && !e->r && !e->f && !e->tpe.type) {
					if (set_type_param(sql, types+i, e->flag) == 0)
						e->tpe = types[i];
					else
						return NULL;
				}
				tpe = exp_subtype(e);
				if (!tpe)
					return NULL;
				if (has_type[i] && tpe) {
					cmp_supertype(types+i, types+i, tpe);
				} else {
					has_type[i] = 1;
					types[i] = *tpe;
				}
			}
		}
	}

	for (node *m = vals->h; m; m = m->next) {
		sql_exp *tuple = m->data;
		if (is_values(tuple)) {
			list *exps = tuple->f;
			list *nexps = sa_list(sql->sa);
			for(n = exps->h, i = 0; n; n = n->next, i++) {
				sql_exp *e = n->data;

				e = exp_check_type(sql, types+i, NULL, e, type_equal);
				if (!e)
					return NULL;
				exp_label(sql->sa, e, ++sql->label);
				append(nexps, e);
			}
			tuple->f = nexps;
		} else {
			sql_rel *tuple_relation = exp_rel_get_rel(sql->sa, tuple);

			list *nexps = sa_list(sql->sa);
			for(n = tuple_relation->exps->h, i = 0; n; n = n->next, i++) {
				sql_exp *e = n->data;

				e = exp_check_type(sql, types+i, NULL, e, type_equal);
				if (!e)
					return NULL;
				append(nexps, e);
			}
			tuple_relation->exps = nexps;
		}
	}
	return tuples;
}

static int
rel_binop_check_types(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs, int upcast)
{
	sql_subtype *t1 = exp_subtype(ls), *t2 = exp_subtype(rs);

	if (!t1 || !t2) {
		if (t2 && !t1 && rel_set_type_param(sql, t2, rel, ls, upcast) < 0)
			return -1;
		if (t1 && !t2 && rel_set_type_param(sql, t1, rel, rs, upcast) < 0)
			return -1;
	}
	if (!exp_subtype(ls) && !exp_subtype(rs)) {
		(void) sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) on both sides of an expression");
		return -1;
	}
	return 0;
}

static list *
tuples_check_types(mvc *sql, list *tuple_values, sql_exp *tuples)
{
	list *tuples_list = exp_get_values(tuples);
	sql_exp *first_tuple = tuples_list->h->data;
	list *nvalues = sa_list(sql->sa);
	if (is_values(first_tuple)) {
		list *vals = first_tuple->f;

		for (node *n = tuple_values->h, *m = vals->h; n && m; n = n->next, m = m->next) {
			sql_exp *le = n->data, *re = m->data;

			if (rel_binop_check_types(sql, NULL, le, re, 0) < 0)
				return NULL;
			if ((le = exp_check_type(sql, exp_subtype(re), NULL, le, type_equal)) == NULL)
				return NULL;
			append(nvalues, le);
		}
		return nvalues;
	} else {
		sql_rel *tuple_relation = exp_rel_get_rel(sql->sa, first_tuple);

		assert(list_length(tuple_values) == list_length(tuple_relation->exps));
		for (node *n = tuple_values->h, *m = tuple_relation->exps->h; n && m; n = n->next, m = m->next) {
			sql_exp *le = n->data, *re = m->data;

			if (rel_binop_check_types(sql, NULL, le, re, 0) < 0)
				return NULL;
			if ((le = exp_check_type(sql, exp_subtype(re), NULL, le, type_equal)) == NULL)
				return NULL;
			append(nvalues, le);
		}
		return nvalues;
	}
	return tuple_values;
}

static sql_rel *
rel_values(sql_query *query, symbol *tableref, list *refs)
{
	mvc *sql = query->sql;
	sql_rel *r = NULL;
	symbol *values = tableref;
	symbol *optname = NULL;
	if (tableref->token == SQL_TABLE) {
		values = tableref->data.lval->h->data.sym;
		optname = tableref->data.lval->t->type == type_symbol ? tableref->data.lval->t->data.sym : NULL;
	}
	dlist *rowlist = values->data.lval;
	node *m;
	list *exps = sa_list(sql->sa);
	exp_kind ek = {type_value, card_value, TRUE};

	for (dnode *o = rowlist->h; o; o = o->next) {
		dlist *values = o->data.lval;

		if (!list_empty(exps) && list_length(exps) != dlist_length(values)) {
			return sql_error(sql, 02, SQLSTATE(42000) "VALUES: number of columns doesn't match between rows");
		} else {
			dnode *n;

			if (list_empty(exps)) {
				for (n = values->h; n; n = n->next) {
					sql_exp *vals = exp_values(sql->sa, sa_list(sql->sa));

					exp_label(sql->sa, vals, ++sql->label);
					list_append(exps, vals);
				}
			}
			for (n = values->h, m = exps->h; n && m; n = n->next, m = m->next) {
				sql_exp *vals = m->data;
				list *vals_list = vals->f;
				sql_exp *e = rel_value_exp(query, NULL, n->data.sym, sql_sel | sql_values, ek);
				if (!e)
					return NULL;
				list_append(vals_list, e);
			}
		}
	}

	/* loop to check types and cardinality */
	unsigned int card = exps->h && list_length(((sql_exp*)exps->h->data)->f) > 1 ? CARD_MULTI : CARD_ATOM;
	for (m = exps->h; m; m = m->next) {
		sql_exp *e = m->data;

		if (!(e = exp_values_set_supertype(sql, e, NULL)))
			return NULL;
		e->card = card;
		m->data = e;
	}

	r = rel_project(sql->sa, NULL, exps);
	r->nrcols = list_length(exps);
	r->card = card;
	return rel_table_optname(sql, r, optname, refs);
}

static int
check_is_lateral(symbol *tableref)
{
	if (tableref->token == SQL_NAME || tableref->token == SQL_TABLE ||
		tableref->token == SQL_VALUES) {
		if (dlist_length(tableref->data.lval) == 3)
			return tableref->data.lval->h->next->data.i_val;
		return 0;
	} else if (tableref->token == SQL_WITH) {
		if (dlist_length(tableref->data.lval) == 5)
			return tableref->data.lval->h->next->next->next->data.i_val;
		return 0;
	} else if (tableref->token == SQL_SELECT) {
		SelectNode *sn = (SelectNode *) tableref;
		return sn->lateral;
	} else if (tableref->token == SQL_EXCEPT || tableref->token == SQL_INTERSECT ||
			   tableref->token == SQL_UNION) {
		if (dlist_length(tableref->data.lval) == 6)
			return tableref->data.lval->h->next->next->next->next->data.i_val;
		return 0;
	} else {
		return 0;
	}
}

static sql_rel *
rel_reduce_on_column_privileges(mvc *sql, sql_rel *rel, sql_table *t)
{
	list *exps = sa_list(sql->sa);

	for (node *n = rel->exps->h, *m = ol_first_node(t->columns); n && m; n = n->next, m = m->next) {
		sql_exp *e = n->data;
		sql_column *c = m->data;

		if (column_privs(sql, c, PRIV_SELECT))
			append(exps, e);
	}
	if (!list_empty(exps)) {
		rel->exps = exps;
		return rel;
	}
	return NULL;
}

sql_rel *
table_ref(sql_query *query, symbol *tableref, int lateral, list *refs)
{
	mvc *sql = query->sql;
	char *tname = NULL;
	sql_table *t = NULL;
	sql_rel *res = NULL;

	if (tableref->token == SQL_NAME) {
		dlist *name = tableref->data.lval->h->data.lval;
		sql_rel *temp_table = NULL;
		char *sname = qname_schema(name);
		int allowed = 1;

		tname = qname_schema_object(name);

		if (dlist_length(name) > 2)
			return sql_error(sql, 02, SQLSTATE(3F000) "SELECT: only a schema and table name expected");

		if (!sname)
			temp_table = stack_find_rel_view(sql, tname);
		if (!temp_table)
			t = find_table_or_view_on_scope(sql, NULL, sname, tname, "SELECT", false);
		if (!t && !temp_table)
			return NULL;
		if (!temp_table && !table_privs(sql, t, PRIV_SELECT))
			allowed = 0;

		if (tableref->data.lval->t->type == type_symbol && tableref->data.lval->t->data.sym) /* AS */
			tname = tableref->data.lval->t->data.sym->data.lval->h->data.sval;
		if (temp_table && !t) {
			node *n;
			int needed = !is_simple_project(temp_table->op);

			if (is_basetable(temp_table->op) && !temp_table->exps) {
				if (strcmp(rel_base_name(temp_table), tname) != 0)
					rel_base_rename(temp_table, tname);
			} else {
				for (n = temp_table->exps->h; n && !needed; n = n->next) {
					sql_exp *e = n->data;

					if (!exp_relname(e) || strcmp(exp_relname(e), tname) != 0)
						needed = 1;
				}

				if (needed) {
					list *exps = rel_projections(sql, temp_table, NULL, 1, 1);

					temp_table = rel_project(sql->sa, temp_table, exps);
					for (n = exps->h; n; n = n->next) {
						sql_exp *e = n->data;

						noninternexp_setname(sql, e, tname, NULL);
						set_basecol(e);
					}
					list_hash_clear(exps);
				}
			}
			if (temp_table && tableref->data.lval->t->type == type_symbol && tableref->data.lval->t->data.sym && tableref->data.lval->t->data.sym->data.lval->h->next->data.lval) /* AS with column aliases */
				temp_table = rel_table_optname(sql, temp_table, tableref->data.lval->t->data.sym, refs);
			if (allowed)
				return temp_table;
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: access denied for %s to table '%s'", get_string_global_var(sql, "current_user"), tname);
		} else if (isView(t)) {
			/* instantiate base view */
			node *n,*m;
			sql_rel *rel;

			if (sql->emode == m_deps) {
				rel = rel_basetable(sql, t, tname);
				if (!allowed)
					rel_base_disallow(rel);
			} else {
				/* when recreating a view, the view itself can't be found */
				if (sql->objid && sql->objid == t->base.id)
					return sql_error(sql, 02, SQLSTATE(42000) "SELECT: attempting to recursively bind view '%s'.'%s'", t->s->base.name, tname);
				rel = rel_parse(sql, t->s, t->query, m_instantiate);
				if (rel && sql->emode == m_deps)
					rel = rel_unnest(sql, rel);
			}

			if (!rel)
				return NULL;
			/* Rename columns of the rel_parse relation */
			if (sql->emode != m_deps) {
				assert(is_project(rel->op));
				set_processed(rel);
				if (is_mset(rel->op) || is_simple_project(rel->op) || (is_groupby(rel->op) && !list_empty(rel->r))) {
					/* it's unsafe to set the projection names because of possible dependent sorting/grouping columns */
					rel = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 0, 0));
					set_processed(rel);
				}
				for (n = ol_first_node(t->columns), m = rel->exps->h; n && m; n = n->next, m = m->next) {
					sql_column *c = n->data;
					sql_exp *e = m->data;

					exp_setname(sql, e, tname, c->base.name);
					set_basecol(e);
				}
				list_hash_clear(rel->exps);
			}
			if (rel && !allowed && t->query && (rel = rel_reduce_on_column_privileges(sql, rel, t)) == NULL)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: access denied for %s to view '%s.%s'", get_string_global_var(sql, "current_user"), t->s->base.name, tname);
			return rel;
		}
		if ((isMergeTable(t) || isReplicaTable(t)) && list_length(t->members)==0)
			return sql_error(sql, 02, SQLSTATE(42000) "%s '%s'.'%s' should have at least one table associated",
							TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->s->base.name, tname);
		res = rel_basetable(sql, t, tname);
		if (!allowed) {
			rel_base_disallow(res);
			if (rel_base_has_column_privileges(sql, res) == 0)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: access denied for %s to %s '%s.%s'", get_string_global_var(sql, "current_user"), isView(t) ? "view" : "table", t->s->base.name, tname);
		}
		if (tableref->data.lval->t->type == type_symbol && tableref->data.lval->t->data.sym && tableref->data.lval->t->data.sym->data.lval->h->next->data.lval) { /* AS with column aliases */
			res = rel_table_optname(sql, res, tableref->data.lval->t->data.sym, refs);
		} else if (refs) { /* if this relation is under a FROM clause, check for duplicate names */
			if (list_find(refs, tname, (fcmp) &strcmp))
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: relation name \"%s\" specified more than once", tname);
			assert(tname);
			list_append(refs, tname);
		}
		return res;
	} else if (tableref->token == SQL_VALUES || (tableref->token == SQL_TABLE && tableref->data.lval->h->data.sym->token == SQL_VALUES)) {
		return rel_values(query, tableref, refs);
	} else if (tableref->token == SQL_TABLE) {
		return rel_named_table_function(query, NULL, tableref, lateral, refs);
	} else if (tableref->token == SQL_SELECT) {
		return rel_subquery_optname(query, tableref, refs);
	} else if (tableref->token == SQL_UNION || tableref->token == SQL_EXCEPT || tableref->token == SQL_INTERSECT) {
		/* subqueries will be called, ie no need to test for duplicate references */
		sql_rel *tq = rel_setquery(query, tableref);

		if (!tq)
			return NULL;
		/* look for lateral joins */
		symbol *optname = tableref->data.lval->t->type == type_symbol ? tableref->data.lval->t->data.sym : NULL;
		return rel_table_optname(sql, tq, optname, refs);
	} else {
		return query_exp_optname(query, tableref, refs);
	}
}

static sql_exp *
rel_exp_variable_on_scope(mvc *sql, const char *sname, const char *vname)
{
	sql_subtype *tpe = NULL;
	sql_var *var = NULL;
	sql_arg *a = NULL;
	int level = 1;

	if (find_variable_on_scope(sql, sname, vname, &var, &a, &tpe, &level, "SELECT")) {
		if (var) /* if variable is known from the stack or a global var */
			return exp_param_or_declared(sql->sa, var->sname ? sa_strdup(sql->sa, var->sname) : NULL, sa_strdup(sql->sa, var->name), &(var->var.tpe), level);
		if (a) /* if variable is a parameter */
			return exp_param_or_declared(sql->sa, NULL, sa_strdup(sql->sa, vname), &(a->type), level);
	}
	return NULL;
}

static sql_exp *
exps_get_exp(list *exps, int nth)
{
	node *n = NULL;
	int i = 0;

	if (exps)
		for (n=exps->h, i=1; n && i<nth; n=n->next, i++)
			;
	if (n && i == nth)
		return n->data;
	return NULL;
}

static sql_rel *
rel_find_groupby(sql_rel *groupby)
{
	if (groupby && !is_processed(groupby) && !is_base(groupby->op)) {
		while(!is_processed(groupby) && !is_base(groupby->op)) {
			if (is_groupby(groupby->op) || !groupby->l)
				break;
			if (groupby->l)
				groupby = groupby->l;
		}
		if (groupby && is_groupby(groupby->op))
			return groupby;
	}
	return NULL;
}

static int
is_groupby_col(sql_rel *gb, sql_exp *e)
{
	gb = rel_find_groupby(gb);

	if (gb) {
		if (exp_relname(e)) {
			if (exp_name(e) && exps_bind_column2(gb->r, exp_relname(e), exp_name(e), NULL))
				return 1;
		} else {
			if (exp_name(e) && exps_bind_column(gb->r, exp_name(e), NULL, NULL, 1))
				return 1;
		}
	}
	return 0;
}

static void
set_dependent_( sql_rel *r)
{
	if (is_select(r->op))
		r = r->l;
	if (r && is_join(r->op))
		set_dependent(r);
}

static
sql_rel* find_union(visitor *v, sql_rel *rel) {
	if (rel->op == op_union || rel->op == op_munion)
		v->data = rel;
	return rel;
}

static inline
bool group_by_pk_project_uk_cond(mvc* sql, sql_rel* inner, sql_exp* exp,const char* sname, const char* tname) {
	sql_table* t = find_table_or_view_on_scope(sql, NULL, sname, tname, "SELECT", false);
	bool allow = false;
	if (t) {
		sql_idx* pki = NULL;
		list *ukil = sa_list(sql->sa);

		for (node * n = ol_first_node(t->idxs); n; n = n->next) {
			sql_idx *i = n->data;
			if (!i->key)
				continue;
			switch (i->key->type) {
			case pkey:
				pki = i;
				continue;
			case ukey:
			case unndkey:
				list_append(ukil, i);
				continue;
			default:
				continue;
			}
		}
		if (pki && pki->columns->cnt == 1 && inner->r && ((list*) inner->r)->cnt == 1) {
			/* for now only check simple case where primary key and group by expression is a single column*/
			sql_exp* gbe = ((list*) inner->r)->h->data;
			sql_column* pkc = ((sql_kc *)pki->columns->h->data)->c;
			if (gbe->type == e_column && strcmp(gbe->alias.name, pkc->base.name) == 0) {
				node *n;
				for (n = ukil->h; n; n = n->next){
					sql_idx* uki = n->data;
					if (uki->columns->cnt == 1) {
						/* for now only check simple case where unique key is a single column*/
						sql_column* ukc = ((sql_kc *)uki->columns->h->data)->c;
						if (strcmp(exp->alias.name, ukc->base.name) == 0) {
							allow = true;
							break;
						}
					}
				}
			}
		}

		if (allow) {
			/* sufficiency condition: abort if relation contains union subrelation
			* because it may break functional dependency between pk and uk */
			visitor v = {.sql=sql};
			rel_visitor_topdown(&v, inner, &find_union);
			if (v.data)
				allow = false;
		}
	}

	return allow;

}

static sql_exp *
rel_column_ref(sql_query *query, sql_rel **rel, symbol *column_r, int f)
{
	mvc *sql = query->sql;
	sql_exp *exp = NULL;
	dlist *l = NULL;
	sql_rel *inner = rel?*rel:NULL, *outer = NULL;
	int used_lower_after_processed = 0;

	assert((column_r->token == SQL_COLUMN || column_r->token == SQL_IDENT) && column_r->type == type_list);
	l = column_r->data.lval;

	if (dlist_length(l) == 1) {
		const char *name = l->h->data.sval;
		if (!name)
			return NULL;

		if (!exp && inner)
			if (!(exp = rel_bind_column(sql, inner, name, f, 0)) && sql->session->status == -ERR_AMBIGUOUS)
				return NULL;
		if (!exp && inner && ((is_sql_aggr(f) && (is_groupby(inner->op) || is_select(inner->op))) ||
						     (is_groupby(inner->op) && inner->flag))) {
			/* if inner is selection, ie having clause, get the left relation to reach group by */
			sql_rel *gp = inner;
			while (gp && is_select(gp->op))
				gp = gp->l;
			if (gp && !is_basetable(gp->op) && gp->l && !(exp = rel_bind_column(sql, gp->l, name, f, 0)) && sql->session->status == -ERR_AMBIGUOUS)
				return NULL;
		}
		if (!exp && query && query_has_outer(query)) {
			int i;

			for (i=query_has_outer(query)-1; i>= 0 && !exp && (outer = query_fetch_outer(query,i)); i--) {
				if (!(exp = rel_bind_column(sql, outer, name, f, 0)) && sql->session->status == -ERR_AMBIGUOUS)
					return NULL;
				if (!exp && is_groupby(outer->op)) {
					if (!(exp = rel_bind_column(sql, outer->l, name, f, 0)) && sql->session->status == -ERR_AMBIGUOUS)
						return NULL;
					else
						used_lower_after_processed = is_processed(outer);
				}
				if (exp && is_simple_project(outer->op) && !rel_find_exp(outer, exp))
					exp = rel_project_add_exp(sql, outer, exp);
				if (exp)
					break;
			}
			if (exp && exp->card != CARD_AGGR && is_groupby(outer->op) && !is_sql_aggr(f) && rel_find_exp(outer->l, exp))
				return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", name);
			if (exp && outer && outer->card <= CARD_AGGR && exp->card > CARD_AGGR && !is_sql_aggr(f))
				return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", name);
			if (exp && outer && !is_sql_aggr(f) && !is_sql_aggr(query_fetch_outer_state(query, i))) {
				if (used_lower_after_processed || query_outer_used_exp( query, i, exp, f)) {
					sql_exp *lu = used_lower_after_processed?exp:query_outer_last_used(query, i);
					if (exp_name(lu) && exp_relname(lu) && !has_label(lu))
						return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: subquery uses ungrouped column \"%s.%s\" from outer query", exp_relname(lu), exp_name(lu));
					return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: subquery uses ungrouped column from outer query");
				}
			}
			if (exp) {
				int of = query_fetch_outer_state(query, i);
				if (is_groupby(outer->op) && !is_sql_aggr(f)) {
					exp = rel_groupby_add_aggr(sql, outer, exp);
					exp->card = CARD_ATOM;
				} else if (is_groupby(outer->op) && is_sql_aggr(f) && exps_any_match(outer->exps, exp))
					exp = exp_ref(sql, exp);
				else
					exp->card = CARD_ATOM;
				set_freevar(exp, i);
				/*
				if (exp->alias.label == exp->nid)
					exp->alias.label = -(sql->nid++);
					*/
				if (!is_sql_where(of) && !is_sql_aggr(of) && !is_sql_aggr(f) && !outer->grouped)
					set_outer(outer);
			}
			if (exp && outer && (is_select(outer->op) || is_join(outer->op)))
				set_dependent_(outer);
		}

		/* some views are just in the stack, like before and after updates views */
		if (rel && sql->use_views) {
			sql_rel *v = NULL;
			int dup = stack_find_rel_view_projection_columns(sql, name, &v); /* trigger views are basetables relations, so those may conflict */

			if (dup < 0 || (v && exp && *rel && is_base(v->op) && v != *rel)) /* comparing pointers, ugh */
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s' ambiguous", name);
			if (v && !exp) {
				if (*rel)
					*rel = rel_crossproduct(sql->sa, *rel, rel_dup(v), op_join);
				else
					*rel = rel_dup(v);
				exp = rel_bind_column(sql, *rel, name, f, 0);
			}
		}
		if (!exp) /* If no column was found, try a variable or parameter */
			exp = rel_exp_variable_on_scope(sql, NULL, name);

		if (!exp) {
			if (inner && !is_sql_aggr(f) && is_groupby(inner->op) && inner->l && (exp = rel_bind_column(sql, inner->l, name, f, 0)))
				return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", name);
		}

		if (!exp)
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: identifier '%s' unknown", name);
		if (exp && inner && inner->card <= CARD_AGGR && exp->card > CARD_AGGR && (is_sql_sel(f) || is_sql_having(f)) && (!is_sql_aggr(f) && !(inner->flag)))
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", name);
		if (exp && inner && is_groupby(inner->op) && !is_sql_aggr(f) && !is_freevar(exp) && !inner->flag)
			exp = rel_groupby_add_aggr(sql, inner, exp);
	} else if (dlist_length(l) == 2 || dlist_length(l) == 3) {
		const char *sname = NULL;
		const char *tname = l->h->data.sval;
		const char *cname = l->h->next->data.sval;
		if (dlist_length(l) == 3) {
			sname = l->h->data.sval;
			tname = l->h->next->data.sval;
			cname = l->h->next->next->data.sval;
		}
		if (!cname)
			return NULL;

		if (!exp && rel && inner)
			if (!(exp = rel_bind_column3(sql, inner, sname, tname, cname, f)) && sql->session->status == -ERR_AMBIGUOUS)
				return NULL;
		if (!exp && inner && is_sql_aggr(f) && (is_groupby(inner->op) || is_select(inner->op))) {
			/* if inner is selection, ie having clause, get the left relation to reach group by */
			sql_rel *gp = inner;
			while (gp && is_select(gp->op))
				gp = gp->l;
			if (gp && !is_basetable(gp->op) && gp->l && !(exp = rel_bind_column3(sql, gp->l, sname, tname, cname, f)) && sql->session->status == -ERR_AMBIGUOUS)
				return NULL;
		}
		if (!exp && query && query_has_outer(query)) {
			int i;

			for (i=query_has_outer(query)-1; i>= 0 && !exp && (outer = query_fetch_outer(query,i)); i--) {
				if (!(exp = rel_bind_column3(sql, outer, sname, tname, cname, f | sql_outer)) && sql->session->status == -ERR_AMBIGUOUS)
					return NULL;
				if (!exp && is_groupby(outer->op)) {
					if (!(exp = rel_bind_column3(sql, outer->l, sname, tname, cname, f)) && sql->session->status == -ERR_AMBIGUOUS)
						return NULL;
					else
						used_lower_after_processed = is_processed(outer);
				}
				if (exp && is_simple_project(outer->op) && !rel_find_exp(outer, exp))
					exp = rel_project_add_exp(sql, outer, exp);
				if (exp)
					break;
			}
			if (exp && exp->card != CARD_AGGR && is_groupby(outer->op) && !is_sql_aggr(f) && rel_find_exp(outer->l, exp))
				return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", tname, cname);
			if (exp && outer && outer->card <= CARD_AGGR && exp->card > CARD_AGGR && !is_sql_aggr(f))
				return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", tname, cname);
			if (exp && outer && !is_sql_aggr(f)) {
				if (used_lower_after_processed || query_outer_used_exp( query, i, exp, f)) {
					sql_exp *lu = used_lower_after_processed?exp:query_outer_last_used(query, i);
					if (exp_name(lu) && exp_relname(lu) && !has_label(lu))
						return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: subquery uses ungrouped column \"%s.%s\" from outer query", exp_relname(lu), exp_name(lu));
					return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: subquery uses ungrouped column from outer query");
				}
			}
			if (exp) {
				int of = query_fetch_outer_state(query, i);
				if (is_groupby(outer->op) && !is_sql_aggr(f)) {
					exp = rel_groupby_add_aggr(sql, outer, exp);
					exp->card = CARD_ATOM;
				} else if (is_groupby(outer->op) && is_sql_aggr(f) && exps_any_match(outer->exps, exp))
					exp = exp_ref(sql, exp);
				else
					exp->card = CARD_ATOM;
				set_freevar(exp, i);
				/*
				if (exp->alias.label == exp->nid)
					exp->alias.label = -(sql->nid++);
					*/
				if (!is_sql_where(of) && !is_sql_aggr(of) && !is_sql_aggr(f) && !outer->grouped)
					set_outer(outer);
			}
			if (exp && outer && (is_select(outer->op) || is_join(outer->op)))
				set_dependent_(outer);
		}

		/* some views are just in the stack, like before and after updates views */
		if (rel && sql->use_views) {
			sql_rel *v = stack_find_rel_view(sql, tname);

			if (v && exp && *rel && is_base(v->op) && v != *rel) /* trigger views are basetables relations, so those may conflict */
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s.%s' ambiguous", tname, cname);
			if (v && !exp) {
				if (*rel)
					*rel = rel_crossproduct(sql->sa, *rel, rel_dup(v), op_join);
				else
					*rel = rel_dup(v);
				if (!(exp = rel_bind_column3(sql, *rel, sname, tname, cname, f)) && sql->session->status == -ERR_AMBIGUOUS)
					return NULL;
			}
		}
		if (!exp) { /* If no column was found, try a global variable */
			sql_var *var = NULL;
			sql_subtype *tpe = NULL;
			int level = 0;
			sql_arg *a = NULL;

			if (find_variable_on_scope(sql, tname, cname, &var, &a, &tpe, &level, "SELECT")) { /* search schema with table name, ugh */
				assert(level == 0);
				exp = exp_param_or_declared(sql->sa, sa_strdup(sql->sa, var->sname), sa_strdup(sql->sa, var->name), &(var->var.tpe), 0);
			}
		}
		if (!exp) {
			if (inner && !is_sql_aggr(f) && is_groupby(inner->op) && inner->l && (exp = rel_bind_column3(sql, inner->l, sname, tname, cname, f))) {
				if (group_by_pk_project_uk_cond(sql, inner, exp, sname, tname)) {
					/* SQL23 feature: very special case where primary key is used in GROUP BY expression and
					 * unique key is in the project list or ORDER BY clause */
					sql->session->status = 0;
					sql->errstr[0] = 0;
					exp->card = CARD_AGGR;
					list_append(inner->exps, exp);
				}
				else
					return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", tname, cname);
			}
		}

		if (!exp)
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42S22) "SELECT: no such column '%s.%s'", tname, cname);
		if (exp && inner && inner->card <= CARD_AGGR && exp->card > CARD_AGGR && (is_sql_sel(f) || is_sql_having(f)) && !is_sql_aggr(f))
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", tname, cname);
		if (exp && inner && is_groupby(inner->op) && !is_sql_aggr(f) && !is_freevar(exp))
			exp = rel_groupby_add_aggr(sql, inner, exp);
	} else if (dlist_length(l) > 3) {
		return sql_error(sql, 02, SQLSTATE(42000) "cross-database references are not implemented");
	}
	if (exp && !exp_is_atom(exp) && rel && !outer) {
		if (query->last_exp && query->last_rel == *rel && !is_sql_aggr(query->last_state) && is_sql_aggr(f)) {
			if (!is_groupby(query->last_rel->op) || list_empty(query->last_rel->r) || !exps_find_exp(query->last_rel->r, query->last_exp)) {
				if (exp_relname(query->last_exp) && exp_name(query->last_exp) && !has_label(query->last_exp))
					return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", exp_relname(query->last_exp), exp_name(query->last_exp));
				return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
			}
		}
		query->prev = query->last_exp;
		query->last_exp = exp;
		query->last_state = f;
		query->last_rel = *rel;
	}
	return exp;
}

int
rel_convert_types(mvc *sql, sql_rel *ll, sql_rel *rr, sql_exp **L, sql_exp **R, int scale_fixing, check_type tpe)
{
	sql_exp *ls = *L;
	sql_exp *rs = *R;
	sql_subtype *lt = exp_subtype(ls);
	sql_subtype *rt = exp_subtype(rs);

	if (!rt && !lt) {
		sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) on both sides of an expression");
		return -1;
	}
	if (rt && (!lt || !lt->type))
		 return rel_set_type_param(sql, rt, ll, ls, 0);
	if (lt && (!rt || !rt->type))
		 return rel_set_type_param(sql, lt, rr, rs, 0);

	if (rt && lt) {
		sql_subtype *i = lt;
		sql_subtype *r = rt;

		if (subtype_cmp(lt, rt) != 0 || (tpe == type_equal_no_any && (lt->type->localtype==0 || rt->type->localtype==0))) {
			sql_subtype super;

			cmp_supertype(&super, r, i);
			if (scale_fixing) {
				/* convert ls to super type */
				ls = exp_check_type(sql, &super, ll, ls, tpe);
				/* convert rs to super type */
				rs = exp_check_type(sql, &super, rr, rs, tpe);
			} else {
				/* convert ls to super type */
				super.scale = lt->scale;
				ls = exp_check_type(sql, &super, ll, ls, tpe);
				/* convert rs to super type */
				super.scale = rt->scale;
				rs = exp_check_type(sql, &super, rr, rs, tpe);
			}
		}
		*L = ls;
		*R = rs;
		if (!ls || !rs)
			return -1;
		return 0;
	}
	return -1;
}

static sql_rel *
push_select_exp(mvc *sql, sql_rel *rel, sql_exp *e, sql_exp *ls, int f)
{
	if (is_outerjoin(rel->op)) {
		if ((is_left(rel->op) || is_full(rel->op)) && rel_find_exp(rel->l, ls)) {
			rel_join_add_exp(sql->sa, rel, e);
			return rel;
		} else if ((is_right(rel->op) || is_full(rel->op)) && rel_find_exp(rel->r, ls)) {
			rel_join_add_exp(sql->sa, rel, e);
			return rel;
		}
		if (is_left(rel->op) && rel_find_exp(rel->r, ls)) {
			rel->r = rel_push_select(sql, rel->r, ls, e, f);
			return rel;
		} else if (is_right(rel->op) && rel_find_exp(rel->l, ls)) {
			rel->l = rel_push_select(sql, rel->l, ls, e, f);
			return rel;
		}
	}
	/* push select into the given relation */
	return rel_push_select(sql, rel, ls, e, f);
}

static sql_rel *
push_join_exp(mvc *sql, sql_rel *rel, sql_exp *e, sql_exp *L, sql_exp *R, sql_exp *R2, int f)
{
	sql_rel *r;
	if (/*is_semi(rel->op) ||*/ (is_outerjoin(rel->op) && !is_processed((rel)))) {
		rel_join_add_exp(sql->sa, rel, e);
		return rel;
	}
	/* push join into the given relation */
	if ((r = rel_push_join(sql, rel, L, R, R2, e, f)) != NULL)
		return r;
	rel_join_add_exp(sql->sa, rel, e);
	return rel;
}

static sql_rel *
rel_select_push_filter_exp_down(mvc *sql, sql_rel *rel, sql_exp *e, list *l, list *r, int ff)
{
	sql_exp *ll;
	if (exps_card(r) <= CARD_ATOM && (exps_are_atoms(r) || exps_have_freevar(sql, r) || exps_have_freevar(sql, l))) {
		if (exps_card(l) == exps_card(r) || rel->processed)  /* bin compare op */
			return rel_select(sql->sa, rel, e);
		if ((ll = exps_find_one_multi_exp(l)))
			return push_select_exp(sql, rel, e, ll, ff);
	}
	if (is_outerjoin(rel->op))
		return rel_select(sql->sa, rel, e);
	return rel_select_add_exp(sql->sa, rel, e);
}

static sql_rel *
rel_filter(mvc *sql, sql_rel *rel, list *l, list *r, char *sname, char *filter_op, int anti, int ff)
{
	node *n;
	sql_exp *e = NULL;
	sql_subfunc *f = NULL;
	list *tl = sa_list(sql->sa);
	bool found = false;

	for (n = l->h; n; n = n->next){
		sql_exp *e = n->data;

		list_append(tl, exp_subtype(e));
	}
	for (n = r->h; n; n = n->next){
		sql_exp *e = n->data;

		list_append(tl, exp_subtype(e));
	}
	/* find filter function */
	f = bind_func_(sql, sname, filter_op, tl, F_FILT, false, &found, false);
	if (f) {
		node *n,*m = f->func->ops->h;
		list *nexps = sa_list(sql->sa);

		for(n=l->h; m && n; m = m->next, n = n->next) {
			sql_arg *a = m->data;
			sql_exp *e = n->data;

			e = exp_check_type(sql, &a->type, rel, e, type_equal);
			if (!e)
				return NULL;
			list_append(nexps, e);
		}
		l = nexps;
		nexps = sa_list(sql->sa);
		for(n=r->h; m && n; m = m->next, n = n->next) {
			sql_arg *a = m->data;
			sql_exp *e = n->data;

			e = exp_check_type(sql, &a->type, rel, e, type_equal);
			if (!e)
				return NULL;
			list_append(nexps, e);
		}
		r = nexps;
	}
	if (!f)
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: %s FILTER function %s%s%s'%s'",
						 found ? "insufficient privileges for" : "no such", sname ? "'":"", sname ? sname : "", sname ? "'.":"", filter_op);
	e = exp_filter(sql->sa, l, r, f, anti);

	if (exps_one_is_rel(l) || exps_one_is_rel(r)) /* uncorrelated subquery case */
		return rel_select(sql->sa, rel, e);
	/* atom or row => select */
	for (node *n=l->h; n; n = n->next) {
		sql_exp *ls = n->data;

		if (ls->card > rel->card) {
			if (exp_name(ls) && !has_label(ls))
				return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(ls));
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
		}
	}
	for (node *n=r->h; n; n = n->next) {
		sql_exp *rs = n->data;

		if (rs->card > rel->card) {
			if (exp_name(rs) && !has_label(rs))
				return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(rs));
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
		}
	}
	return rel_select_push_filter_exp_down(sql, rel, e, l, r, ff);
}

static sql_rel *
rel_filter_exp_(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *r1, sql_exp *r2, sql_exp *r3, char *filter_op, int anti, int f)
{
	list *l = sa_list(sql->sa);
	list *r = sa_list(sql->sa);

	list_append(l, ls);
	list_append(r, r1);
	if (r2)
		list_append(r, r2);
	if (r3)
		list_append(r, r3);
	return rel_filter(sql, rel, l, r, "sys", filter_op, anti, f);
}

static sql_rel *
rel_select_push_compare_exp_down(mvc *sql, sql_rel *rel, sql_exp *e, sql_exp *ls, sql_exp *rs, sql_exp *rs2, int f)
{
	if (!is_join(rel->op) && !is_select(rel->op))
		return rel_select(sql->sa, rel, e);
	if ((rs->card <= CARD_ATOM || (rs2 && ls->card <= CARD_ATOM)) &&
		(exp_is_atom(rs) || (rs2 && exp_is_atom(ls)) || exp_has_freevar(sql, rs) || exp_has_freevar(sql, ls)) &&
		(!rs2 || (rs2->card <= CARD_ATOM && (exp_is_atom(rs2) || exp_has_freevar(sql, rs2))))) {
		if (ls->card == rs->card || (rs2 && (ls->card == rs2->card || rs->card == rs2->card)) || rel->processed) /* bin compare op */
			return rel_select(sql->sa, rel, e);

		return push_select_exp(sql, rel, e, ls, f);
	} else { /* join */
		return push_join_exp(sql, rel, e, ls, rs, rs2, f);
	}
	return rel;
}

static sql_rel *
rel_compare_exp_(sql_query *query, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *rs2, int type, int anti, int quantifier, int f, int symmetric, int is_semantics)
{
	mvc *sql = query->sql;
	sql_exp *e = NULL;

	if (quantifier || exp_is_rel(ls) || exp_is_rel(rs) || (rs2 && exp_is_rel(rs2))) {
		if (rs2) {
			e = exp_compare2(sql->sa, ls, rs, rs2, type, symmetric);
			if (anti)
				set_anti(e);
		} else {
			assert(!symmetric);
			if (rel_convert_types(sql, rel, rel, &ls, &rs, 1, type_equal_no_any) < 0)
				return NULL;
			e = exp_compare_func(sql, ls, rs, compare_func((comp_type)type, anti), quantifier);
		}
		return rel_select(sql->sa, rel, e);
	} else if (!rs2) {
		assert(!symmetric);
		if (ls->card < rs->card) {
			sql_exp *swap = ls;
			ls = rs;
			rs = swap;
			type = (int)swap_compare((comp_type)type);
		}
		if (rel_convert_types(sql, rel, rel, &ls, &rs, 1, type_equal_no_any) < 0)
			return NULL;
		e = exp_compare(sql->sa, ls, rs, type);
		if (is_semantics) set_semantics(e);
	} else {
		assert(rs2);
		if (rel_convert_types(sql, rel, rel, &ls, &rs, 1, type_equal_no_any) < 0)
			return NULL;
		if (!(rs2 = exp_check_type(sql, exp_subtype(ls), rel, rs2, type_equal)))
			return NULL;
		e = exp_compare2(sql->sa, ls, rs, rs2, type, symmetric);
	}
	if (anti)
		set_anti(e);

	if (!rel)
		return rel_select(sql->sa, rel_project_exp(sql, exp_atom_bool(sql->sa, 1)), e);

	/* atom or row => select */
	if (ls->card > rel->card || rs->card > rel->card || (rs2 && rs2->card > rel->card)) {
		sql_exp *e = ls->card > rel->card ? ls : rs->card > rel->card ? rs : rs2;
		if (exp_name(e) && !has_label(e))
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(e));
		return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	return rel_select_push_compare_exp_down(sql, rel, e, ls, rs, rs2, f);
}

static sql_rel *
rel_compare_exp(sql_query *query, sql_rel *rel, sql_exp *ls, sql_exp *rs, char *compare_op, int reduce, int quantifier, int need_not, int f, int is_semantics)
{
	mvc *sql = query->sql;
	comp_type type = cmp_equal;

	if (!ls || !rs)
		return NULL;

	if (!quantifier && ((!rel && !query_has_outer(query)) || !reduce)) {
		/* TODO to handle filters here */
		sql_exp *e;

		if (rel_convert_types(sql, rel, rel, &ls, &rs, 1, type_equal_no_any) < 0)
			return NULL;
		e = rel_binop_(sql, rel, ls, rs, "sys", compare_op, card_value, true);

		if (!e)
			return NULL;
		if (!reduce) {
			if (rel->op == op_project) {
				append(rel->exps, e);
			} else {
				list *exps = new_exp_list(sql->sa);

				append(exps, e);
				return rel_project(sql->sa, rel, exps);
			}
		} else {
			return rel_select(sql->sa, rel, e);
		}
	}
	type = compare_str2type(compare_op);
	assert(type != cmp_filter);
	return rel_compare_exp_(query, rel, ls, rs, NULL, type, need_not, quantifier, f, 0, is_semantics);
}

static sql_rel *
rel_compare(sql_query *query, sql_rel *rel, symbol *sc, symbol *lo, symbol *ro, char *compare_op, int f, exp_kind k, int quantifier, int is_semantics)
{
	mvc *sql = query->sql;
	sql_exp *rs = NULL, *ls;
	comp_type cmp_type = compare_str2type(compare_op);
	exp_kind ek = {type_value, card_column, FALSE};
	int need_not = 0;

	if ((quantifier == 1 && cmp_type == cmp_equal) ||
	    (quantifier == 2 && cmp_type == cmp_notequal)) {
		dnode *n = sc->data.lval->h;
		dlist *dl = dlist_create(sql->sa);
		/* map into IN/NOT IN */
		sc->token = cmp_type==cmp_equal?SQL_IN:SQL_NOT_IN;
		n->next->type = type_list;
		n->next->data.lval = dl;
		n->next->next->next = NULL; /* remove quantifier */
		dl->h = n->next->next;
		n->next->next = NULL; /* (remove comparison) moved righthand side */
		return rel_logical_exp(query, rel, sc, f);
	}
	/* <> ANY -> NOT (= ALL) */
	if (quantifier == 1 && cmp_type == cmp_notequal) {
		need_not = 1;
		quantifier = 2;
		cmp_type = cmp_equal;
		compare_op = "=";
	}

	if ((lo->token == SQL_SELECT || lo->token == SQL_UNION || lo->token == SQL_EXCEPT || lo->token == SQL_INTERSECT || lo->token == SQL_VALUES) &&
		(ro->token != SQL_SELECT && ro->token != SQL_UNION && ro->token != SQL_EXCEPT && ro->token != SQL_INTERSECT && ro->token != SQL_VALUES)) {
		symbol *tmp = lo; /* swap subquery to the right hand side */

		lo = ro;
		ro = tmp;

		if (compare_op[0] == '>')
			compare_op[0] = '<';
		else if (compare_op[0] == '<' && compare_op[1] != '>')
			compare_op[0] = '>';
		cmp_type = swap_compare(cmp_type);
	}

	ls = rel_value_exp(query, &rel, lo, f|sql_farg, ek);
	if (!ls)
		return NULL;
	if (ls && rel && exp_has_freevar(sql, ls) && is_sql_sel(f))
		ls = rel_project_add_exp(sql, rel, ls);
	if (quantifier)
		ek.card = card_set;

	rs = rel_value_exp(query, &rel, ro, f|sql_farg, ek);
	if (!rs)
		return NULL;
	if (ls->card > rs->card && rs->card == CARD_AGGR && is_sql_having(f)) {
		if (exp_name(ls) && exp_relname(ls) && !has_label(ls))
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", exp_relname(ls), exp_name(ls));
		return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (rs->card > ls->card && ls->card == CARD_AGGR && is_sql_having(f)) {
		if (exp_name(rs) && exp_relname(rs) && !has_label(rs))
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", exp_relname(rs), exp_name(rs));
		return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	return rel_compare_exp(query, rel, ls, rs, compare_op, k.reduce, quantifier, need_not, f, is_semantics);
}

static sql_exp*
_rel_nop(mvc *sql, char *sname, char *fname, list *tl, sql_rel *rel, list *exps, exp_kind ek)
{
	bool found = false;
	int table_func = (ek.card == card_relation);
	sql_ftype type = (ek.card == card_loader)?F_LOADER:((ek.card == card_none)?F_PROC:
		   ((ek.card == card_relation)?F_UNION:F_FUNC));

	sql_subfunc *f = bind_func_(sql, sname, fname, tl, type, false, &found, false);
	if (f && !(exps = check_arguments_and_find_largest_any_type(sql, rel, exps, f, table_func, false)))
		f = NULL;

	if (f)
		return exp_op(sql->sa, exps, f);
	char *arg_list = nary_function_arg_types_2str(sql, tl, list_length(tl));
	return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: %s operator %s%s%s'%s'(%s)",
					 found ? "insufficient privileges for" : "no such", sname ? "'":"", sname ? sname : "", sname ? "'.":"", fname, arg_list ? arg_list : "");
}

static sql_exp *
exp_exist(sql_query *query, sql_rel *rel, sql_exp *le, int exists)
{
	mvc *sql = query->sql;
	sql_subfunc *exists_func = NULL;
	sql_subtype *t;
	sql_exp *res;

	if (!exp_name(le))
		exp_label(sql->sa, le, ++sql->label);
	if (exp_is_rel(le)) { /* for the subquery case, propagate to the inner query */
		sql_rel *r = exp_rel_get_rel(sql->sa, le);
		if (is_project(r->op) && !list_empty(r->exps)) {
			for (node *n = r->exps->h; n; n = n->next)
				if (!exp_subtype(n->data) && rel_set_type_param(sql, sql_bind_localtype("bit"), r, n->data, 0) < 0) /* workaround */
					return NULL;
			le->tpe = *exp_subtype(r->exps->h->data); /* just take the first expression type */
		}
	} else if (!exp_subtype(le) && rel_set_type_param(sql, sql_bind_localtype("bit"), rel, le, 0) < 0) /* workaround */
		return NULL;
	t = exp_subtype(le);

	if (!(exists_func = sql_bind_func(sql, "sys", exists ? "sql_exists" : "sql_not_exists", t, NULL, F_FUNC, true, true)))
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "exist operator on type %s missing", t ? t->type->base.name : "unknown");
	res = exp_unop(sql->sa, le, exists_func);
	set_has_no_nil(res);
	return res;
}

static sql_exp *
rel_exists_value_exp(sql_query *query, sql_rel **rel, symbol *sc, int f)
{
	exp_kind ek = {type_value, card_exists, FALSE};
	sql_exp *le, *e;

	le = rel_value_exp(query, rel, sc->data.sym, f|sql_farg, ek);
	if (!le)
		return NULL;
	if (!(e = exp_exist(query, rel ? *rel : NULL, le, sc->token == SQL_EXISTS)))
		return NULL;
	/* only freevar should have CARD_AGGR */
	e->card = CARD_ATOM;
	return e;
}

static sql_rel *
rel_exists_exp(sql_query *query, sql_rel *rel, symbol *sc, int f)
{
	exp_kind ek = {type_value, card_exists, TRUE};
	mvc *sql = query->sql;
	sql_rel *sq = NULL;

	if (is_psm_call(f) || is_sql_merge(f))
		return sql_error(sql, 02, SQLSTATE(42000) "%s: subqueries not supported inside %s", is_psm_call(f) ? "CALL" : "MERGE", is_psm_call(f) ? "CALL statements" : "MERGE conditions");
	if (rel) {
		query_processed(query);
		query_push_outer(query, rel, f);
	}
	sq = rel_subquery(query, sc->data.sym, ek);
	if (rel)
		rel = query_pop_outer(query);
	assert(!is_sql_sel(f));
	if (sq) {
		sql_exp *e = exp_rel(sql, sq);
		if (!(e = exp_exist(query, rel, e, sc->token == SQL_EXISTS)))
			return NULL;
		/* only freevar should have CARD_AGGR */
		e->card = CARD_ATOM;
		rel = rel_select_add_exp(sql->sa, rel, e);
		return rel;
	}
	return NULL;
}

static int
is_project_true(sql_rel *r)
{
	if (r && !r->l && list_length(r->exps) == 1) {
		sql_exp *e = r->exps->h->data;
		if (exp_is_atom(e) && exp_is_true(e))
			return 1;
	}
	return 0;
}

static sql_exp *
rel_in_value_exp(sql_query *query, sql_rel **rel, symbol *sc, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, TRUE};
	dlist *dl = sc->data.lval;
	symbol *lo = NULL;
	dnode *n = dl->h->next, *dn = NULL;
	sql_exp *le = NULL, *re, *e = NULL;
	list *ll = sa_list(sql->sa);
	int is_tuple = 0, add_select = 0;

	/* complex case */
	//if (dl->h->type == type_list) { /* (a,b..) in (.. ) */
	if (dl->h->type == type_symbol && dl->h->data.sym->token == SQL_VALUES) {
		lo = dl->h->data.sym;
		dn = lo->data.lval->h->data.lval->h;
		lo = dn->data.sym;
		dn = dn->next;
	} else {
		lo = dl->h->data.sym;
	}
	for( ; lo; lo = dn?dn->data.sym:NULL, dn = dn?dn->next:NULL ) {
		le = rel_value_exp(query, rel, lo, f|sql_farg, ek);
		if (!le)
			return NULL;
		append(ll, le);
	}
	if (list_length(ll) == 1) {
		le = ll->h->data;
		ek.card = card_set;
	} else {
		le = exp_values(sql->sa, ll);
		exp_label(sql->sa, le, ++sql->label);
		ek.type = list_length(ll);
		is_tuple = list_length(ll);
	}
	/* list of values or subqueries */
	/* 3 right hand side cases,
	 *	1 - value/tuple list of atoms
	 *	2 - list of scalar sub queries (needs check on 1 row/column per query)
	 *	3 - single tabular ie. multi column, multi row query
	 */
	if (n->type == type_list) {
		sql_exp *values;
		list *vals = sa_list(sql->sa);

		n = dl->h->next;
		n = n->data.lval->h;

		for (; n; n = n->next) {
			if (n->type == type_list) {
				dnode *m = n->data.lval->h;
				list *rl = sa_list(sql->sa);
				for (; m; m = m->next) {
					assert(m->type == type_symbol);
					sql_exp *re = rel_value_exp(query, rel, m->data.sym, f|sql_farg, ek);
					if (!re)
						return NULL;
					append(rl, re);
				}
				re = exp_values(sql->sa, rl);
			} else {
				assert(n->type == type_symbol);
				re = rel_value_exp(query, rel, n->data.sym, f|sql_farg, ek);
			}
			if (!re)
				return NULL;
			if (is_tuple && (!exp_is_rel(re) && !is_values(re)))
				return sql_error(sql, 02, SQLSTATE(42000) "Cannot match a tuple to a single value");
			if (is_tuple && exp_is_rel(re)) {
				sql_rel *r = exp_rel_get_rel(sql->sa, re);

				if (!r)
					return sql_error(sql, 02, SQLSTATE(42000) "Subquery missing");
				if (r->nrcols != ek.type)
					return sql_error(sql, 02, SQLSTATE(42000) "Subquery has too %s columns", (r->nrcols < ek.type) ? "few" : "many");
				re = exp_rel_label(sql, re);
				if (r && r->l && n->next) /* not a value list */
					return sql_error(sql, 02, SQLSTATE(42000) "subquery must return only one column");
			} else if (exp_is_rel(re)) {
				sql_rel *r = exp_rel_get_rel(sql->sa, re);
				add_select = 1;
				if (rel && *rel && is_join((*rel)->op))
					set_dependent((*rel));
				if (is_project(r->op) && is_project_true(r->l) && list_length(r->exps) == 1)
					re = r->exps->h->data;
			} else if (is_values(re) && is_tuple != list_length(exp_get_values(re))) {
				return sql_error(sql, 02, SQLSTATE(42000) "Tuple sizes do not match");
			}
			append(vals, re);
		}

		if (list_empty(vals))
			return sql_error(sql, 02, SQLSTATE(42000) "The list of values for IN operator cannot be empty");

		values = exp_values(sql->sa, vals);
		exp_label(sql->sa, values, ++sql->label);
		if (is_tuple) {
			if (!(values = exp_tuples_set_supertype(sql, exp_get_values(le), values)))
				return NULL;
			if (!(le->f = tuples_check_types(sql, exp_get_values(le), values)))
				return NULL;
		} else { /* if it's not a tuple, enforce coercion on the type for every element on the list */
			sql_subtype super, *le_tpe = exp_subtype(le), *values_tpe = NULL;

			for (node *m = vals->h; m; m = m->next) { /* first get values supertype */
				sql_exp *e = m->data;
				sql_subtype *tpe = exp_subtype(e);

				if (values_tpe && tpe) {
					cmp_supertype(&super, values_tpe, tpe);
					*values_tpe = super;
				} else if (!values_tpe && tpe) {
					super = *tpe;
					values_tpe = &super;
				}
			}
			if (!le_tpe)
				le_tpe = values_tpe;
			if (!values_tpe)
				values_tpe = le_tpe;
			if (!le_tpe || !values_tpe)
				return sql_error(sql, 01, SQLSTATE(42000) "For the IN operator, both sides must have a type defined");
			cmp_supertype(&super, values_tpe, le_tpe); /* compute supertype */

			/* on selection/join cases we can generate cmp expressions instead of anyequal for trivial cases */
			if ((is_sql_where(f) || is_sql_having(f)) && !is_sql_farg(f) && !exp_has_rel(le) && exps_are_atoms(vals)) {
				if (list_length(vals) == 1) { /* use cmp_equal instead of cmp_in for 1 expression */
					sql_exp *first = vals->h->data;
					if (rel_convert_types(sql, rel ? *rel : NULL, rel ? *rel : NULL, &le, &first, 1, type_equal_no_any) < 0)
						return NULL;
					e = exp_compare(sql->sa, le, first, (sc->token == SQL_IN) ? cmp_equal : cmp_notequal);
				} else { /* use cmp_in instead of anyequal for n simple expressions */
					for (node *n = vals->h ; n ; n = n->next)
						if ((n->data = exp_check_type(sql, &super, rel ? *rel : NULL, n->data, type_equal)) == NULL)
							return NULL;
					if ((le = exp_check_type(sql, &super, rel ? *rel : NULL, le, type_equal)) == NULL)
						return NULL;
					e = exp_in(sql->sa, le, vals, (sc->token == SQL_IN) ? cmp_in : cmp_notin);
				}
			}
			if (!e) { /* after computing supertype, check types for each IN value */
				for (node *n = vals->h ; n ; n = n->next)
					if ((n->data = exp_check_type(sql, &super, rel ? *rel : NULL, n->data, type_equal)) == NULL)
						return NULL;
				values->tpe = *exp_subtype(vals->h->data);
				if (!(le = exp_check_type(sql, &super, rel ? *rel : NULL, le, type_equal)))
					return NULL;
			}
		}
		if (!e) {
			if (add_select && rel && *rel && !is_project((*rel)->op) && !is_select((*rel)->op) && !is_base((*rel)->op))
				*rel = rel_select(sql->sa, *rel, NULL);
			if ((rel && *rel) || exp_has_rel(le) || exp_has_rel(values))
				e = exp_in_func(sql, le, values, (sc->token == SQL_IN), is_tuple);
			else
				e = exp_in_aggr(sql, le, values, (sc->token == SQL_IN), is_tuple);
		}
	}
	return e;
}

static sql_rel *
rel_in_exp(sql_query *query, sql_rel *rel, symbol *sc, int f)
{
	mvc *sql = query->sql;
	sql_exp *e = rel_in_value_exp(query, &rel, sc, f);

	assert(!is_sql_sel(f));
	if (!e || !rel)
		return NULL;

	if (e->type == e_cmp) { /* it's a exp_in or cmp_equal of simple expressions, push down early on if possible */
		sql_exp *ls = e->l;
		bool rlist = (e->flag == cmp_in || e->flag == cmp_notin);
		unsigned int rcard = rlist ? exps_card(e->r) : exp_card(e->r);
		int r_is_atoms = rlist ? exps_are_atoms(e->r) : exp_is_atom(e->r);
		int r_has_freevar = rlist ? exps_have_freevar(sql, e->r) : exp_has_freevar(sql, e->r);

		if (rcard <= CARD_ATOM && (r_is_atoms || r_has_freevar || exp_has_freevar(sql, ls))) {
			if ((exp_card(ls) == rcard) || rel->processed) /* bin compare op */
				return rel_select(sql->sa, rel, e);

			return push_select_exp(sql, rel, e, ls, f);
		} else { /* join */
			sql_exp *rs = rlist ? exps_find_one_multi_exp(e->r) : e->r;
			if (rs)
				return push_join_exp(sql, rel, e, ls, rs, NULL, f);
		}
	}
	if (is_outerjoin(rel->op))
		return rel_select(sql->sa, rel, e);
	return rel_select_add_exp(sql->sa, rel, e);
}

static bool
not_symbol_can_be_propagated(mvc *sql, symbol *sc)
{
	switch (sc->token) {
	case SQL_IN:
	case SQL_NOT_IN:
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
	case SQL_LIKE:
	case SQL_NOT_LIKE:
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
	case SQL_IS_NULL:
	case SQL_IS_NOT_NULL:
	case SQL_NOT:
	case SQL_COMPARE:
		return true;
	case SQL_AND:
	case SQL_OR: {
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		return not_symbol_can_be_propagated(sql, lo) && not_symbol_can_be_propagated(sql, ro);
	}
	default:
		return false;
	}
}

/* Warning, this function assumes the entire bison tree can be negated, so call it after 'not_symbol_can_be_propagated' */
static symbol *
negate_symbol_tree(mvc *sql, symbol *sc)
{
	switch (sc->token) {
	case SQL_IN:
		sc->token = SQL_NOT_IN;
		break;
	case SQL_NOT_IN:
		sc->token = SQL_IN;
		break;
	case SQL_EXISTS:
		sc->token = SQL_NOT_EXISTS;
		break;
	case SQL_NOT_EXISTS:
		sc->token = SQL_EXISTS;
		break;
	case SQL_LIKE:
		sc->token = SQL_NOT_LIKE;
		break;
	case SQL_NOT_LIKE:
		sc->token = SQL_LIKE;
		break;
	case SQL_BETWEEN:
		sc->token = SQL_NOT_BETWEEN;
		break;
	case SQL_NOT_BETWEEN:
		sc->token = SQL_BETWEEN;
		break;
	case SQL_IS_NULL:
		sc->token = SQL_IS_NOT_NULL;
		break;
	case SQL_IS_NOT_NULL:
		sc->token = SQL_IS_NULL;
		break;
	case SQL_NOT: { /* nested NOTs eliminate each other */
		if (sc->data.sym->token == SQL_ATOM) {
			AtomNode *an = (AtomNode*) sc->data.sym;
			sc = newAtomNode(sql->sa, an->a);
		} else if (sc->data.sym->token == SQL_SELECT) {
			SelectNode *sn = (SelectNode*) sc->data.sym;
			sc = newSelectNode(sql->sa, sn->distinct, sn->selection, sn->into, sn->from, sn->where, sn->groupby, sn->having,
							   sn->orderby, sn->name, sn->limit, sn->offset, sn->sample, sn->seed, sn->window);
		} else {
			memmove(sc, sc->data.sym, sizeof(symbol));
		}
	} break;
	case SQL_COMPARE: {
		dnode *cmp_n = sc->data.lval->h;
		comp_type neg_cmp_type = negate_compare(compare_str2type(cmp_n->next->data.sval)); /* negate the comparator */
		if (cmp_n->next->next->next) {
			switch(cmp_n->next->next->next->data.i_val)
			{
			case 0: /* negating ANY/ALL */
				cmp_n->next->next->next->data.i_val = 1;
				break;
			case 1: /* negating ANY/ALL */
				cmp_n->next->next->next->data.i_val = 0;
				break;
			case 2: /* negating IS [NOT] DINSTINCT FROM */
				cmp_n->next->next->next->data.i_val = 3;
				break;
			case 3: /* negating IS [NOT] DINSTINCT FROM */
				cmp_n->next->next->next->data.i_val = 2;
				break;
			}
		}
		cmp_n->next->data.sval = sa_strdup(sql->sa, compare_func(neg_cmp_type, 0));
	} break;
	case SQL_AND:
	case SQL_OR: {
		sc->data.lval->h->data.sym = negate_symbol_tree(sql, sc->data.lval->h->data.sym);
		sc->data.lval->h->next->data.sym= negate_symbol_tree(sql, sc->data.lval->h->next->data.sym);
		sc->token = sc->token == SQL_AND ? SQL_OR : SQL_AND;
	} break;
	default:
		break;
	}
	return sc;
}

static int
exp_between_check_types(sql_subtype *res, sql_subtype *t1, sql_subtype *t2, sql_subtype *t3)
{
	bool type_found = false;
	sql_subtype super;

	if (t1 && t2) {
		cmp_supertype(&super, t2, t1);
		type_found = true;
	} else if (t1) {
		super = *t1;
		type_found = true;
	} else if (t2) {
		super = *t2;
		type_found = true;
	}
	if (t3) {
		if (type_found)
			cmp_supertype(&super, t3, &super);
		else
			super = *t3;
		type_found = true;
	}
	if (!type_found)
		return -1;
	*res = super;
	return 0;
}

static bool
exp_is_null_no_value_opt(sql_exp *e)
{
	if (!e)
		return false;
	while (is_convert(e->type))
		e = e->l;
	return e->type == e_atom && e->l && atom_null(e->l);
}

sql_exp *
rel_logical_value_exp(sql_query *query, sql_rel **rel, symbol *sc, int f, exp_kind ek)
{
	mvc *sql = query->sql;

	if (!sc)
		return NULL;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	switch (sc->token) {
	case SQL_OR:
	case SQL_AND:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		sql_exp *ls, *rs;

		if (!(ls = rel_value_exp(query, rel, lo, f|sql_farg, ek)))
			return NULL;
		if (!(rs = rel_value_exp(query, rel, ro, f|sql_farg, ek)))
			return NULL;
		return rel_binop_(sql, rel ? *rel : NULL, ls, rs, "sys", sc->token == SQL_OR ? "or": "and", card_value, false);
	}
	case SQL_FILTER:
		/* [ x,..] filter [ y,..] */
		/* todo add anti, [ x,..] not filter [ y,...] */
		/* no correlation */
	{
		dnode *ln = sc->data.lval->h->data.lval->h;
		dnode *rn = sc->data.lval->h->next->next->data.lval->h;
		dlist *filter_op = sc->data.lval->h->next->data.lval;
		char *fname = qname_schema_object(filter_op);
		char *sname = qname_schema(filter_op);
		list *exps, *tl;
		sql_subtype *obj_type = NULL;

		exps = sa_list(sql->sa);
		tl = sa_list(sql->sa);
		for (; ln; ln = ln->next) {
			symbol *sym = ln->data.sym;

			sql_exp *e = rel_value_exp(query, rel, sym, f|sql_farg, ek);
			if (!e)
				return NULL;
			if (!obj_type)
				obj_type = exp_subtype(e);
			list_append(exps, e);
			append(tl, exp_subtype(e));
		}
		for (; rn; rn = rn->next) {
			symbol *sym = rn->data.sym;

			sql_exp *e = rel_value_exp(query, rel, sym, f|sql_farg, ek);
			if (!e)
				return NULL;
			list_append(exps, e);
			append(tl, exp_subtype(e));
		}
		/* find the predicate filter function */
		return _rel_nop(sql, sname, fname, tl, rel ? *rel : NULL, exps, ek);
	}
	case SQL_COMPARE:
	{
		dnode *n = sc->data.lval->h;
		symbol *lo = n->data.sym;
		symbol *ro = n->next->next->data.sym;
		char *compare_op = n->next->data.sval;
		int quantifier = 0, need_not = 0;
		sql_exp *rs = NULL, *ls;
		comp_type cmp_type = compare_str2type(compare_op);
		bool is_not_distinct_from = false;
		bool is_distinct_from = false;

		/*
		 * = ANY -> IN, <> ALL -> NOT( = ANY) -> NOT IN
		 * = ALL -> all(groupby(all, nil)), <> ANY -> NOT ( = ALL )
		 */
		if (n->next->next->next)
			quantifier = n->next->next->next->data.i_val + 1;
		assert(quantifier == 0 || quantifier == 1 || quantifier == 2 || quantifier == 3 || quantifier == 4);

		/* [NOT] DISTINCT FROM */
		if (quantifier == 3) {
				is_not_distinct_from = true;
				quantifier = 0;
		}
		else if (quantifier == 4) {
				is_distinct_from = true;
				quantifier = 0;
		}

		if ((quantifier == 1 && cmp_type == cmp_equal) ||
		    (quantifier == 2 && cmp_type == cmp_notequal)) {
			dlist *dl = dlist_create(sql->sa);
			/* map into IN/NOT IN */
			sc->token = cmp_type==cmp_equal?SQL_IN:SQL_NOT_IN;
			n->next->type = type_list;
			n->next->data.lval = dl;
			n->next->next->next = NULL; /* remove quantifier */
			dl->h = n->next->next;
			n->next->next = NULL; /* (remove comparison) moved righthand side */
			return rel_logical_value_exp(query, rel, sc, f, ek);
		}
		/* <> ANY -> NOT (= ALL) */
		if (quantifier == 1 && cmp_type == cmp_notequal) {
			need_not = 1;
			quantifier = 2;
			cmp_type = cmp_equal;
			compare_op = "=";
		}

		ls = rel_value_exp(query, rel, lo, f|sql_farg, ek);
		if (!ls)
			return NULL;
		if (quantifier)
			ek.card = card_set;

		rs = rel_value_exp(query, rel, ro, f|sql_farg, ek);
		if (!rs)
			return NULL;

		if (is_distinct_from || is_not_distinct_from) {
			if (rel_convert_types(sql, rel ? *rel : NULL, rel ? *rel : NULL, &ls, &rs, 1, type_equal_no_any) < 0)
				return NULL;
			sql_exp* e = exp_compare(sql->sa, ls, rs, is_not_distinct_from?cmp_equal:cmp_notequal);
			set_semantics(e);
			return e;
		}

		if (rs->type == e_atom)
			quantifier = 0;

		if (!exp_is_rel(ls) && !exp_is_rel(rs) && ls->card < rs->card) {
			sql_exp *swap = ls; /* has to swap parameters like in the rel_logical_exp case */
			ls = rs;
			rs = swap;
			cmp_type = swap_compare(cmp_type);
		}

		if (rel_convert_types(sql, rel ? *rel : NULL, rel ? *rel : NULL, &ls, &rs, 1, type_equal_no_any) < 0)
			return NULL;
		if (exp_is_null_no_value_opt(ls) && exp_is_null_no_value_opt(rs))
			return exp_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("bit"), NULL, 0));

		return exp_compare_func(sql, ls, rs, compare_func(cmp_type, need_not), quantifier);
	}
	/* Set Member ship */
	case SQL_IN:
	case SQL_NOT_IN:
		return rel_in_value_exp(query, rel, sc, f);
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
		return rel_exists_value_exp(query, rel, sc, f);
	case SQL_LIKE:
	case SQL_NOT_LIKE:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		int insensitive = sc->data.lval->h->next->next->data.i_val;
		int anti = (sc->token == SQL_NOT_LIKE) != (sc->data.lval->h->next->next->next->data.i_val != 0);
		sql_subtype *st = sql_bind_localtype("str");
		sql_exp *le = rel_value_exp(query, rel, lo, f|sql_farg, ek), *re, *ee = NULL, *ie = exp_atom_bool(sql->sa, insensitive);

		if (!le)
			return NULL;

		if (!exp_subtype(le))
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: parameter not allowed on "
					"left hand side of LIKE operator");

		lo = ro->data.lval->h->data.sym;
		/* like uses a single string pattern */
		ek.card = card_value;
		re = rel_value_exp(query, rel, lo, f|sql_farg, ek);
		if (!re)
			return NULL;
		if ((re = exp_check_type(sql, st, rel ? *rel : NULL, re, type_equal)) == NULL)
			return sql_error(sql, 02, SQLSTATE(42000) "LIKE: wrong type, should be string");
		if ((le = exp_check_type(sql, st, rel ? *rel : NULL, le, type_equal)) == NULL)
			return sql_error(sql, 02, SQLSTATE(42000) "LIKE: wrong type, should be string");
		/* Do we need to escape ? */
		if (dlist_length(ro->data.lval) == 2) {
			char *escape = ro->data.lval->h->next->data.sval;
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, escape)));
		} else {
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, "")));
		}
		return rel_nop_(sql, rel ? *rel : NULL, le, re, ee, ie, "sys", anti ? "not_like" : "like", card_value);
	}
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		int symmetric = sc->data.lval->h->next->data.i_val;
		symbol *ro1 = sc->data.lval->h->next->next->data.sym;
		symbol *ro2 = sc->data.lval->h->next->next->next->data.sym;
		sql_exp *le, *re1, *re2;
		sql_subtype super;

		assert(sc->data.lval->h->next->type == type_int);

		if (!(le = rel_value_exp(query, rel, lo, f|sql_farg, ek)))
			return NULL;
		if (!(re1 = rel_value_exp(query, rel, ro1, f|sql_farg, ek)))
			return NULL;
		if (!(re2 = rel_value_exp(query, rel, ro2, f|sql_farg, ek)))
			return NULL;

		if (exp_between_check_types(&super, exp_subtype(le), exp_subtype(re1), exp_subtype(re2)) < 0)
			return sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) on both sides of an expression");

		if ((le = exp_check_type(sql, &super, rel ? *rel:NULL, le, type_equal)) == NULL ||
		    (re1 = exp_check_type(sql, &super, rel ? *rel:NULL, re1, type_equal)) == NULL ||
		    (re2 = exp_check_type(sql, &super, rel ? *rel:NULL, re2, type_equal)) == NULL)
			return NULL;

		le = exp_compare2(sql->sa, le, re1, re2, 3, symmetric);
		if (sc->token == SQL_NOT_BETWEEN)
			set_anti(le);
		return le;
	}
	case SQL_IS_NULL:
	case SQL_IS_NOT_NULL:
	/* is (NOT) NULL */
	{
		sql_exp *le = rel_value_exp(query, rel, sc->data.sym, f|sql_farg, ek);

		if (!le)
			return NULL;
		le = rel_unop_(sql, rel ? *rel : NULL, le, "sys", sc->token == SQL_IS_NULL ? "isnull" : "isnotnull", card_value);
		if (!le)
			return NULL;
		set_has_no_nil(le);
		return le;
	}
	case SQL_NOT: {
		if (not_symbol_can_be_propagated(sql, sc->data.sym)) {
			sc->data.sym = negate_symbol_tree(sql, sc->data.sym);
			return rel_logical_value_exp(query, rel, sc->data.sym, f, ek);
		}
		sql_exp *le = rel_value_exp(query, rel, sc->data.sym, f|sql_farg, ek);

		if (!le)
			return NULL;
		return rel_unop_(sql, rel ? *rel : NULL, le, "sys", "not", card_value);
	}
	case SQL_ATOM: {
		AtomNode *an = (AtomNode *) sc;
		assert(an && an->a);
		return exp_atom(sql->sa, an->a);
	}
	case SQL_IDENT:
	case SQL_COLUMN:
		return rel_column_ref(query, rel, sc, f);
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT: {
		sql_rel *sq;

		if (is_psm_call(f) || is_sql_merge(f))
			return sql_error(sql, 02, SQLSTATE(42000) "%s: subqueries not supported inside %s", is_psm_call(f) ? "CALL" : "MERGE", is_psm_call(f) ? "CALL statements" : "MERGE conditions");
		if (rel && *rel)
			query_push_outer(query, *rel, f);
		sq = rel_setquery(query, sc);
		if (rel && *rel) {
			*rel = query_pop_outer(query);
			if (is_sql_join(f) && is_groupby((*rel)->op)) {
				return sql_error(sql, 05, SQLSTATE(42000) "SELECT: aggregate functions not allowed in JOIN conditions");
			} else if (is_sql_where(f) && is_groupby((*rel)->op)) {
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: aggregate functions not allowed in WHERE clause");
			} else if ((is_sql_update_set(f) || is_sql_psm(f)) && is_groupby((*rel)->op)) {
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: aggregate functions not allowed in SET, WHILE, IF, ELSE, CASE, WHEN, RETURN, ANALYZE clauses");
			}
		}
		if (!sq)
			return NULL;
		if (ek.type == type_value && ek.card <= card_set && is_project(sq->op) && list_length(sq->exps) > 1)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery must return only one column");
		if (ek.type == type_relation && is_project(sq->op) && list_length(sq->exps) != ek.type)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery has too %s columns", list_length(sq->exps) < ek.type ? "few" : "many");
		if (ek.type == type_value && ek.card < card_set && sq->card >= CARD_AGGR && (is_sql_sel(f) | is_sql_having(f) | is_sql_farg(f) |
			( is_sql_where(f) && rel && (!*rel || is_basetable((*rel)->op) || is_simple_project((*rel)->op) || is_joinop((*rel)->op)))))
			sq = rel_zero_or_one(sql, sq, ek);
		return exp_rel(sql, sq);
	}
	case SQL_DEFAULT:
		return sql_error(sql, 02, SQLSTATE(42000) "DEFAULT keyword not allowed outside insert and update statements");
	default: {
		sql_exp *le = rel_value_exp(query, rel, sc, f|sql_farg, ek);
		sql_subtype bt;

		if (!le)
			return NULL;
		sql_find_subtype(&bt, "boolean", 0, 0);
		if ((le = exp_check_type(sql, &bt, rel ? *rel : NULL, le, type_equal)) == NULL)
			return NULL;
		return rel_binop_(sql, rel ? *rel : NULL, le, exp_atom_bool(sql->sa, 1), "sys", "=", 0, true);
	}
	}
	/* never reached, as all switch cases have a `return` */
}

sql_rel *
rel_logical_exp(sql_query *query, sql_rel *rel, symbol *sc, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, TRUE};

	if (!sc)
		return NULL;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	switch (sc->token) {
	case SQL_OR:
	{
		list *exps = NULL, *lexps = NULL, *rexps = NULL;
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		sql_rel *lr, *rr;

		if (!rel)
			return NULL;

		lr = rel;
		rr = rel_dup(lr);

		if (is_outerjoin(rel->op) && !is_processed(rel)) {
			exps = rel->exps;

			lr = rel_select_copy(sql->sa, lr, sa_list(sql->sa));
			lr = rel_logical_exp(query, lr, lo, f | sql_or);
			if (!lr)
				return NULL;
			query_processed(query);
			rr = rel_select_copy(sql->sa, rr, sa_list(sql->sa));
			rr = rel_logical_exp(query, rr, ro, f | sql_or);
			if (!rr)
				return NULL;
			if (lr->l == rr->l) {
				lexps = lr->exps;
				lr = lr->l;
				rexps = rr->exps;
				rr = rr->l;
			}
			rel = NULL;
		} else {
			lr = rel_logical_exp(query, lr, lo, f | sql_or);
			if (!lr)
				return NULL;
			rr = rel_logical_exp(query, rr, ro, f | sql_or);
		}

		if (!lr || !rr)
			return NULL;
		return rel_or(sql, rel, lr, rr, exps, lexps, rexps);
	}
	case SQL_AND:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		rel = rel_logical_exp(query, rel, lo, f);
		if (!rel)
			return NULL;
		return rel_logical_exp(query, rel, ro, f);
	}
	case SQL_FILTER:
		/* [ x,..] filter [ y,..] */
		/* todo add anti, [ x,..] NOT filter [ y,...] */
		/* no correlation */
	{
		dnode *ln = sc->data.lval->h->data.lval->h;
		dnode *rn = sc->data.lval->h->next->next->data.lval->h;
		dlist *filter_op = sc->data.lval->h->next->data.lval;
		char *fname = qname_schema_object(filter_op);
		char *sname = qname_schema(filter_op);
		list *l, *r;

		l = sa_list(sql->sa);
		r = sa_list(sql->sa);
		for (; ln; ln = ln->next) {
			symbol *sym = ln->data.sym;

			sql_exp *e = rel_value_exp(query, &rel, sym, f|sql_farg, ek);
			if (!e)
				return NULL;
			list_append(l, e);
		}
		for (; rn; rn = rn->next) {
			symbol *sym = rn->data.sym;

			sql_exp *e = rel_value_exp(query, &rel, sym, f|sql_farg, ek);
			if (!e)
				return NULL;
			list_append(r, e);
		}
		return rel_filter(sql, rel, l, r, sname, fname, 0, f);
	}
	case SQL_COMPARE:
	{
		dnode *n = sc->data.lval->h;
		symbol *lo = n->data.sym;
		symbol *ro = n->next->next->data.sym;
		char *compare_op = n->next->data.sval;
		int quantifier = 0;
		int is_semantics = 0;

		if (n->next->next->next)
			quantifier = n->next->next->next->data.i_val + 1;
		assert(quantifier == 0 || quantifier == 1 || quantifier == 2 || quantifier == 3 || quantifier == 4);

		if (quantifier >= 3) {
			if (quantifier == 4)
				compare_op = "<>";
			quantifier = 0;
			is_semantics = 1;
		}
		return rel_compare(query, rel, sc, lo, ro, compare_op, f, ek, quantifier, is_semantics);
	}
	/* Set Member ship */
	case SQL_IN:
	case SQL_NOT_IN:
		return rel_in_exp(query, rel, sc, f);
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
		return rel_exists_exp(query, rel , sc, f);
	case SQL_LIKE:
	case SQL_NOT_LIKE:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		int insensitive = sc->data.lval->h->next->next->data.i_val;
		int anti = (sc->token == SQL_NOT_LIKE) != (sc->data.lval->h->next->next->next->data.i_val != 0);
		sql_subtype *st = sql_bind_localtype("str");
		sql_exp *le = rel_value_exp(query, &rel, lo, f|sql_farg, ek), *re, *ee = NULL, *ie = exp_atom_bool(sql->sa, insensitive);

		if (!le)
			return NULL;

		if (!exp_subtype(le))
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: parameter not allowed on "
					"left hand side of LIKE operator");

		/* Do we need to escape ? */
		if (dlist_length(ro->data.lval) == 2) {
			char *escape = ro->data.lval->h->next->data.sval;
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, escape)));
		} else {
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, "")));
		}
		ro = ro->data.lval->h->data.sym;
		re = rel_value_exp(query, &rel, ro, f|sql_farg, ek);
		if (!re)
			return NULL;
		if ((re = exp_check_type(sql, st, rel, re, type_equal)) == NULL)
			return sql_error(sql, 02, SQLSTATE(42000) "LIKE: wrong type, should be string");
		if ((le = exp_check_type(sql, st, rel, le, type_equal)) == NULL)
			return sql_error(sql, 02, SQLSTATE(42000) "LIKE: wrong type, should be string");
		return rel_filter_exp_(sql, rel, le, re, ee, ie, "like", anti, f);
	}
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		int symmetric = sc->data.lval->h->next->data.i_val;
		symbol *ro1 = sc->data.lval->h->next->next->data.sym;
		symbol *ro2 = sc->data.lval->h->next->next->next->data.sym;
		sql_exp *le, *re1, *re2;
		sql_subtype super;

		assert(sc->data.lval->h->next->type == type_int);

		if (!(le = rel_value_exp(query, &rel, lo, f|sql_farg, ek)))
			return NULL;
		if (!(re1 = rel_value_exp(query, &rel, ro1, f|sql_farg, ek)))
			return NULL;
		if (!(re2 = rel_value_exp(query, &rel, ro2, f|sql_farg, ek)))
			return NULL;

		if (exp_between_check_types(&super, exp_subtype(le), exp_subtype(re1), exp_subtype(re2)) < 0)
			return sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) on both sides of an expression");

		if ((le = exp_check_type(sql, &super, rel, le, type_equal)) == NULL ||
		    (re1 = exp_check_type(sql, &super, rel, re1, type_equal)) == NULL ||
		    (re2 = exp_check_type(sql, &super, rel, re2, type_equal)) == NULL)
			return NULL;

		return rel_compare_exp_(query, rel, le, re1, re2, 3, sc->token == SQL_NOT_BETWEEN ? 1 : 0, 0, f, symmetric, 0);
	}
	case SQL_IS_NULL:
	case SQL_IS_NOT_NULL:
	/* is (NOT) NULL */
	{
		sql_exp *le = rel_value_exp(query, &rel, sc->data.sym, f|sql_farg, ek);
		sql_subtype *t;

		if (!le)
			return NULL;
		if (!(t = exp_subtype(le)))
			return sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) for IS%s NULL operator", sc->token == SQL_IS_NOT_NULL ? " NOT" : "");
		le = exp_compare(sql->sa, le, exp_atom(sql->sa, atom_general(sql->sa, t, NULL, 0)), cmp_equal);
		if (sc->token == SQL_IS_NOT_NULL)
			set_anti(le);
		set_has_no_nil(le);
		set_semantics(le);
		return rel_select_push_compare_exp_down(sql, rel, le, le->l, le->r, NULL, f);
	}
	case SQL_NOT: {
		if (not_symbol_can_be_propagated(sql, sc->data.sym)) {
			sc->data.sym = negate_symbol_tree(sql, sc->data.sym);
			return rel_logical_exp(query, rel, sc->data.sym, f);
		}
		sql_exp *le = rel_value_exp(query, &rel, sc->data.sym, f|sql_farg, ek);
		sql_subtype bt;

		sql_find_subtype(&bt, "boolean", 0, 0);
		if (!le || !(le = exp_check_type(sql, &bt, rel, le, type_equal)))
			return NULL;
		le = exp_compare(sql->sa, le, exp_atom_bool(sql->sa, 0), cmp_equal);
		return rel_select_push_compare_exp_down(sql, rel, le, le->l, le->r, NULL, f);
	}
	case SQL_ATOM: {
		/* TRUE or FALSE */
		sql_rel *or = rel;
		AtomNode *an = (AtomNode *) sc;
		sql_exp *e = exp_atom(sql->sa, an->a);

		if (e) {
			sql_subtype bt;

			sql_find_subtype(&bt, "boolean", 0, 0);
			e = exp_check_type(sql, &bt, rel, e, type_equal);
		}
		if (!e || or != rel)
			return NULL;
		e = exp_compare(sql->sa, e, exp_atom_bool(sql->sa, 1), cmp_equal);
		return rel_select_push_compare_exp_down(sql, rel, e, e->l, e->r, NULL, f);
	}
	case SQL_IDENT:
	case SQL_COLUMN: {
		sql_rel *or = rel;
		sql_exp *e = rel_column_ref(query, &rel, sc, f);

		if (e) {
			sql_subtype bt;

			sql_find_subtype(&bt, "boolean", 0, 0);
			e = exp_check_type(sql, &bt, rel, e, type_equal);
		}
		if (!e || or != rel)
			return NULL;
		e = exp_compare(sql->sa, e, exp_atom_bool(sql->sa, 1), cmp_equal);
		return rel_select_push_compare_exp_down(sql, rel, e, e->l, e->r, NULL, f);
	}
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT: {
		sql_rel *sq;

		if (is_psm_call(f) || is_sql_merge(f))
			return sql_error(sql, 02, SQLSTATE(42000) "%s: set operations not supported inside %s", is_psm_call(f) ? "CALL" : "MERGE", is_psm_call(f) ? "CALL statements" : "MERGE conditions");
		if (rel)
			query_push_outer(query, rel, f);
		sq = rel_setquery(query, sc);
		if (rel) {
			rel = query_pop_outer(query);
			if (is_sql_join(f) && is_groupby(rel->op)) {
				return sql_error(sql, 05, SQLSTATE(42000) "SELECT: aggregate functions not allowed in JOIN conditions");
			} else if (is_sql_where(f) && is_groupby(rel->op)) {
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: aggregate functions not allowed in WHERE clause");
			} else if ((is_sql_update_set(f) || is_sql_psm(f)) && is_groupby(rel->op)) {
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: aggregate functions not allowed in SET, WHILE, IF, ELSE, CASE, WHEN, RETURN, ANALYZE clauses");
			}
		}
		if (!sq)
			return NULL;
		assert(ek.type == type_value); /* I don't expect IN tuple matching calls to land here */
		if (is_sql_where(f) && is_groupby(rel->op))
			assert(0);
		if (ek.card <= card_set && is_project(sq->op) && list_length(sq->exps) > 1)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery must return only one column");
		if (!rel)
			return sq;
		sq = rel_zero_or_one(sql, sq, ek);
		if (is_sql_where(f) || is_sql_having(f)) {
			sql_exp *le = exp_rel(sql, sq);
			sql_subtype bt;

			sql_find_subtype(&bt, "boolean", 0, 0);
			le = exp_check_type(sql, &bt, rel, le, type_equal);
			if (!le)
				return NULL;
			le = exp_compare(sql->sa, le, exp_atom_bool(sql->sa, 1), cmp_equal);
			return rel_select_push_compare_exp_down(sql, rel, le, le->l, le->r, NULL, f);
		} else {
			sq = rel_crossproduct(sql->sa, rel, sq, (f==sql_sel || is_single(sq))?op_left:op_join);
			set_processed(sq);
		}
		return sq;
	}
	case SQL_DEFAULT:
		return sql_error(sql, 02, SQLSTATE(42000) "DEFAULT keyword not allowed outside insert and update statements");
	default: {
		sql_exp *le = rel_value_exp(query, &rel, sc, f|sql_farg, ek);
		sql_subtype bt;

		if (!le)
			return NULL;
		if (le && (!is_compare(le->type) || le->flag > cmp_filter)) {
			sql_find_subtype(&bt, "boolean", 0, 0);
			if (!(le = exp_check_type(sql, &bt, rel, le, type_equal)))
				return NULL;
			le = exp_compare(sql->sa, le, exp_atom_bool(sql->sa, 1), cmp_equal);
		}
		if (le->flag == cmp_filter)
			return rel_select_push_filter_exp_down(sql, rel, le, le->l, le->r, f);
		else
			return rel_select_push_compare_exp_down(sql, rel, le, le->l, le->r, le->f, f);
	}
	}
	/* never reached, as all switch cases have a `return` */
}

static sql_exp * _rel_aggr(sql_query *query, sql_rel **rel, int distinct, char *sname, char *aname, dnode *arguments, symbol *orderby, int f);
static sql_exp *rel_aggr(sql_query *query, sql_rel **rel, symbol *se, int f);

sql_exp *
rel_unop_(mvc *sql, sql_rel *rel, sql_exp *e, char *sname, char *fname, int card)
{
	bool found = false;
	sql_subtype *t = exp_subtype(e);
	sql_ftype type = (card == card_loader)?F_LOADER:((card == card_none)?F_PROC:
		   ((card == card_relation)?F_UNION:F_FUNC));

	sql_subfunc *f = bind_func(sql, sname, fname, t, NULL, 1, type, false, &found, false);
	if (f) {
		sql_arg *a = f->func->ops->h->data;
		t = &a->type;
	}
	if (f && t && type_has_tz(t) && f->func->fix_scale == SCALE_FIX) {
		/* set timezone (using msec (.3)) */
		sql_subtype *intsec = sql_bind_subtype(sql->sa, "sec_interval", 10 /*hour to second */, 3);
		atom *a = atom_int(sql->sa, intsec, sql->timezone);
		sql_exp *tz = exp_atom(sql->sa, a);

		e = rel_binop_(sql, rel, e, tz, "sys", "sql_add", card, true);
		if (!e)
			return NULL;
	}
	if (f) {
		if (check_card(card, f)) {
			list *args = list_append(sa_list(sql->sa), e);
			args = check_arguments_and_find_largest_any_type(sql, rel, args, f, card == card_relation && e->card > CARD_ATOM, false);
			if (args)
				return exp_op(sql->sa, args, f);
		}
		found = false; /* reset found */
		f = NULL;
	}
	/* reset error */
	sql->session->status = 0;
	sql->errstr[0] = '\0';
	return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: %s unary operator %s%s%s'%s'(%s)",
					 found ? "insufficient privileges for" : "no such", sname ? "'":"", sname ? sname : "", sname ? "'.":"", fname, t ? t->type->base.name : "?");
}

sql_exp *
rel_binop_(mvc *sql, sql_rel *rel, sql_exp *l, sql_exp *r, char *sname, char *fname, int card, bool exact)
{
	sql_subtype *t1 = exp_subtype(l), *t2 = exp_subtype(r);
	sql_ftype type = (card == card_loader)?F_LOADER:((card == card_none)?F_PROC:((card == card_relation)?F_UNION:F_FUNC));
	bool found = false;

	if (card == card_loader)
		card = card_none;

	if (is_commutative(sname, fname) && l->card < r->card) { /* move constants to the right if possible */
		sql_subtype *tmp = t1;
		t1 = t2;
		t2 = tmp;
		sql_exp *res = l;
		l = r;
		r = res;
	}
	/* swap complex types (intervals) to left side of +, * */
	if (t1 && t2 && is_commutative(sname, fname)) {
		if ((EC_INTERVAL(t1->type->eclass) && EC_TEMP(t2->type->eclass)) ||
           ((!EC_TEMP(t1->type->eclass) && !EC_INTERVAL(t1->type->eclass)) && EC_INTERVAL(t2->type->eclass))) {
			sql_subtype *tmp = t1;
			t1 = t2;
			t2 = tmp;
			sql_exp *res = l;
			l = r;
			r = res;
		}
	}

	sql_subfunc *f = bind_func(sql, sname, fname, t1, t2, 2, type, false, &found, exact);
	if (f && check_card(card,f)) {
		t1 = exp_subtype(l);
		t2 = exp_subtype(r);
		list *args = list_append(list_append(sa_list(sql->sa), l), r);

		args = check_arguments_and_find_largest_any_type(sql, rel, args, f, 0, false);
		if (args)
				return exp_op(sql->sa, args, f);
		return NULL;
	}

	if (!exp_subtype(l) || !exp_subtype(r))
		return sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) on both sides of an expression");
	return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: %s binary operator %s%s%s'%s'(%s,%s)",
					 found ? "insufficient privileges for" : "no such", sname ? "'":"", sname ? sname : "", sname ? "'.":"", fname,
					 exp_subtype(l)->type->base.name, exp_subtype(r)->type->base.name);
}

sql_exp *
rel_nop_(mvc *sql, sql_rel *rel, sql_exp *a1, sql_exp *a2, sql_exp *a3, sql_exp *a4, char *sname, char *fname, int card)
{
	list *tl = sa_list(sql->sa);
	sql_subfunc *f = NULL;
	sql_ftype type = (card == card_none)?F_PROC:((card == card_relation)?F_UNION:F_FUNC);

	/* rel_nop_ should only be called for functions available to everyone, ie defined at sql_types! */
	(void) rel;
	append(tl, exp_subtype(a1));
	append(tl, exp_subtype(a2));
	append(tl, exp_subtype(a3));
	if (a4)
		append(tl, exp_subtype(a4));

	if (!(f = bind_func_(sql, sname, fname, tl, type, false, NULL, true)))
		return NULL;
	if (!a4)
		return exp_op3(sql->sa, a1,a2,a3,f);
	return exp_op4(sql->sa, a1,a2,a3,a4,f);
}

static sql_func *
inplace_func(mvc *sql)
{
	sql_func *f = SA_NEW(sql->sa, sql_func);

    *f = (sql_func) {
        .mod = "",
        .imp = "",
        .type = F_PROC,
        .lang = FUNC_LANG_INT,
        .query = NULL,
        .ops = sql->params,
        .res = NULL,
    };
    base_init(sql->sa, &f->base, 0, true, NULL);
    f->base.new = 1;
    f->base.id = -1;
    f->base.name = "-1";
    f->instantiated = TRUE;
	return f;
}

static list *
reorder_args(mvc *sql, list *exps, list *names, list *params)
{
	list *nexps = sa_list(sql->sa);
	for(node *n = params->h; n; n = n->next) {
		sql_arg *a = n->data;
		int found =0;
		for(node *m = names->h, *o = exps->h; m && o; m = m->next, o = o->next) {
			if (strcmp(m->data, a->name) == 0) {
				append(nexps, o->data);
				found = 1;
				break;
			}
		}
		if (!found)
			return NULL;
	}
	return nexps;
}

static sql_exp *
rel_nop(sql_query *query, sql_rel **rel, symbol *se, int fs, exp_kind ek)
{
	mvc *sql = query->sql;
	dnode *l = se->data.lval->h;
	dnode *ops = l->next->next->data.lval?l->next->next->data.lval->h:NULL;
	list *names = NULL;
	char *fname = NULL, *sname = NULL;;

	if (l->type != type_int) {
		int nargs = 0;
		fname = qname_schema_object(l->data.lval);
		sname = qname_schema(l->data.lval);

		for (dnode *n = ops; n; n = n->next)
			nargs++;

		/* first try aggregate */
		if (find_func(sql, sname, fname, nargs, F_AGGR, false, NULL, NULL)) {
			dnode *dn = l->next->next;
			symbol *orderby = dn->next?dn->next->data.sym:NULL;
			return _rel_aggr(query, rel, l->next->data.i_val, sname, fname, dn->data.lval->h, orderby, fs);
		}
	}

	int nr_args = 0;
	int split = (l->type == type_int && l->data.i_val == -1);
	list *exps = sa_list(sql->sa), *tl = sa_list(sql->sa);
	exp_kind iek = {type_value, card_column, FALSE};

	if (split)
		names = sa_list(sql->sa);
	for (; ops; ops = ops->next, nr_args++) {
		sql_exp *e = rel_value_exp(query, rel, ops->data.sym, fs|sql_farg, iek);
		if (!e)
			return NULL;
		if (split) {
			ops = ops->next;
			append(names, ops->data.sval);
		}
		append(exps, e);
		append(tl, exp_subtype(e));
	}
	if (l->type == type_int) {
		/* exec nr (ops)*/
		int nr = l->data.i_val;
		cq *q = NULL;

		if (nr == -1 || (q = qc_find(sql->qc, nr))) {
			list *nexps = new_exp_list(sql->sa);
			sql_func *f = q?q->f:inplace_func(sql);
			list *ops = q?f->ops:sql->params;

			tl = sa_list(sql->sa);
			if (list_length(ops) != list_length(exps))
				return sql_error(sql, 02, SQLSTATE(42000) "EXEC called with wrong number of arguments: expected %d, got %d", list_length(ops), list_length(exps));
			if (split) {
				exps = reorder_args(sql, exps, names, ops);
				if (!exps)
					return sql_error(sql, 02, SQLSTATE(42000) "EXEC called with wrong arguments");
			}
			if (exps->h && ops) {
				for (node *n = exps->h, *m = ops->h; n && m; n = n->next, m = m->next) {
					sql_arg *a = m->data;
					sql_exp *e = n->data;
					sql_subtype *ntp = &a->type;

					if (ntp && ntp->type)
						e = exp_check_type(sql, ntp, NULL, e, type_equal);
					else
						a->type = *exp_subtype(e);
					if (!e)
						return NULL;
					append(nexps, e);
					append(tl, exp_subtype(e));
				}
			}

			if (q)
				sql->type = q->type;
			return exp_op(sql->sa, list_empty(nexps) ? NULL : nexps, sql_dup_subfunc(sql->sa, f, tl, NULL));
		} else {
			return sql_error(sql, 02, SQLSTATE(42000) "EXEC: PREPARED Statement missing '%d'", nr);
		}
	}

	if (nr_args == 2 && is_commutative(sname, fname)) {
		sql_subtype *t1 = tl->h->data;
		sql_subtype *t2 = tl->t->data;

		if (t1 && t2 && ((EC_INTERVAL(t1->type->eclass) && EC_TEMP(t2->type->eclass)) ||
           ((!EC_TEMP(t1->type->eclass) && !EC_INTERVAL(t1->type->eclass)) && EC_INTERVAL(t2->type->eclass)))) {
			list_revert(exps);
			list_revert(tl);
		}

	}

	if (!sname && strcmp(fname, "field") == 0) { /* map into join */
		if (list_length(exps) < 2)
			return sql_error(sql, 02, SQLSTATE(42000) "Field function called with not enough arguments");
		sql_exp *le = exps->h->data;
		set_freevar(le, 1);
		list_remove_data(exps, NULL, le);
		sql_exp *re = exp_values(sql->sa, exps);
		exp_label(sql->sa, re, ++sql->label);
		sql_rel *r = rel_project(sql->sa, NULL, append(sa_list(sql->sa), re));
		sql_exp *id = NULL;
		rel_add_identity(sql, r, &id);
		re = exp_ref(sql, re);
		id = exp_ref(sql, id);
		if (r) {
			r->nrcols = list_length(exps);
			sql_exp *e = exp_compare(sql->sa, le, re, cmp_equal);
			r = rel_select(sql->sa, r, e);
			r = rel_project(sql->sa, r, append(sa_list(sql->sa), exp_convert(sql, id, exp_subtype(id), sql_bind_localtype("int"))));
			re = exp_rel(sql, r);
			return re;
		}
	}
	return _rel_nop(sql, sname, fname, tl, rel ? *rel : NULL, exps, ek);
}

typedef struct aggr_input {
	sql_query *query;
	int groupby;
	char *err;
} aggr_input;

static sql_exp *
exp_valid(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	aggr_input *ai = v->data;
	(void)rel; (void)depth;

	int vf = is_freevar(e);
	if (!v->changes && vf && vf < ai->groupby) { /* check need with outer query */
		sql_rel *sq = query_fetch_outer(ai->query, vf-1);

		/* problem freevar have cardinality CARD_ATOM */
		if (sq->card <= CARD_AGGR && exp_card(e) != CARD_AGGR && is_alias(e->type)) {
			if (!exps_bind_column(sq->exps, e->l, e->r, NULL, 0)) {
				v->changes = 1;
				ai->err = SQLSTATE(42000) "SELECT: subquery uses ungrouped column from outer query";
			}
		}
	} else if (!v->changes && vf && vf == ai->groupby) { /* check if input is already aggregated */
		sql_rel *sq = query_fetch_outer(ai->query, vf-1);
		sql_exp *a = NULL;

		if (sq->card <= CARD_AGGR && is_alias(e->type)) {
			if ((a = exps_bind_column(sq->exps, e->l, e->r, NULL, 0)) && is_aggr(a->type)) { /* aggregate */
				v->changes = 1;
				ai->err = SQLSTATE(42000) "SELECT: aggregate function calls cannot be nested";
			}
		}
	}
	return e;
}

static char *
exps_valid(sql_query *query, list *exps, int groupby)
{
	aggr_input ai = { .query = query, .groupby = groupby };
	visitor v = { .sql = query->sql, .data = &ai };

	exps_exp_visitor_topdown(&v, NULL, exps, 0, &exp_valid, true);
	if (v.changes)
		return ai.err;
	return NULL;
}

static list * rel_order_by(sql_query *query, sql_rel **R, symbol *orderby, int needs_distinct, int f);

static sql_exp *
_rel_aggr(sql_query *query, sql_rel **rel, int distinct, char *sname, char *aname, dnode *args, symbol *orderby, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_subfunc *a = NULL;
	int no_nil = 0, group = 0, all_aggr = query_has_outer(query), i;
	unsigned int all_freevar = 0;
	sql_rel *groupby = rel ? *rel : NULL, *sel = NULL, *gr, *og = NULL, *res = groupby;
	sql_rel *subquery = NULL;
	list *exps = NULL, *ungrouped_cols = NULL;
	bool is_grouping = !strcmp(aname, "grouping"), has_args = false, found = false, used_rel = false;

	if (!all_aggr) {
		if (!groupby) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: missing group by", toUpperCopy(uaname, aname));
		} else if (is_sql_groupby(f)) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate function '%s' not allowed in GROUP BY clause", toUpperCopy(uaname, aname), aname);
		} else if (is_sql_values(f)) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed on an unique value", toUpperCopy(uaname, aname));
		} else if (is_sql_join(f)) { /* the is_sql_join test must come before is_sql_where, because the join conditions are handled with sql_where */
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed in JOIN conditions", toUpperCopy(uaname, aname));
		} else if (is_sql_where(f)) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed in WHERE clause", toUpperCopy(uaname, aname));
		} else if (is_sql_update_set(f) || is_sql_psm(f)) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed in SET, WHILE, IF, ELSE, CASE, WHEN, RETURN, ANALYZE clauses (use subquery)", toUpperCopy(uaname, aname));
		} else if (is_sql_aggr(f)) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions cannot be nested", toUpperCopy(uaname, aname));
		} else if (is_psm_call(f)) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed inside CALL", toUpperCopy(uaname, aname));
		} else if (is_sql_from(f)) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed in functions in FROM", toUpperCopy(uaname, aname));
		}
	}

	exps = sa_list(sql->sa);
	if (args && args->data.sym) {
		bool arguments_correlated = true, all_const = true;

		all_freevar = all_aggr?1:0;
		for (i = 0; args && args->data.sym; args = args->next, i++) {
			int base = (!groupby || !is_project(groupby->op) || is_base(groupby->op) || is_processed(groupby));
			sql_rel *gl = base?groupby:groupby->l, *ogl = gl; /* handle case of subqueries without correlation */
			sql_exp *e = rel_value_exp(query, &gl, args->data.sym, (f | sql_aggr)& ~sql_farg, ek);
			bool found_one_freevar = false;

			if (!e)
				return NULL;
			used_rel |= (rel_has_exp(gl, e, true) == 0);
			has_args = true;
			if (gl && gl != ogl) {
				if (gl->grouped) {
					char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
					return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions cannot be nested", toUpperCopy(uaname, aname));
				}
				if (!base)
					groupby->l = subquery = gl;
				else
					groupby = subquery = gl;
			}
			sql_subtype *t = exp_subtype(e);
			if (!t) { /* we also do not expect parameters here */
				char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
				return sql_error(sql, 02, SQLSTATE(42000) "%s: parameters not allowed as arguments to aggregate functions", toUpperCopy(uaname, aname));
			}
			if (!t->type->localtype) {
				if (e->type == e_atom && !e->f) {
					t = sql_bind_localtype("bte");
					e->tpe = *t;
					if (e->l)
						e->l = atom_set_type(sql->sa, e->l, t);
				}
			}

			all_aggr &= (exp_card(e) <= CARD_AGGR && !exp_is_atom(e) && is_aggr(e->type) && !is_func(e->type) && (!groupby || !is_groupby(groupby->op) || !groupby->r || !exps_find_exp(groupby->r, e)));
			exp_only_freevar(query, e, &arguments_correlated, &found_one_freevar, &ungrouped_cols);
			all_freevar &= (arguments_correlated && found_one_freevar) || (is_atom(e->type)?all_freevar:0); /* no uncorrelated variables must be found, plus at least one correlated variable to push this aggregate to an outer query */
			all_const &= is_atom(e->type);
			list_append(exps, e);
		}
		if (all_const)
			all_freevar = 0;
	}
	if (!all_freevar) {
		if (is_sql_groupby(f)) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate function '%s' not allowed in GROUP BY clause", toUpperCopy(uaname, aname), aname);
		} else if (is_sql_from(f)) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed in functions in FROM", toUpperCopy(uaname, aname));
		} else if (is_sql_aggr(f) && groupby->grouped) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions cannot be nested", toUpperCopy(uaname, aname));
		} else if (is_sql_values(f)) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed on an unique value", toUpperCopy(uaname, aname));
		} else if (is_sql_join(f)) { /* the is_sql_join test must come before is_sql_where, because the join conditions are handled with sql_where */
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed in JOIN conditions", toUpperCopy(uaname, aname));
		} else if (is_sql_where(f)) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed in WHERE clause", toUpperCopy(uaname, aname));
		} else if (!all_aggr && !list_empty(ungrouped_cols)) {
			for (node *n = ungrouped_cols->h ; n ; n = n->next) {
				sql_rel *outer;
				sql_exp *e = (sql_exp*) n->data;

				if ((outer = query_fetch_outer(query, is_freevar(e)-1))) {
					int of = query_fetch_outer_state(query, is_freevar(e)-1);
					if (outer->grouped) {
						bool err = false, was_processed = false;

						if (is_processed(outer)) {
							was_processed = true;
							reset_processed(outer);
						}
						if (!is_groupby_col(outer, e))
							err = true;
						if (was_processed)
							set_processed(outer);
						if (err) {
							if (exp_name(e) && exp_relname(e) && !has_label(e))
								return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: subquery uses ungrouped column \"%s.%s\" from outer query", exp_relname(e), exp_name(e));
							return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: subquery uses ungrouped column from outer query");
						}
					} else if (!used_rel && is_sql_where(of)) {
						char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
						return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed in WHERE clause", toUpperCopy(uaname, aname));
					} else if (!is_sql_aggr(of)) {
						set_outer(outer);
					}
				}
			}
		}
	}

	if (all_freevar) { /* case 2, ie use outer */
		int card;
		sql_exp *exp = NULL;
		/* find proper groupby relation */
		for (node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			int vf = exp_freevar_offset(sql, e);
			if (vf > (int)all_freevar)
				all_freevar = vf;
			exp = e;
		}
		if (query_has_outer(query) >= all_freevar) {
			int sql_state = query_fetch_outer_state(query,all_freevar-1);
			res = groupby = query_fetch_outer(query, all_freevar-1);
			card = query_outer_used_card(query, all_freevar-1);
			/* given groupby validate all input expressions */
			char *err;
			if (groupby && !is_groupby(groupby->op)) {
				sql_exp *p = query_outer_last_used(query, all_freevar-1);
				if (p && !is_aggr(p->type) && !is_groupby_col(groupby, p)) {
					if (p->type == e_column)
						return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", (char*)p->l, (char*)p->r);
					if (exp_name(p) && exp_relname(p) && !has_label(p))
						return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", exp_relname(p), exp_name(p));
					return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
				}
			}
			if ((err = exps_valid(query, exps, all_freevar)) != NULL) {
				strcpy(sql->errstr, err);
				sql->session->status = -ERR_GROUPBY;
				return NULL;
			}
			if (exp && !is_groupby_col(res, exp)) {
				if (is_sql_groupby(sql_state))
					return sql_error(sql, 05, SQLSTATE(42000) "SELECT: aggregate function '%s' not allowed in GROUP BY clause", aname);
				if (is_sql_aggr(sql_state) && groupby->grouped) {
					char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
					return sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions cannot be nested", toUpperCopy(uaname, aname));
				}
				if (is_sql_values(sql_state))
					return sql_error(sql, 05, SQLSTATE(42000) "SELECT: aggregate functions not allowed on an unique value");
				if (is_sql_update_set(sql_state) || is_sql_psm(f))
					return sql_error(sql, 05, SQLSTATE(42000) "SELECT: aggregate functions not allowed in SET, WHILE, IF, ELSE, CASE, WHEN, RETURN, ANALYZE clauses");
				if (is_sql_join(sql_state))
					return sql_error(sql, 05, SQLSTATE(42000) "SELECT: aggregate functions not allowed in JOIN conditions");
				if (is_sql_where(sql_state))
					return sql_error(sql, 05, SQLSTATE(42000) "SELECT: aggregate functions not allowed in WHERE clause");
				if (is_psm_call(sql_state))
					return sql_error(sql, 05, SQLSTATE(42000) "CALL: aggregate functions not allowed inside CALL");
				if (is_sql_from(sql_state))
					return sql_error(sql, 05, SQLSTATE(42000) "SELECT: aggregate functions not allowed in functions in FROM");
				if (card > CARD_AGGR) { /* used an expression before on the non grouped relation */
					sql_exp *lu = query_outer_last_used(query, all_freevar-1);
					if (lu->type == e_column)
						return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: subquery uses ungrouped column \"%s.%s\" from outer query", (char*)lu->l, (char*)lu->r);
					if (exp_name(lu) && exp_relname(lu) && !has_label(lu))
						return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: subquery uses ungrouped column \"%s.%s\" from outer query", exp_relname(lu), exp_name(lu));
					return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: subquery uses ungrouped column from outer query");
				}
				if (is_outer(groupby))
					return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: subquery uses ungrouped column from outer query");
			}
		} else {
			all_freevar = 0;
		}
	} else if (!subquery && groupby && is_outer(groupby) && !is_groupby(groupby->op))
		return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: subquery uses ungrouped column from outer query");

	/* find having select */
	if (!subquery && groupby && !is_processed(groupby) && is_sql_having(f)) {
		og = groupby;
		while(!is_processed(groupby) && !is_base(groupby->op)) {
			if (is_select(groupby->op) || !groupby->l)
				break;
			if (groupby->l)
				groupby = groupby->l;
		}
		if (groupby && is_select(groupby->op) && !is_processed(groupby)) {
			group = 1;
			sel = groupby;
			/* At the end we switch back to the old projection relation og.
			 * During the partitioning and ordering we add the expressions to the intermediate relations. */
		}
		if (!sel)
			groupby = og;
		if (sel && sel->l)
			groupby = sel->l;
	}

	/* find groupby */
	if (!subquery && groupby && !is_processed(groupby) && !is_base(groupby->op)) {
		og = groupby;
		groupby = rel_find_groupby(groupby);
		if (groupby)
			group = 1;
		else
			groupby = og;
	}

	if (!groupby && exps_card(exps) > CARD_ATOM) {
		char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
		return sql_error(sql, 02, SQLSTATE(42000) "%s: missing group by", toUpperCopy(uaname, aname));
	}

	if (!subquery && groupby && groupby->op != op_groupby) {		/* implicit groupby */
		if (!all_freevar && query->last_exp && !is_sql_aggr(query->last_state)) {
			if (exp_relname(query->last_exp) && exp_name(query->last_exp) && !has_label(query->last_exp))
				return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", exp_relname(query->last_exp), exp_name(query->last_exp));
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
		}
		res = groupby = rel_groupby(sql, groupby, NULL);
	}
	if (subquery) {
		assert(!all_freevar);
		res = groupby;
		if (is_sql_sel(f) && is_left(subquery->op) && !is_groupby(groupby->op)) {
			res = groupby = rel_groupby(sql, groupby, NULL);
			exps_set_freevar(sql, exps, groupby); /* mark free variables */
		} else if (!is_groupby(groupby->op)) {
			res = groupby = rel_groupby(sql, groupby, NULL);
		}
		assert(!is_base(groupby->op));
	}
	if ((!exps || exps_card(exps) > CARD_ATOM) && (!res || !groupby))
		return NULL;

	list *obe = NULL;
	bool handled_order = true;
	if (args)
			return NULL;
	if (orderby) { /* handle order by */
		int base = (!groupby || !is_project(groupby->op) || is_base(groupby->op) || is_processed(groupby));
		sql_rel *gl = base?groupby:groupby->l;//, *ogl = gl; /* handle case of subqueries without correlation */
		obe = rel_order_by(query, &gl, orderby, 0, f);
		if (!obe)
			return NULL;
		handled_order = false;
	}

	if (all_freevar) {
		query_update_outer(query, res, all_freevar-1);
	} else if (rel) {
		*rel = res;
		if (query->last_rel != res) {
			query->last_rel = res;
			query->last_state |= sql_aggr;
		}
	}

	if (!has_args) {	/* count(*) case */
		obe = NULL; /* no errors, although the order by is useless */
		sql_exp *e;

		if (strcmp(aname, "count") != 0) {
			char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
			return sql_error(sql, 02, SQLSTATE(42000) "%s: unable to perform '%s(*)'", toUpperCopy(uaname, aname), aname);
		}
		a = sql_bind_func(sql, "sys", aname, sql_bind_localtype("void"), NULL, F_AGGR, true, true);
		e = exp_aggr(sql->sa, NULL, a, distinct, 0, groupby?groupby->card:CARD_ATOM, 0);

		if (!groupby)
			return e;
		if (all_freevar)
			query_outer_used_exp(query, all_freevar-1, e, sql_aggr);
		e = rel_groupby_add_aggr(sql, groupby, e);
		if (!group && !all_freevar)
			return e;
		if (all_freevar) {
			assert(!is_simple_project(res->op));
			e->card = CARD_ATOM;
			set_freevar(e, all_freevar-1);
			return e;
		}
		return e;
	}

	/* use cnt as nils shouldn't be counted */
	no_nil = 1;

	gr = groupby;
	if (gr && gr->op == op_project && gr->l)
		gr = gr->l;

	if (is_grouping) {
		sql_subtype *tpe;
		list *l = (list*) groupby->r;

		if (list_length(l) <= 7)
			tpe = sql_bind_localtype("bte");
		else if (list_length(l) <= 15)
			tpe = sql_bind_localtype("sht");
		else if (list_length(l) <= 31)
			tpe = sql_bind_localtype("int");
		else if (list_length(l) <= 63)
			tpe = sql_bind_localtype("lng");
#ifdef HAVE_HGE
		else if (list_length(l) <= 127)
			tpe = sql_bind_localtype("hge");
#endif
		else
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: GROUPING the number of grouping columns is larger"
								" than the maximum number of representable bits from this server (%d > %d)", list_length(l),
#ifdef HAVE_HGE
							127
#else
							 63
#endif
							);
		a = sql_bind_func_result(sql, sname, aname, F_AGGR, true, tpe, 1, exp_subtype(exps->h->data));
	} else {
		a = sql_bind_func_(sql, sname, aname, exp_types(sql->sa, exps), F_AGGR, false, false);
		if (!a && obe && list_length(obe) == 1) { /* try to find aggregation function with requires order by column */
			list *nexps = append(sa_list(sql->sa), obe->h->data);
			nexps = list_merge(nexps, exps, (fdup) NULL);
			a = sql_bind_func_(sql, sname, aname, exp_types(sql->sa, nexps), F_AGGR, false, false);
			if (a && a->func->order_required) {
				/* reset error */
				handled_order = true;
				sql->session->status = 0;
				sql->errstr[0] = '\0';
				exps = nexps;
				obe = NULL;
			}
		}
	}

	if (a) {
		found = true;
		if (!execute_priv(sql, a->func))
			a = NULL;
		if (!is_grouping && a && !(exps = check_arguments_and_find_largest_any_type(sql, rel ? *rel : NULL, exps, a, 0, false)))
			a = NULL;
	}

	if (a) {
		bool hasnil = have_nil(exps) || (strcmp(aname, "count") != 0 && (!groupby || list_empty(groupby->r))); /* for global case, the aggregate may return NULL */
		sql_exp *e = exp_aggr(sql->sa, exps, a, distinct, no_nil, groupby?groupby->card:CARD_ATOM, hasnil);

		if (!obe && a->func->order_required && !handled_order) {
			/* TODO preper error on missing order by */
			return NULL;
		}
		if (obe && !a->func->order_required && !a->func->opt_order)
			obe = NULL;
		if (obe) /* add order by expressions */
			e->r = append(sa_list(sql->sa), obe);
		if (!groupby)
			return e;
		if (all_freevar)
			query_outer_aggregated(query, all_freevar-1, e);
		e = rel_groupby_add_aggr(sql, groupby, e);
		if (!group && !all_freevar)
			return e;
		if (all_freevar) {
			rel_bind_vars(sql, groupby->l, exps);
			assert(!is_simple_project(res->op));
			e->card = CARD_ATOM;
			set_freevar(e, all_freevar-1);
			return e;
		}
		return e;
	}
	const char *type = "unknown";
	char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);

	if (!list_empty(exps)) {
		sql_exp *e = exps->h->data;
		type = exp_subtype(e)->type->base.name;
	}
	return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "%s: %s aggregate %s%s%s'%s'(%s)", toUpperCopy(uaname, aname), found ? "insufficient privileges for" : "no such",
					 sname ? "'":"", sname ? sname : "", sname ? "'.":"", aname, type);
}

static sql_exp *
rel_aggr(sql_query *query, sql_rel **rel, symbol *se, int f)
{
	dlist *l = se->data.lval;
	dnode *d = l->h->next->next;
	symbol *orderby = d->next?d->next->data.sym:NULL;
	int distinct = l->h->next->data.i_val;
	char *aname = qname_schema_object(l->h->data.lval);
	char *sname = qname_schema(l->h->data.lval);

	return _rel_aggr(query, rel, distinct, sname, aname, d->data.lval?d->data.lval->h:NULL, orderby, f);
}

static sql_exp *
rel_case(sql_query *query, sql_rel **rel, symbol *opt_cond, dlist *when_search_list, symbol *opt_else, int f)
{
	mvc *sql = query->sql;
	sql_subtype *tpe = NULL;
	list *conds = new_exp_list(sql->sa), *results = new_exp_list(sql->sa);
	sql_subtype *restype = NULL, *condtype = NULL, ctype, rtype, bt;
	sql_exp *res = NULL, *opt_cond_exp = NULL;
	exp_kind ek = {type_value, card_column, FALSE};

	if (opt_cond) {
		if (!(opt_cond_exp = rel_value_exp(query, rel, opt_cond, f, ek)))
			return NULL;
		condtype = exp_subtype(opt_cond_exp);
	}

	for (dnode *dn = when_search_list->h; dn; dn = dn->next) {
		sql_exp *cond = NULL, *result = NULL;
		dlist *when = dn->data.sym->data.lval;

		if (opt_cond)
			cond = rel_value_exp(query, rel, when->h->data.sym, f, ek);
		else
			cond = rel_logical_value_exp(query, rel, when->h->data.sym, f, ek);
		if (!cond)
			return NULL;
		append(conds, cond);
		tpe = exp_subtype(cond);
		if (tpe && condtype) {
			result_datatype(&ctype, condtype, tpe);
			condtype = &ctype;
		} else if (tpe) {
			condtype = tpe;
		}

		if (!(result = rel_value_exp(query, rel, when->h->next->data.sym, f, ek)))
			return NULL;
		append(results, result);
		tpe = exp_subtype(result);
		if (tpe && restype) {
			result_datatype(&rtype, restype, tpe);
			restype = &rtype;
		} else if (tpe) {
			restype = tpe;
		}
	}
	if (opt_else) {
		if (!(res = rel_value_exp(query, rel, opt_else, f, ek)))
			return NULL;

		tpe = exp_subtype(res);
		if (tpe && restype) {
			result_datatype(&rtype, restype, tpe);
			restype = &rtype;
		} else if (tpe) {
			restype = tpe;
		}

		if (!restype)
			return sql_error(sql, 02, SQLSTATE(42000) "Result type missing");
		if (restype->type->localtype == TYPE_void) /* NULL */
			restype = sql_bind_localtype("str");

		if (!(res = exp_check_type(sql, restype, rel ? *rel : NULL, res, type_equal)))
			return NULL;
	} else {
		if (!restype)
			return sql_error(sql, 02, SQLSTATE(42000) "Result type missing");
		if (restype->type->localtype == TYPE_void) /* NULL */
			restype = sql_bind_localtype("str");
		res = exp_null(sql->sa, restype);
	}

	if (!condtype)
		return sql_error(sql, 02, SQLSTATE(42000) "Condition type missing");
	if (condtype->type->localtype == TYPE_void) /* NULL */
		condtype = sql_bind_localtype("str");
	if (opt_cond_exp && !(opt_cond_exp = exp_check_type(sql, condtype, rel ? *rel : NULL, opt_cond_exp, type_equal)))
		return NULL;
	sql_find_subtype(&bt, "boolean", 0, 0);
	list *args = sa_list(sql->sa);
	if (opt_cond_exp)
		append(args, opt_cond_exp);
	for (node *n = conds->h, *m = results->h; n && m; n = n->next, m = m->next) {
		sql_exp *cond = n->data;
		sql_exp *result = m->data;

		if (!(result = exp_check_type(sql, restype, rel ? *rel : NULL, result, type_equal)))
			return NULL;

		if (!(cond = exp_check_type(sql, condtype, rel ? *rel : NULL, cond, type_equal)))
			return NULL;
		if (!opt_cond_exp && !(cond = exp_check_type(sql, &bt, rel ? *rel : NULL, cond, type_equal)))
			return NULL;
		append(args, cond);
		append(args, result);
	}
	assert(res);
	list_append(args, res);
	list *types = sa_list(sql->sa);
	types = append(append(types, restype), restype);
	sql_subfunc *case_func = find_func(sql, NULL, opt_cond_exp?"casewhen":"case", list_length(types), F_FUNC, true, NULL, NULL);
	res = exp_op(sql->sa, args, case_func);
	((sql_subfunc*)res->f)->res->h->data = sql_create_subtype(sql->sa, restype->type, restype->digits, restype->scale);
	return res;
}

static sql_exp *
rel_complex_case(sql_query *query, sql_rel **rel, dlist *case_args, int f, str func)
{
	exp_kind ek = {type_value, card_column, FALSE};
	list *args = sa_list(query->sql->sa);
	sql_subtype *restype = NULL, rtype;
	sql_exp *res;

	/* generate nested func calls */
	for(dnode *dn = case_args->h; dn; dn = dn->next) {
		sql_exp *a = rel_value_exp(query, rel, dn->data.sym, f, ek);
		if (!a)
			return NULL;
		append(args, a);
		/* all arguments should have the same type */
		sql_subtype *tpe = exp_subtype(a);
		if (tpe && restype) {
			result_datatype(&rtype, restype, tpe);
			restype = &rtype;
		} else if (tpe) {
			restype = tpe;
		}
	}
	if (!restype)
		return sql_error(query->sql, 02, SQLSTATE(42000) "Result type missing");
	if (restype->type->localtype == TYPE_void) /* NULL */
		restype = sql_bind_localtype("str");
	list *nargs = sa_list(query->sql->sa);
	for (node *m = args->h; m; m = m->next) {
		sql_exp *result = m->data;

		if (!(result = exp_check_type(query->sql, restype, rel ? *rel : NULL, result, type_equal)))
			return NULL;
		append(nargs, result);
	}
	list *types = append(append(sa_list(query->sql->sa), restype), restype);
	sql_subfunc *fnc = find_func(query->sql, NULL, func, list_length(types), F_FUNC, true, NULL, NULL);
	res = exp_op(query->sql->sa, nargs, fnc);
	((sql_subfunc*)res->f)->res->h->data = sql_create_subtype(query->sql->sa, restype->type, restype->digits, restype->scale);
	return res;
}

static sql_exp *
rel_case_exp(sql_query *query, sql_rel **rel, symbol *se, int f)
{
	dlist *l = se->data.lval;

	if (se->token == SQL_COALESCE) {
		return rel_complex_case(query, rel, l, f | sql_farg, "coalesce");
	} else if (se->token == SQL_NULLIF) {
		return rel_complex_case(query, rel, l, f | sql_farg, "nullif");
	} else if (l->h->type == type_list) {
		dlist *when_search_list = l->h->data.lval;
		symbol *opt_else = l->h->next->data.sym;

		return rel_case(query, rel, NULL, when_search_list, opt_else, f | sql_farg);
	} else {
		symbol *scalar_exp = l->h->data.sym;
		dlist *when_value_list = l->h->next->data.lval;
		symbol *opt_else = l->h->next->next->data.sym;

		return rel_case(query, rel, scalar_exp, when_value_list, opt_else, f | sql_farg);
	}
}

#define E_ATOM_STRING(e) ((atom*)(e)->l)->data.val.sval

static sql_exp *
rel_cast(sql_query *query, sql_rel **rel, symbol *se, int f)
{
	mvc *sql = query->sql;
	dlist *dl = se->data.lval;
	symbol *s = dl->h->data.sym;
	sql_subtype *tpe = &dl->h->next->data.typeval;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_exp *e = rel_value_exp(query, rel, s, f|sql_farg, ek);

	if (!e)
		return NULL;

	/* strings may need to be truncated */
	if (EC_VARCHAR(tpe->type->eclass) && tpe->digits > 0) {
		sql_subtype *et = exp_subtype(e);
		/* truncate only if the number of digits are smaller or from clob */
		if (et && EC_VARCHAR(et->type->eclass) && (tpe->digits < et->digits || et->digits == 0)) {
			sql_subfunc *c = sql_bind_func(sql, "sys", "truncate", et, sql_bind_localtype("int"), F_FUNC, true, true);
			if (c)
				e = exp_binop(sql->sa, e, exp_atom_int(sql->sa, tpe->digits), c);
		}
	}

	if (tpe->type->eclass == EC_DEC) {
		sql_subtype *et = exp_subtype(e);
		if (e->type == e_atom && !tpe->digits) {
			if (et->type->eclass == EC_NUM || et->type->eclass == EC_DEC) {
				tpe->digits = atom_num_digits(e->l);
				tpe = sql_bind_subtype(sql->sa, "decimal", tpe->digits, et->scale);
			} else if (EC_VARCHAR(et->type->eclass)) {
				char *s = E_ATOM_STRING(e);
				unsigned int min_precision = 0, min_scale = 0;
				bool dot_seen = false;
				for (size_t i = 0; i < strlen(s); i++) {
					if (isdigit(s[i])) {
						min_precision++;
						if (dot_seen)
							min_scale++;
					} else if (s[i] == '.') {
						dot_seen = true;
					}
				}
				tpe = sql_bind_subtype(sql->sa, "decimal", min_precision, min_scale);
			} else { /* fallback */
				tpe = sql_bind_subtype(sql->sa, "decimal", 18, 3);
			}
		} else if (!tpe->digits && !tpe->scale) {
			if (et->type->eclass == EC_NUM)
				tpe = sql_bind_subtype(sql->sa, "decimal", et->digits, 0);
			else /* fallback */
				tpe = sql_bind_subtype(sql->sa, "decimal", 18, 3);
		}
	}

	if (e)
		e = exp_check_type(sql, tpe, rel ? *rel : NULL, e, type_cast);

	if (e && e->type == e_convert)
		exp_label(sql->sa, e, ++sql->label);

	return e;
}

static sql_exp *
rel_next_value_for( mvc *sql, symbol *se )
{
	char *sname = qname_schema(se->data.lval);
	char *seqname = qname_schema_object(se->data.lval);
	sql_sequence *seq = NULL;
	sql_subtype t;
	sql_subfunc *f;

	if (!sname)
		sname = "sys";
	if (!stack_find_rel_view(sql, seqname)) {
		if (!(seq = find_sequence_on_scope(sql, sname, seqname, "NEXT VALUE FOR")))
			return NULL;
		if (!mvc_schema_privs(sql, seq->s))
			return sql_error(sql, 02, SQLSTATE(42000) "NEXT VALUE FOR: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), seq->s->base.name);
	}
	sql_find_subtype(&t, "varchar", 0, 0);
	f = sql_bind_func(sql, "sys", "next_value_for", &t, &t, F_FUNC, true, true);
	assert(f);
	/* sequence found in the stack, ie just created. use given schema? */
	return exp_binop(sql->sa, exp_atom_str(sql->sa, seq && seq->s ? seq->s->base.name : sname, &t), exp_atom_str(sql->sa, seqname, &t), f);
}

/* some users like to use aliases already in the groupby */
static sql_exp *
rel_selection_ref(sql_query *query, sql_rel **rel, char *name, dlist *selection)
{
	allocator *sa = query->sql->sa;
	dlist *nl;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_exp *res = NULL;
	symbol *nsym;

	if (!selection)
		return NULL;

	for (dnode *n = selection->h; n; n = n->next) {
		/* we only look for columns */
		tokens to = n->data.sym->token;
		if (to == SQL_COLUMN || to == SQL_IDENT) {
			dlist *l = n->data.sym->data.lval;
			/* AS name */
			if (l->h->next->data.sval && strcmp(l->h->next->data.sval, name) == 0) {
				sql_exp *ve = rel_value_exp(query, rel, l->h->data.sym, sql_sel|sql_groupby, ek);
				if (ve) {
					if (res)
						return sql_error(query->sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s' ambiguous", name);
					res = ve;

					nl = dlist_create(sa);
					exp_setname(query->sql, ve, NULL, name);
					/* now we should rewrite the selection such that it uses the new group by column */
					dlist_append_string(sa, nl, sa_strdup(sa, name));
					nsym = symbol_create_list(sa, to, nl);
					nl = dlist_create(sa);
					dlist_append_symbol(sa, nl, nsym);
					/* no alias */
					dlist_append_symbol(sa, nl, NULL);
					n->data.sym = symbol_create_list(sa, to, nl);
				}
			}
		}
	}
	return res;
}

static char*
symbol_get_identifier(symbol *sym)
{
	dlist *syml;

	if (sym->token != SQL_COLUMN && sym->token != SQL_IDENT)
		return NULL;
	syml = sym->data.lval;
	if (dlist_length(syml) > 1)
		return NULL;

	return syml->h->data.sval;
}

static sql_exp*
rel_group_column(sql_query *query, sql_rel **rel, symbol *grp, dlist *selection, list *exps, int f)
{
	sql_query *lquery = query_create(query->sql);
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_value, TRUE};
	sql_exp *e = rel_value_exp2(lquery, rel, grp, f, ek);

	if (e && exp_is_atom(e)) {
		sql_subtype *tpe = exp_subtype(e);
		if (!is_atom(e->type) ||!tpe || tpe->type->eclass != EC_NUM) {
			if (!tpe)
				return sql_error(sql, 02, SQLSTATE(42000) "Cannot have a parameter (?) for group by column");
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: non-integer constant in GROUP BY");
		}
	}

	if (!e) {
		char buf[ERRSIZE], *name;
		int status = sql->session->status;
		strcpy(buf, sql->errstr);
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';

		if ((name = symbol_get_identifier(grp))) {
			e = rel_selection_ref(query, rel, name, selection);
			if (!e) { /* attempt to find in the existing list of group by expressions */
				for (node *n = exps->h ; n && !e ; n = n->next) {
					sql_exp *ge = (sql_exp *) n->data;
					const char *gen = exp_name(ge);

					if (gen && strcmp(name, gen) == 0)
						e = exp_ref(sql, ge);
				}
			}
		}
		if (!e && query_has_outer(query)) {
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';
			e = rel_value_exp2(query, rel, grp, f, ek);
		}
		if (!e) {
			if (sql->errstr[0] == 0) {
				sql->session->status = status;
				strcpy(sql->errstr, buf);
			}
			return NULL;
		}
	}
	if (!exp_subtype(e))
		return sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) for group by column");
	return e;
}

static list*
list_power_set(allocator *sa, list* input) /* cube */
{
	list *res = sa_list(sa);
	/* N stores total number of subsets */
	int N = (int) pow(2, input->cnt);

	/* generate each subset one by one */
	for (int i = 0; i < N; i++) {
		list *ll = sa_list(sa);
		int j = 0; /* check every bit of i */
		for (node *n = input->h ; n ; n = n->next) {
			/* if j'th bit of i is set, then append */
			if (i & (1 << j))
				list_prepend(ll, n->data);
			j++;
		}
		list_prepend(res, ll);
	}
	return res;
}

static list*
list_rollup(allocator *sa, list* input)
{
	list *res = sa_list(sa);

	for (int counter = input->cnt; counter > 0; counter--) {
		list *ll = sa_list(sa);
		int j = 0;
		for (node *n = input->h; n && j < counter; j++, n = n->next)
			list_append(ll, n->data);
		list_append(res, ll);
	}
	list_append(res, sa_list(sa)); /* global aggregate case */
	return res;
}

static int
list_equal(list* list1, list* list2)
{
	for (node *n = list1->h; n ; n = n->next) {
		sql_exp *e = (sql_exp*) n->data;
		if (!exps_find_exp(list2, e))
			return 1;
	}
	for (node *n = list2->h; n ; n = n->next) {
		sql_exp *e = (sql_exp*) n->data;
		if (!exps_find_exp(list1, e))
			return 1;
	}
	return 0;
}

static list*
lists_cartesian_product_and_distinct(allocator *sa, list *l1, list *l2)
{
	list *res = sa_list(sa);

	/* for each list of l2, merge into each list of l1 while removing duplicates */
	for (node *n = l1->h ; n ; n = n->next) {
		list *sub_list = (list*) n->data;

		for (node *m = l2->h ; m ; m = m->next) {
			list *other = (list*) m->data;
			list_append(res, list_distinct(list_merge(list_dup(sub_list, (fdup) NULL), other, (fdup) NULL), (fcmp) list_equal, (fdup) NULL));
		}
	}
	return res;
}

static list*
rel_groupings(sql_query *query, sql_rel **rel, symbol *groupby, dlist *selection, int f, bool grouping_sets, list **sets)
{
	mvc *sql = query->sql;
	list *exps = new_exp_list(sql->sa);

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	for (dnode *o = groupby->data.lval->h; o; o = o->next) {
		symbol *grouping = o->data.sym;
		list *next_set = NULL;

		if (grouping->token == SQL_GROUPING_SETS) { /* call recursively, and merge the generated sets */
			list *other = rel_groupings(query, rel, grouping, selection, f, true, &next_set);
			if (!other)
				return NULL;
			exps = list_distinct(list_merge(exps, other, (fdup) NULL), (fcmp) exp_equal, (fdup) NULL);
		} else {
			dlist *dl = grouping->data.lval;
			if (dl) {
				list *set_cols = new_exp_list(sql->sa); /* columns and combination of columns to be used for the next set */

				for (dnode *oo = dl->h; oo; oo = oo->next) {
					symbol *grp = oo->data.sym;
					list *next_tuple = new_exp_list(sql->sa); /* next tuple of columns */

					if (grp->token == SQL_COLUMN_GROUP) { /* set of columns */
						assert(is_sql_group_totals(f));
						for (dnode *ooo = grp->data.lval->h; ooo; ooo = ooo->next) {
							symbol *elm = ooo->data.sym;
							sql_exp *e = rel_group_column(query, rel, elm, selection, exps, f);
							if (!e)
								return NULL;
							assert(e->type == e_column);
							list_append(next_tuple, e);
							list_append(exps, e);
						}
					} else { /* single column or expression */
						sql_exp *e = rel_group_column(query, rel, grp, selection, exps, f);
						if (!e)
							return NULL;
						if (e->type != e_column && !exp_is_atom(e)) { /* store group by expressions in the stack */
							if (is_sql_group_totals(f))
								return sql_error(sql, 02, SQLSTATE(42000) "GROUP BY: grouping expressions not possible with ROLLUP, CUBE and GROUPING SETS");
							if (!exp_has_rel(e) && !frame_push_groupby_expression(sql, grp, e))
								return NULL;
						}
						list_append(next_tuple, e);
						list_append(exps, e);
					}
					list_append(set_cols, next_tuple);
				}
				if (is_sql_group_totals(f)) {
					if (grouping->token == SQL_ROLLUP)
						next_set = list_rollup(sql->sa, set_cols);
					else if (grouping->token == SQL_CUBE)
						next_set = list_power_set(sql->sa, set_cols);
					else /* the list of sets is not used in the "GROUP BY a, b, ..." case */
						next_set = list_append(new_exp_list(sql->sa), set_cols);
				}
			} else if (is_sql_group_totals(f) && grouping_sets) /* The GROUP BY () case is the global aggregate which is always added by ROLLUP and CUBE */
				next_set = list_append(new_exp_list(sql->sa), new_exp_list(sql->sa));
		}
		if (is_sql_group_totals(f)) { /* if there are no sets, set the found one, otherwise calculate cartesian product and merge the distinct ones */
			if (!next_set)
				return sql_error(sql, 02, SQLSTATE(42000) "GROUP BY: GROUPING SETS is empty");
			if (!*sets)
				*sets = next_set;
			else
				*sets = grouping_sets ? list_merge(*sets, next_set, (fdup) NULL) : lists_cartesian_product_and_distinct(sql->sa, *sets, next_set);
		}
	}
	return exps;
}

static list*
rel_partition_groupings(sql_query *query, sql_rel **rel, symbol *partitionby, dlist *selection, int f)
{
	mvc *sql = query->sql;
	dnode *o = partitionby->data.lval->h;
	list *exps = new_exp_list(sql->sa);

	for (; o; o = o->next) {
		symbol *grp = o->data.sym;
		exp_kind ek = {type_value, card_value, TRUE};
		sql_exp *e = rel_value_exp2(query, rel, grp, f, ek);

		if (!e) {
			int status = sql->session->status;
			char buf[ERRSIZE], *name;

			/* reset error */
			sql->session->status = 0;
			strcpy(buf, sql->errstr);
			sql->errstr[0] = '\0';

			if ((name = symbol_get_identifier(grp))) {
				e = rel_selection_ref(query, rel, name, selection);
				if (!e) { /* attempt to find in the existing list of partition by expressions */
					for (node *n = exps->h ; n ; n = n->next) {
						sql_exp *ge = (sql_exp *) n->data;
						const char *gen = exp_name(ge);

						if (gen && strcmp(name, gen) == 0) {
							e = exp_ref(sql, ge);
							break;
						}
					}
				}
			}
			if (!e) {
				if (sql->errstr[0] == 0) {
					sql->session->status = status;
					strcpy(sql->errstr, buf);
				}
				return NULL;
			}
		}

		if (exp_has_rel(e))
			return sql_error(sql, 02, SQLSTATE(42000) "PARTITION BY: subqueries not allowed in PARTITION BY clause");

		if (e->type != e_column) { /* store group by expressions in the stack */
			if (!frame_push_groupby_expression(sql, grp, e))
				return NULL;
		}

		if (e->card > CARD_AGGR)
			e->card = CARD_AGGR;
		append(exps, e);
	}
	return exps;
}

/* find selection expressions matching the order by column expression */
/* complex columns only */
static sql_exp *
rel_order_by_column_exp(sql_query *query, sql_rel **R, symbol *column_r, int needs_distinct, int f)
{
	mvc *sql = query->sql;
	sql_rel *r = *R, *p = NULL;
	sql_exp *e = NULL, *found = NULL;
	exp_kind ek = {type_value, card_column, FALSE};

	if (!r)
		return e;

	if (is_simple_project(r->op) && r->l && is_processed(r)) {
		p = r;
		r = r->l;
	}

	e = rel_value_exp(query, &r, column_r, f, ek);

	if (r && !p)
		*R = r;
	else if (r)
		p->l = r;
	if (e && p) {
		if (is_project(p->op) && (found = exps_any_match(p->exps, e))) { /* if one of the projections matches, return a reference to it */
			e = exp_ref(sql, found);
		} else {
			if (needs_distinct)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: with DISTINCT ORDER BY expressions must appear in select list");
			e = rel_project_add_exp(sql, p, e);
			if (r) {
				for (node *n = p->exps->h ; n ; n = n->next) {
					sql_exp *ee = n->data;

					if (ee->card > r->card) {
						if (exp_name(ee) && !has_label(ee))
							return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(ee));
						return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
					}
				}
			}
		}
		return e;
	}
	if (e && r && is_project(r->op)) {
		sql_exp *found = exps_find_exp(r->exps, e);

		if (!found) {
			if (needs_distinct)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: with DISTINCT ORDER BY expressions must appear in select list");
			if (!is_simple_project(r->op) && !is_groupby(r->op))
				*R = r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 0));
			append(r->exps, e);
		} else {
			e = found;
		}
		e = exp_ref(sql, e);
	}
	return e;
}

static dlist *
simple_selection(symbol *sq)
{
	if (sq->token == SQL_SELECT) {
		SelectNode *sn;
		sn = (SelectNode *) sq;

		if (!sn->from && !sn->where && !sn->distinct && !sn->window && dlist_length(sn->selection) == 1)
			return sn->selection;
	}
	return NULL;
}

static list *
rel_order_by(sql_query *query, sql_rel **R, symbol *orderby, int needs_distinct, int f)
{
	mvc *sql = query->sql;
	sql_rel *rel = *R, *or = rel; /* the order by relation */
	list *exps = new_exp_list(sql->sa);

	if (!orderby->data.lval) { /* by all */
		if (is_sql_orderby(f)) {
			assert(is_project(rel->op));
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				append(exps, exp_ref(sql, e));
			}
			return exps;
		}
		return NULL;
	}

	dnode *o = orderby->data.lval->h;
	dlist *selection = NULL;

	if (is_sql_orderby(f)) {
		assert(is_project(rel->op));
		rel = rel->l;
	}

	for (; o; o = o->next) {
		symbol *order = o->data.sym;

		if (order->token == SQL_COLUMN || order->token == SQL_IDENT) {
			symbol *col = order->data.lval->h->data.sym;
			int direction = order->data.lval->h->next->data.i_val;
			sql_exp *e = NULL;

			assert(order->data.lval->h->next->type == type_int);
			if ((selection = simple_selection(col)) != NULL) {
				dnode *o = selection->h;
				order = o->data.sym;
				if (order->data.lval->h->type == type_symbol)
					col = order->data.lval->h->data.sym;
				/* remove optional name from selection */
				order->data.lval->h->next = NULL;
			}

			if (col->token == SQL_COLUMN || col->token == SQL_IDENT || col->token == SQL_ATOM) {
				exp_kind ek = {type_value, card_column, FALSE};

				e = rel_value_exp2(query, &rel, col, f, ek);

				if (e && e->card <= CARD_ATOM) {
					sql_subtype *tpe = exp_subtype(e);
					/* integer atom on the stack */
					if (!is_sql_window(f) && e->type == e_atom &&
					    tpe->type->eclass == EC_NUM) {
						atom *a = e->l;
						int nr = (int)atom_get_int(a);

						e = exps_get_exp(rel->exps, nr);
						if (!e)
							return sql_error(sql, 02, SQLSTATE(42000) "SELECT: the order by column number (%d) is not in the number of projections range (%d)", nr, list_length(rel->exps));
						e = exp_ref(sql, e);
					}
				} else if (e && exp_card(e) > rel->card) {
					if (exp_name(e) && !has_label(e))
						return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(e));
					return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
				}
				if (e && !exp_name(e))
					exp_label(sql->sa, e, ++sql->label);
				if (e && rel && is_project(rel->op)) {
					sql_exp *found = exps_find_exp(rel->exps, e);

					if (!found) {
						if (needs_distinct)
							return sql_error(sql, 02, SQLSTATE(42000) "SELECT: with DISTINCT ORDER BY expressions must appear in select list");
						if (!is_freevar(e))
							append(rel->exps, e);
					} else {
						e = found;
					}
					if (!is_freevar(e))
						e = exp_ref(sql, e);
				}
			}

			if (rel && !e && sql->session->status != -ERR_AMBIGUOUS) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';

				e = rel_order_by_column_exp(query, &rel, col, needs_distinct, sql_sel | sql_orderby | (f & sql_group_totals) | (f & sql_window));
			}
			if (!e)
				return NULL;
			if (!exp_subtype(e))
				return sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) for order by column");
			set_direction(e, direction);
			list_append(exps, e);
		} else {
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: order not of type SQL_COLUMN");
		}
	}
	if (is_sql_orderby(f) && or != rel)
		or->l = rel;
	if (is_sql_window(f))
		*R = rel;
	return exps;
}

static int
generate_window_bound(tokens sql_token, bool first_half)
{
	switch (sql_token) {
		case SQL_PRECEDING:
			return first_half ? BOUND_FIRST_HALF_PRECEDING : BOUND_SECOND_HALF_PRECEDING;
		case SQL_FOLLOWING:
			return first_half ? BOUND_FIRST_HALF_FOLLOWING : BOUND_SECOND_HALF_FOLLOWING;
		case SQL_CURRENT_ROW:
			return first_half ? CURRENT_ROW_PRECEDING : CURRENT_ROW_FOLLOWING;
		default:
			assert(0);
	}
	return 0;
}

/* window functions */
static sql_exp*
generate_window_bound_call(mvc *sql, sql_exp **estart, sql_exp **eend, sql_exp *pe, sql_exp *e,
						   sql_exp *start, sql_exp *fend, int frame_type, int excl, tokens t1, tokens t2)
{
	list *rargs1 = sa_list(sql->sa), *rargs2 = sa_list(sql->sa), *targs1 = sa_list(sql->sa), *targs2 = sa_list(sql->sa);
	sql_subfunc *dc1, *dc2;
	sql_subtype *it = sql_bind_localtype("int");

	if (pe) {
		append(targs1, exp_subtype(pe));
		append(targs2, exp_subtype(pe));
		append(rargs1, exp_copy(sql, pe));
		append(rargs2, exp_copy(sql, pe));
	}
	append(rargs1, exp_copy(sql, e));
	append(rargs2, exp_copy(sql, e));
	append(targs1, exp_subtype(e));
	append(targs2, exp_subtype(e));
	append(targs1, it);
	append(targs2, it);
	append(targs1, it);
	append(targs2, it);
	append(targs1, it);
	append(targs2, it);
	append(targs1, exp_subtype(start));
	append(targs2, exp_subtype(fend));

	dc1 = sql_bind_func_(sql, "sys", "window_bound", targs1, F_ANALYTIC, true, false);
	dc2 = sql_bind_func_(sql, "sys", "window_bound", targs2, F_ANALYTIC, true, false);
	if (!dc1 || !dc2)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: function 'window_bound' not found");
	append(rargs1, exp_atom_int(sql->sa, frame_type));
	append(rargs2, exp_atom_int(sql->sa, frame_type));
	append(rargs1, exp_atom_int(sql->sa, generate_window_bound(t1, true)));
	append(rargs2, exp_atom_int(sql->sa, generate_window_bound(t2, false)));
	append(rargs1, exp_atom_int(sql->sa, excl));
	append(rargs2, exp_atom_int(sql->sa, excl));
	append(rargs1, start);
	append(rargs2, fend);

	*estart = exp_op(sql->sa, rargs1, dc1);
	*eend = exp_op(sql->sa, rargs2, dc2);
	return e; /* return something to say there were no errors */
}

#define EC_NUMERIC(e) (e==EC_NUM||EC_INTERVAL(e)||e==EC_DEC||e==EC_FLT)

static sql_exp*
calculate_window_bound(sql_query *query, sql_rel *p, tokens token, symbol *bound, sql_exp *ie, int frame_type, int f)
{
	mvc *sql = query->sql;
	sql_subtype *bt, *bound_tp = sql_bind_localtype("lng"), *iet = exp_subtype(ie);
	sql_exp *res = NULL;

	if ((bound->token == SQL_PRECEDING || bound->token == SQL_FOLLOWING || bound->token == SQL_CURRENT_ROW) && bound->type == type_int) {
		atom *a = NULL;
		bt = (frame_type == FRAME_ROWS || frame_type == FRAME_GROUPS) ? bound_tp : iet;

		if ((bound->data.i_val == UNBOUNDED_PRECEDING_BOUND || bound->data.i_val == UNBOUNDED_FOLLOWING_BOUND)) {
			a = atom_max_value(sql->sa, EC_NUMERIC(bt->type->eclass) ? bt : bound_tp);
		} else if (bound->data.i_val == CURRENT_ROW_BOUND) {
			a = atom_zero_value(sql->sa, EC_NUMERIC(bt->type->eclass) ? bt : bound_tp);
		} else {
			assert(0);
		}
		res = exp_atom(sql->sa, a);
	} else { /* arbitrary expression case */
		exp_kind ek = {type_value, card_column, FALSE};
		const char *bound_desc = (token == SQL_PRECEDING) ? "PRECEDING" : "FOLLOWING";

		assert(token == SQL_PRECEDING || token == SQL_FOLLOWING);
		if (!(res = rel_value_exp2(query, &p, bound, f, ek)))
			return NULL;
		if (!(bt = exp_subtype(res))) { /* frame bound is a parameter */
			sql_subtype *t = (frame_type == FRAME_ROWS || frame_type == FRAME_GROUPS) ? bound_tp : iet;
			if (rel_set_type_param(sql, t, p, res, 0) < 0) /* workaround */
				return NULL;
			bt = exp_subtype(res);
		}
		if (exp_is_null_no_value_opt(res))
			return sql_error(sql, 02, SQLSTATE(42000) "%s offset must not be NULL", bound_desc);
		if ((frame_type == FRAME_ROWS || frame_type == FRAME_GROUPS) && bt->type->eclass != EC_NUM && !(res = exp_check_type(sql, bound_tp, p, res, type_equal)))
			return NULL;
		if (frame_type == FRAME_RANGE) {
			sql_class iet_class = iet->type->eclass;

			if (!EC_NUMERIC(iet_class) && !EC_TEMP(iet_class))
				return sql_error(sql, 02, SQLSTATE(42000) "Ranges with arbitrary expressions are available to numeric, interval and temporal types only");
			if (EC_NUMERIC(iet_class) && !(res = exp_check_type(sql, iet, p, res, type_equal)))
				return NULL;
			if ((iet_class == EC_TIME || iet_class == EC_TIME_TZ) && bt->type->eclass != EC_SEC) {
				(void) sql_error(sql, 02, SQLSTATE(42000) "For %s input the %s boundary must be an interval type up to the day", subtype2string2(sql->ta, iet), bound_desc);
				sa_reset(sql->ta);
				return NULL;
			}
			if (EC_TEMP(iet->type->eclass) && !EC_INTERVAL(bt->type->eclass)) {
				(void) sql_error(sql, 02, SQLSTATE(42000) "For %s input the %s boundary must be an interval type", subtype2string2(sql->ta, iet), bound_desc);
				sa_reset(sql->ta);
				return NULL;
			}
		}
	}
	return res;
}

static dlist*
get_window_clauses(mvc *sql, char* ident, symbol **partition_by_clause, symbol **order_by_clause, symbol **frame_clause)
{
	dlist *window_specification = NULL;
	char *window_ident;
	int pos;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if ((window_specification = frame_get_window_def(sql, ident, &pos)) == NULL)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: window '%s' not found", ident);

	/* avoid infinite lookups */
	if (frame_check_var_visited(sql, pos))
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: cyclic references to window '%s' found", ident);
	frame_set_var_visited(sql, pos);

	if (window_specification->h->next->data.sym) {
		if (*partition_by_clause)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: redefinition of PARTITION BY clause from window '%s'", ident);
		*partition_by_clause = window_specification->h->next->data.sym;
	}
	if (window_specification->h->next->next->data.sym) {
		if (*order_by_clause)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: redefinition of ORDER BY clause from window '%s'", ident);
		*order_by_clause = window_specification->h->next->next->data.sym;
	}
	if (window_specification->h->next->next->next->data.sym) {
		if (*frame_clause)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: redefinition of frame clause from window '%s'", ident);
		*frame_clause = window_specification->h->next->next->next->data.sym;
	}

	window_ident = window_specification->h->data.sval;
	if (window_ident && !get_window_clauses(sql, window_ident, partition_by_clause, order_by_clause, frame_clause))
		return NULL; /* the error was already set */

	return window_specification; /* return something to say there were no errors */
}

/*
 * select x, y, rank_op() over (partition by x order by y) as, ...
                aggr_op(z) over (partition by y order by x) as, ...
 * from table [x,y,z,w,v]
 *
 * project and order by over x,y / y,x
 * a = project( table ) [ x, y, z, w, v ], [ x, y]
 * b = project( table ) [ x, y, z, w, v ], [ y, x]
 *
 * project with order dependent operators, ie combined prev/current value
 * aa = project (a) [ x, y, r = rank_op(diff(x) (marks a new partition), rediff(diff(x), y) (marks diff value with in partition)), z, w, v ]
 * project(aa) [ aa.x, aa.y, aa.r ] -- only keep current output list
 * bb = project (b) [ x, y, a = aggr_op(z, diff(y), rediff(diff(y), x)), z, w, v ]
 * project(bb) [ bb.x, bb.y, bb.a ]  -- only keep current output list
 */
static sql_exp *
rel_rankop(sql_query *query, sql_rel **rel, symbol *se, int f)
{
	mvc *sql = query->sql;
	node *n;
	dlist *l = se->data.lval, *window_specification = NULL;
	symbol *window_function = l->h->data.sym, *partition_by_clause = NULL, *order_by_clause = NULL, *frame_clause = NULL;
	char *aname = NULL, *sname = NULL, *window_ident = NULL;
	sql_subfunc *wf = NULL;
	sql_exp *in = NULL, *pe = NULL, *oe = NULL, *call = NULL, *start = NULL, *eend = NULL, *fstart = NULL, *fend = NULL, *ie = NULL;
	sql_rel *p;
	list *gbe = NULL, *obe = NULL, *args = NULL, *types = NULL, *fargs = NULL;
	dnode *dn = window_function->data.lval->h, *dargs = NULL;
	int distinct = 0, frame_type, pos, nf = f, nfargs = 0;
	bool is_nth_value, supports_frames = false, found = false;

	frame_clear_visited_flag(sql); /* clear visited flags before iterating */

	if (l->h->next->type == type_list) {
		window_specification = l->h->next->data.lval;
	} else if (l->h->next->type == type_string) {
		const char* window_alias = l->h->next->data.sval;
		if ((window_specification = frame_get_window_def(sql, window_alias, &pos)) == NULL)
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: window '%s' not found", window_alias);
		frame_set_var_visited(sql, pos);
	} else {
		assert(0);
	}

	window_ident = window_specification->h->data.sval;
	partition_by_clause = window_specification->h->next->data.sym;
	order_by_clause = window_specification->h->next->next->data.sym;
	frame_clause = window_specification->h->next->next->next->data.sym;

	if (window_ident && !get_window_clauses(sql, window_ident, &partition_by_clause, &order_by_clause, &frame_clause))
		return NULL;

	frame_type = frame_clause ? frame_clause->data.lval->h->next->next->data.i_val : FRAME_RANGE;
	aname = qname_schema_object(dn->data.lval);
	sname = qname_schema(dn->data.lval);

	is_nth_value = !strcmp(aname, "nth_value");
	bool is_value = is_nth_value || !strcmp(aname, "first_value") || !strcmp(aname, "last_value");
	bool rank = false;
	if (strcmp(aname, "row_number") == 0 || strcmp(aname, "rank") == 0 || strcmp(aname, "dense_rank") == 0 ||
	    strcmp(aname, "percent_rank") == 0 || strcmp(aname, "cume_dist") == 0 || strcmp(aname, "ntile") == 0 ||
	    strcmp(aname, "lag") == 0 || strcmp(aname, "lead") == 0 || strcmp(aname, "fetch") == 0)
		rank = true;
	supports_frames = (!rank || is_value);

	if (is_sql_update_set(f) || is_sql_psm(f) || is_sql_values(f) || is_sql_join(f) || is_sql_where(f) || is_sql_groupby(f) || is_sql_having(f) || is_psm_call(f) || is_sql_from(f)) {
		char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
		const char *clause = is_sql_update_set(f)||is_sql_psm(f)?"in SET, WHILE, IF, ELSE, CASE, WHEN, RETURN, ANALYZE clauses (use subquery)":is_sql_values(f)?"on an unique value":
							 is_sql_join(f)?"in JOIN conditions":is_sql_where(f)?"in WHERE clause":is_sql_groupby(f)?"in GROUP BY clause":
							 is_psm_call(f)?"in CALL":is_sql_from(f)?"in functions in FROM":"in HAVING clause";
		return sql_error(sql, 02, SQLSTATE(42000) "%s: window function '%s' not allowed %s", toUpperCopy(uaname, aname), aname, clause);
	} else if (is_sql_aggr(f)) {
		char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
		return sql_error(sql, 02, SQLSTATE(42000) "%s: window functions not allowed inside aggregation functions", toUpperCopy(uaname, aname));
	} else if (is_sql_window(f)) {
		char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
		return sql_error(sql, 02, SQLSTATE(42000) "%s: window functions cannot be nested", toUpperCopy(uaname, aname));
	}
	if (window_function->token == SQL_AGGR) {
		dn->next->next->type = type_list;
		dn->next->next->data.lval = dlist_append_symbol(sql->sa, dlist_create( sql->sa ), dn->next->next->data.sym); /* make a list */
	}
	if (window_function->token == SQL_NOP)
		window_function->token = SQL_AGGR;
	if (window_function->token != SQL_RANK && window_function->token != SQL_AGGR) {
		char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: window function '%s' unknown", toUpperCopy(uaname, aname));
	}

	/* window operations are only allowed in the projection */
	if (!is_sql_sel(f))
		return sql_error(sql, 02, SQLSTATE(42000) "OVER: only possible within the selection");

	p = *rel;
	/* Partition By */
	if (partition_by_clause) {
		gbe = rel_partition_groupings(query, &p, partition_by_clause, NULL /* cannot use (selection) column references, as this result is a selection column */, nf | sql_window);
		if (!gbe)
			return NULL;
		for (n = gbe->h ; n ; n = n->next) {
			sql_exp *en = n->data;

			set_ascending(en);
			set_nulls_first(en);
		}
	}

	/* Order By */
	if (order_by_clause) {
		obe = rel_order_by(query, &p, order_by_clause, 0, sql_window);
		if (!obe)
			return NULL;
	}

	fargs = sa_list(sql->sa);
	if (rank) { /* rank function call */
		dlist *dl = dn->next->next->data.lval;
		bool is_lag = !strcmp(aname, "lag"), is_lead = !strcmp(aname, "lead"),
			 extra_input = !strcmp(aname, "ntile") || !strcmp(aname, "rank") || !strcmp(aname, "dense_rank") || !strcmp(aname, "row_number") || !strcmp(aname, "percent_rank") || !strcmp(aname, "cume_dist");

		distinct = dn->next->data.i_val;
		if (extra_input) { /* pass an input column for analytic functions that don't require it */
			sql_subfunc *star = sql_bind_func(sql, "sys", "star", NULL, NULL, F_FUNC, true, true);
			in = exp_op(sql->sa, NULL, star);
			append(fargs, in);
		}
		if (dl)
			for (dargs = dl->h ; dargs ; dargs = dargs->next) {
				exp_kind ek = {type_value, card_column, FALSE};
				sql_subtype *empty = sql_bind_localtype("void"), *bte = sql_bind_localtype("bte");

				in = rel_value_exp2(query, &p, dargs->data.sym, f | sql_window | sql_farg, ek);
				if (!in)
					return NULL;
				if (!exp_subtype(in)) { /* we also do not expect parameters here */
					char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
					return sql_error(sql, 02, SQLSTATE(42000) "%s: parameters not allowed as arguments to window functions", toUpperCopy(uaname, aname));
				}
				if (!exp_name(in))
					exp_label(sql->sa, in, ++sql->label);

				/* corner case, if the argument is null convert it into something countable such as bte */
				if (subtype_cmp(exp_subtype(in), empty) == 0)
					in = exp_convert(sql, in, empty, bte);
				if ((is_lag || is_lead) && nfargs == 2) { /* lag and lead 3rd arg must have same type as 1st arg */
					sql_exp *first = (sql_exp*) fargs->h->data;
					if (!(in = exp_check_type(sql, exp_subtype(first), p, in, type_equal)))
						return NULL;
				}
				if (!in)
					return NULL;

				append(fargs, in);
				in = exp_ref_save(sql, in);
				nfargs++;
			}
	} else { /* aggregation function call */
		distinct = dn->next->data.i_val;
		assert(dn->next->next->type == type_list);
		dlist *dl = dn->next->next->data.lval;
		for (dargs = dl?dl->h:NULL; dargs && dargs->data.sym ; dargs = dargs->next) {
			exp_kind ek = {type_value, card_column, FALSE};
			sql_subtype *empty = sql_bind_localtype("void"), *bte = sql_bind_localtype("bte");

			in = rel_value_exp2(query, &p, dargs->data.sym, f | sql_window | sql_farg, ek);
			if (!in)
				return NULL;
			if (!exp_subtype(in)) { /* we also do not expect parameters here */
				char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
				return sql_error(sql, 02, SQLSTATE(42000) "%s: parameters not allowed as arguments to window functions", toUpperCopy(uaname, aname));
			}
			if (!exp_name(in))
				exp_label(sql->sa, in, ++sql->label);

			/* corner case, if the argument is null convert it into something countable such as bte */
			if (subtype_cmp(exp_subtype(in), empty) == 0)
				in = exp_convert(sql, in, empty, bte);
			if (!in)
				return NULL;

			append(fargs, in);
			in = exp_ref_save(sql, in);
			nfargs++;

			if (!strcmp(aname, "count"))
				append(fargs, exp_atom_bool(sql->sa, 1)); /* ignore nills */
		}

		if (!nfargs) { /* count(*) */
			if (window_function->token == SQL_AGGR && strcmp(aname, "count") != 0) {
				char *uaname = SA_NEW_ARRAY(sql->ta, char, strlen(aname) + 1);
				return sql_error(sql, 02, SQLSTATE(42000) "%s: unable to perform '%s(*)'", toUpperCopy(uaname, aname), aname);
			}
			sql_subfunc *star = sql_bind_func(sql, "sys", "star", NULL, NULL, F_FUNC, true, true);
			in = exp_op(sql->sa, NULL, star);
			append(fargs, in);
			append(fargs, exp_atom_bool(sql->sa, 0)); /* don't ignore nills */
		}
	}

	if (distinct)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: DISTINCT clause is not implemented for window functions");

	/* diff for partitions */
	if (gbe) {
		sql_subtype *bt = sql_bind_localtype("bit");

		for( n = gbe->h; n; n = n->next) {
			sql_subfunc *df;
			sql_exp *e = n->data;

			if (!exp_subtype(e))
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: parameters not allowed at PARTITION BY clause from window functions");

			e = exp_copy(sql, e);
			args = sa_list(sql->sa);
			if (pe) {
				df = sql_bind_func(sql, "sys", "diff", bt, exp_subtype(e), F_ANALYTIC, true, true);
				append(args, pe);
			} else {
				df = sql_bind_func(sql, "sys", "diff", exp_subtype(e), NULL, F_ANALYTIC, true, true);
			}
			if (!df)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: function 'diff' not found");
			append(args, e);
			pe = exp_op(sql->sa, args, df);
		}
	} else {
		pe = exp_atom_bool(sql->sa, 0);
	}

	/* diff for orderby */
	if (obe) {
		sql_subtype *bt = sql_bind_localtype("bit");

		for( n = obe->h; n; n = n->next) {
			sql_subfunc *df;
			sql_exp *e = n->data;

			if (!exp_subtype(e))
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: parameters not allowed at ORDER BY clause from window functions");

			e = exp_copy(sql, e);
			args = sa_list(sql->sa);
			if (oe) {
				df = sql_bind_func(sql, "sys", "diff", bt, exp_subtype(e), F_ANALYTIC, true, true);
				append(args, oe);
			} else {
				df = sql_bind_func(sql, "sys", "diff", exp_subtype(e), NULL, F_ANALYTIC, true, true);
			}
			if (!df)
				return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: function 'diff' not found");
			append(args, e);
			oe = exp_op(sql->sa, args, df);
		}
	} else {
		oe = exp_atom_bool(sql->sa, 0);
	}

	if (frame_clause || supports_frames) {
		if (frame_type == FRAME_RANGE)
			ie = obe ? (sql_exp*) obe->t->data : in;
		else
			ie = obe ? oe : in;
	}
	assert(oe && pe);

	types = exp_types(sql->sa, fargs);
	wf = bind_func_(sql, sname, aname, types, F_ANALYTIC, false, &found, false);
	if (wf && !list_empty(fargs) && !(fargs = check_arguments_and_find_largest_any_type(sql, NULL, fargs, wf, 0, false)))
		wf = NULL;
	if (!wf) {
		char *arg_list = nfargs ? nary_function_arg_types_2str(sql, types, nfargs) : NULL;
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: %s window function %s%s%s'%s'(%s)",
						 found ? "insufficient privileges for" : "no such", sname ? "'":"", sname ? sname : "", sname ? "'.":"", aname, arg_list ? arg_list : "");
	}

	/* Frame */
	if (frame_clause) {
		dnode *d = frame_clause->data.lval->h;
		symbol *wstart = d->data.sym, *wend = d->next->data.sym, *rstart = wstart->data.lval->h->data.sym,
			   *rend = wend->data.lval->h->data.sym;
		int excl = d->next->next->next->data.i_val;
		bool shortcut = false;

		if (!supports_frames)
			return sql_error(sql, 02, SQLSTATE(42000) "OVER: frame extend only possible with aggregation and first_value, last_value and nth_value functions");
		if (excl != EXCLUDE_NONE)
			return sql_error(sql, 02, SQLSTATE(42000) "Only EXCLUDE NO OTHERS exclusion is currently implemented");
		if (list_empty(obe) && frame_type == FRAME_GROUPS)
			return sql_error(sql, 02, SQLSTATE(42000) "GROUPS frame requires an order by expression");
		if (wstart->token == SQL_FOLLOWING && wend->token == SQL_PRECEDING)
			return sql_error(sql, 02, SQLSTATE(42000) "FOLLOWING offset must come after PRECEDING offset");
		if (wstart->token == SQL_CURRENT_ROW && wend->token == SQL_PRECEDING)
			return sql_error(sql, 02, SQLSTATE(42000) "CURRENT ROW offset must come after PRECEDING offset");
		if (wstart->token == SQL_FOLLOWING && wend->token == SQL_CURRENT_ROW)
			return sql_error(sql, 02, SQLSTATE(42000) "FOLLOWING offset must come after CURRENT ROW offset");
		if (wstart->token != SQL_CURRENT_ROW && wend->token != SQL_CURRENT_ROW && wstart->token == wend->token && frame_type != FRAME_ROWS)
			return sql_error(sql, 02, SQLSTATE(42000) "Non-centered windows are only supported in row frames");
		if (frame_type == FRAME_RANGE) {
			if (((wstart->token == SQL_PRECEDING || wstart->token == SQL_FOLLOWING) && rstart->token != SQL_PRECEDING && rstart->token != SQL_CURRENT_ROW && rstart->token != SQL_FOLLOWING) ||
				((wend->token == SQL_PRECEDING || wend->token == SQL_FOLLOWING) && rend->token != SQL_PRECEDING && rend->token != SQL_CURRENT_ROW && rend->token != SQL_FOLLOWING)) {
				if (list_empty(obe))
					return sql_error(sql, 02, SQLSTATE(42000) "RANGE frame with PRECEDING/FOLLOWING offset requires an order by expression");
				if (list_length(obe) > 1)
					return sql_error(sql, 02, SQLSTATE(42000) "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column");
			}
		}

		if (list_empty(obe) && frame_type == FRAME_RANGE) { /* window functions are weird */
			frame_type = FRAME_ALL;
			shortcut = true;
		} else if (!is_value && (rstart->token == SQL_PRECEDING || rstart->token == SQL_CURRENT_ROW || rstart->token == SQL_FOLLOWING) && rstart->type == type_int &&
			(rend->token == SQL_PRECEDING || rend->token == SQL_CURRENT_ROW || rend->token == SQL_FOLLOWING) && rend->type == type_int) {
			 /* special cases, don't calculate bounds */
			if (frame_type != FRAME_ROWS && rstart->data.i_val == UNBOUNDED_PRECEDING_BOUND && rend->data.i_val == CURRENT_ROW_BOUND) {
				frame_type = FRAME_UNBOUNDED_TILL_CURRENT_ROW;
				shortcut = true;
			} else if (frame_type != FRAME_ROWS && rstart->data.i_val == CURRENT_ROW_BOUND && rend->data.i_val == UNBOUNDED_FOLLOWING_BOUND) {
				frame_type = FRAME_CURRENT_ROW_TILL_UNBOUNDED;
				shortcut = true;
			} else if (rstart->data.i_val == UNBOUNDED_PRECEDING_BOUND && rend->data.i_val == UNBOUNDED_FOLLOWING_BOUND) {
				frame_type = FRAME_ALL;
				shortcut = true;
			} else if (rstart->data.i_val == CURRENT_ROW_BOUND && rend->data.i_val == CURRENT_ROW_BOUND) {
				frame_type = FRAME_CURRENT_ROW;
				shortcut = true;
			}
		}
		if (!shortcut) {
			if (!(fstart = calculate_window_bound(query, p, wstart->token, rstart, ie, frame_type, f | sql_window)))
				return NULL;
			if (!(fend = calculate_window_bound(query, p, wend->token, rend, ie, frame_type, f | sql_window)))
				return NULL;
			if (!generate_window_bound_call(sql, &start, &eend, gbe ? pe : NULL, ie, fstart, fend, frame_type, excl,
											wstart->token, wend->token))
				return NULL;
		}
	} else if (supports_frames) { /* for analytic functions with no frame clause, we use the standard default values */
		if (is_value) {
			sql_subtype *bound_tp = sql_bind_localtype("lng"), *bt = (frame_type == FRAME_ROWS || frame_type == FRAME_GROUPS) ? bound_tp : exp_subtype(ie);
			unsigned char sclass = bt->type->eclass;

			fstart = exp_atom(sql->sa, atom_max_value(sql->sa, EC_NUMERIC(sclass) ? bt : bound_tp));
			fend = order_by_clause ? exp_atom(sql->sa, atom_zero_value(sql->sa, EC_NUMERIC(sclass) ? bt : bound_tp)) :
									 exp_atom(sql->sa, atom_max_value(sql->sa, EC_NUMERIC(sclass) ? bt : bound_tp));

			if (generate_window_bound_call(sql, &start, &eend, gbe ? pe : NULL, ie, fstart, fend, frame_type, EXCLUDE_NONE, SQL_PRECEDING, SQL_FOLLOWING) == NULL)
				return NULL;
		} else {
			frame_type = list_empty(obe) ? FRAME_ALL : FRAME_UNBOUNDED_TILL_CURRENT_ROW;
		}
	}

	args = sa_list(sql->sa);
	for (node *n = fargs->h ; n ; n = n->next)
		list_append(args, n->data);
	list_append(args, pe);
	list_append(args, oe);
	if (supports_frames) {
		list_append(args, exp_atom_int(sql->sa, frame_type));
		list_append(args, start ? start : exp_atom_oid(sql->sa, 1));
		list_append(args, eend ? eend : exp_atom_oid(sql->sa, 1));
	}
	call = exp_rank_op(sql->sa, list_empty(args) ? NULL : args, gbe, obe, wf);
	*rel = p;
	return call;
}

sql_exp *
rel_value_exp2(sql_query *query, sql_rel **rel, symbol *se, int f, exp_kind ek)
{
	mvc *sql = query->sql;
	if (!se)
		return NULL;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (rel && *rel && (*rel)->card == CARD_AGGR) { /* group by expression case, handle it before */
		sql_exp *exp = NULL;
		if (!is_sql_aggr(f) && !is_sql_window(f))
			exp = frame_get_groupby_expression(sql, se);
		if (sql->errstr[0] != '\0')
			return NULL;
		if (exp) {
			sql_exp *res = exp_ref(sql, exp);
			res->card = (*rel)->card;
			if (se->token == SQL_AGGR) {
				dlist *l = se->data.lval;
				int distinct = l->h->next->data.i_val;
				if (distinct)
					set_distinct(res);
			}
			if (!query_has_outer(query) && is_groupby((*rel)->op))
				res = rel_groupby_add_aggr(sql, *rel, res);
			return res;
		}
	}

	switch (se->token) {
	case SQL_NOP:
		return rel_nop(query, rel, se, f, ek);
	case SQL_AGGR:
		return rel_aggr(query, rel, se, f);
	case SQL_WINDOW:
		return rel_rankop(query, rel, se, f);
	case SQL_IDENT:
	case SQL_COLUMN:
		return rel_column_ref(query, rel, se, f );
	case SQL_NAME: {
		dlist *l = se->data.lval;
		const char *sname = qname_schema(l);
		const char *vname = qname_schema_object(l);
		return rel_exp_variable_on_scope(sql, sname, vname);
	}
	case SQL_VALUES:
	case SQL_WITH:
	case SQL_SELECT: {
		sql_rel *r = NULL;

		if (is_psm_call(f) || is_sql_merge(f))
			return sql_error(sql, 02, SQLSTATE(42000) "%s: subqueries not supported inside %s", is_psm_call(f) ? "CALL" : "MERGE", is_psm_call(f) ? "CALL statements" : "MERGE conditions");
		if (rel && *rel)
			query_push_outer(query, *rel, f);
		if (se->token == SQL_WITH) {
			r = rel_with_query(query, se);
		} else if (se->token == SQL_VALUES) {
			r = rel_values(query, se, NULL);
		} else {
			assert(se->token == SQL_SELECT);
			exp_kind nek = ek;
			nek.aggr = is_sql_aggr(f);
			if (is_sql_no_subquery(f))
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery not allowed");

			r = rel_subquery(query, se, nek);
			if (r)
				exps_label(sql, r->exps);
		}
		if (rel && *rel) {
			*rel = query_pop_outer(query);
			if (is_sql_join(f) && is_groupby((*rel)->op)) {
				return sql_error(sql, 05, SQLSTATE(42000) "SELECT: aggregate functions not allowed in JOIN conditions");
			} else if (is_sql_where(f) && is_groupby((*rel)->op)) {
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: aggregate functions not allowed in WHERE clause");
			} else if ((is_sql_update_set(f) || is_sql_psm(f)) && is_groupby((*rel)->op)) {
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: aggregate functions not allowed in SET, WHILE, IF, ELSE, CASE, WHEN, RETURN, ANALYZE clauses");
			}
		}
		if (!r)
			return NULL;
		if (ek.type == type_value && ek.card <= card_set && is_project(r->op) && list_length(r->exps) > 1)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery must return only one column");
		if (ek.type == type_relation && is_project(r->op) && list_length(r->exps) != ek.type)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery has too %s columns", list_length(r->exps) < ek.type ? "few" : "many");
		if (ek.type == type_value && list_length(r->exps) == 1 && !is_sql_psm(f)) /* for now don't rename multi attribute results */
			r = rel_zero_or_one(sql, r, ek);
		return exp_rel(sql, r);
	}
	case SQL_TABLE: {
		/* turn a subquery into a tabular result */
		*rel = rel_selects(query, se->data.sym);
		if (*rel)
			return lastexp(*rel);
		return NULL;
	}
	case SQL_PARAMETER: {
		assert(se->type == type_int);
		sql_arg *a = sql_bind_paramnr(sql, se->data.i_val);
		if (sql->emode != m_prepare) {
			if (a && a->name && a->name[0])
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: named placeholder ('%s') but named values list is missing", a->name);
			else
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: parameters ('?') not allowed in normal queries, use PREPARE");
		}
		return exp_atom_ref(sql->sa, se->data.i_val, a?&a->type:NULL);
	}
	case SQL_NULL:
		return exp_null(sql->sa, sql_bind_localtype("void"));
	case SQL_NEXT:
		return rel_next_value_for(sql, se);
	case SQL_CAST:
		return rel_cast(query, rel, se, f);
	case SQL_CASE:
	case SQL_COALESCE:
	case SQL_NULLIF:
		return rel_case_exp(query, rel, se, f);
	case SQL_RANK:
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: window function %s requires an OVER clause", qname_schema_object(se->data.lval->h->data.lval));
	case SQL_XMLELEMENT:
	case SQL_XMLFOREST:
	case SQL_XMLCOMMENT:
	case SQL_XMLATTRIBUTE:
	case SQL_XMLCONCAT:
	case SQL_XMLDOCUMENT:
	case SQL_XMLPI:
	case SQL_XMLTEXT:
		return rel_xml(query, rel, se, f, ek);
	default:
		return rel_logical_value_exp(query, rel, se, f, ek);
	}
}

static int exps_has_rank(list *exps);

static int
exp_has_rank(sql_exp *e)
{
	switch(e->type) {
	case e_convert:
		return exp_has_rank(e->l);
	case e_func:
		if (e->r)
			return 1;
		/* fall through */
	case e_aggr:
		return exps_has_rank(e->l);
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter)
			return exps_has_rank(e->l) || exps_has_rank(e->r);
		if (e->flag == cmp_in || e->flag == cmp_notin)
			return exp_has_rank(e->l) || exps_has_rank(e->r);
		return exp_has_rank(e->l) || exp_has_rank(e->r) || (e->f && exp_has_rank(e->f));
	default:
		return 0;
	}
}

/* TODO create exps_has (list, fptr ) */
static int
exps_has_rank(list *exps)
{
	if (!exps || list_empty(exps))
		return 0;
	for(node *n = exps->h; n; n=n->next){
		sql_exp *e = n->data;

		if (exp_has_rank(e))
			return 1;
	}
	return 0;
}

sql_exp *
rel_value_exp(sql_query *query, sql_rel **rel, symbol *se, int f, exp_kind ek)
{
	if (!se)
		return NULL;

	if (mvc_highwater(query->sql))
		return sql_error(query->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	sql_exp *e = rel_value_exp2(query, rel, se, f, ek);
	if (e && (se->token == SQL_SELECT || se->token == SQL_TABLE) && !exp_is_rel(e)) {
		assert(*rel);
		return rel_lastexp(query->sql, *rel);
	}
	return e;
}

static sql_exp *
column_exp(sql_query *query, sql_rel **rel, symbol *column_e, int f)
{
	dlist *l = column_e->data.lval;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_exp *ve;

	if (f == sql_sel && rel && *rel && (*rel)->card < CARD_AGGR)
		ek.card = card_value;
	ve = rel_value_exp(query, rel, l->h->data.sym, f, ek);
	if (!ve)
		return NULL;
	/* AS name */
	if (ve && l->h->next->data.sval)
		exp_setname(query->sql, ve, NULL, l->h->next->data.sval);
	return ve;
}

static int
exp_is_not_intern(sql_exp *e)
{
	return is_intern(e)?-1:0;
}

static void
rel_remove_internal_exp(sql_rel *rel)
{
	if (rel->exps) {
		list *n_exps = list_select(rel->exps, rel, (fcmp)&exp_is_not_intern, (fdup)NULL);

		rel->exps = n_exps;
	}
}

static inline int
exp_key(sql_exp *e)
{
	if (e->alias.name)
		return hash_key(e->alias.name);
	return 0;
}

static list *
group_merge_exps(mvc *sql, list *gexps, list *exps)
{
	int nexps = list_length(gexps) + list_length(exps);

	sql_hash *ht = hash_new(sql->ta, nexps, (fkeyvalue)&exp_key);

	for (node *n = gexps->h; n ; n = n->next) { /* first add grouping expressions */
		sql_exp *e = n->data;
		int key = ht->key(e);

		hash_add(ht, key, e);
	}

	for (node *n = exps->h; n ; n = n->next) { /* then test if the new grouping expressions are already there */
		sql_exp *e = n->data;
		int key = ht->key(e);
		sql_hash_e *he = ht->buckets[key&(ht->size-1)];
		bool duplicates = false;

		for (; he && !duplicates; he = he->chain) {
			sql_exp *f = he->value;

			if (!exp_equal(e, f))
				duplicates = true;
		}
		hash_add(ht, key, e);
		if (!duplicates) {
			list_append(gexps, e);
			n->data = exp_ref(sql, e);
		}
	}
	return gexps;
}

static list *
rel_table_exp(sql_query *query, sql_rel **rel, symbol *column_e, bool single_exp )
{
	mvc *sql = query->sql;
	if (column_e->token == SQL_TABLE && column_e->data.lval->h->type == type_symbol) {
		sql_rel *r;

		if (!is_project((*rel)->op))
			return NULL;
		r = rel_named_table_function(query, (*rel)->l, column_e, 0, NULL);
		if (!r)
			return NULL;
		*rel = r;
		return sa_list(sql->sa);
	}
	char *tname = NULL;
	if (column_e->token == SQL_TABLE) {
		tname = column_e->data.lval->h->data.sval;
	} else if (column_e->token == SQL_COLUMN && column_e->data.lval->h->type == type_symbol) {
		symbol *sym = column_e->data.lval->h->data.sym;
		if (sym->token == SQL_COLUMN)
			tname = sym->data.lval->h->data.sval;
		else
			return NULL;
	} else {
		return NULL;
	}


		list *exps = NULL;
		sql_rel *project = *rel, *groupby = NULL;

		/* if there's a group by relation in the tree, skip it for the '*' case and use the underlying projection */
		if (project) {
			while (is_groupby(project->op) || is_select(project->op)) {
				if (is_groupby(project->op))
					groupby = project;
				if (project->l)
					project = project->l;
			}
			assert(project);
		}

		if (project->op == op_project && project->l && project == *rel && !tname && !rel_is_ref(project) && !need_distinct(project) && single_exp) {
			sql_rel *l = project->l;
			if (!l || !is_project(l->op) || list_length(project->exps) == list_length(l->exps)) {
				rel_remove_internal_exp(*rel);
				exps = project->exps;
				*rel = project->l;
			}
		}
		if ((exps || (exps = rel_table_projections(sql, project, tname, 0)) != NULL) && !list_empty(exps)) {
			if (!(exps = check_distinct_exp_names(sql, exps)))
				return sql_error(sql, 02, SQLSTATE(42000) "Duplicate column names in table%s%s%s projection list", tname ? " '" : "", tname ? tname : "", tname ? "'" : "");
			if (groupby) {
				groupby->exps = group_merge_exps(sql, groupby->exps, exps);
				for (node *n = groupby->exps->h ; n ; n = n->next) {
					sql_exp *e = n->data;

					if (e->card > groupby->card) {
						if (exp_name(e) && !has_label(e))
							return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(e));
						return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
					}
				}
			}
			return exps;
		}
		if (!tname)
			return sql_error(sql, 02, SQLSTATE(42000) "Table expression without table name");
		return sql_error(sql, 02, SQLSTATE(42000) "Column expression Table '%s' unknown", tname);
	//return NULL;
}

sql_exp *
rel_column_exp(sql_query *query, sql_rel **rel, symbol *column_e, int f)
{
	if (column_e->token == SQL_COLUMN || column_e->token == SQL_IDENT)
		return column_exp(query, rel, column_e, f);
	return NULL;
}

static sql_rel*
rel_where_groupby_nodes(sql_query *query, sql_rel *rel, SelectNode *sn, int *group_totals)
{
	mvc *sql = query->sql;

	if (sn->where) {
		rel = rel_logical_exp(query, rel, sn->where, sql_where);
		if (!rel) {
			if (sql->errstr[0] == 0)
				return sql_error(sql, 02, SQLSTATE(42000) "Subquery result missing");
			return NULL;
		}
	}
	query_processed(query);

	if (rel && sn->groupby) {
		list *gbe = NULL, *sets = NULL;
		int all = 0;
		if (sn->groupby->data.lval == NULL) { /* ALL */
			all = 1;
		} else {
			for (dnode *o = sn->groupby->data.lval->h; o ; o = o->next) {
				symbol *grouping = o->data.sym;
				if (grouping->token == SQL_ROLLUP || grouping->token == SQL_CUBE || grouping->token == SQL_GROUPING_SETS) {
					*group_totals |= sql_group_totals;
					break;
				}
			}
			gbe = rel_groupings(query, &rel, sn->groupby, sn->selection, sql_sel | sql_groupby | *group_totals, false, &sets);
			if (!gbe)
				return NULL;
		}
		rel = rel_groupby(sql, rel, gbe);
		if (rel && all)
			rel->flag = 2;
		if (sets && list_length(sets) > 1) { /* if there is only one combination, there is no reason to generate unions */
			prop *p = prop_create(sql->sa, PROP_GROUPINGS, rel->p);
			p->value.pval = sets;
			rel->p = p;
		}
	}

	if (rel && sn->having) {
		/* having implies group by, ie if not supplied do a group by */
		if (rel->op != op_groupby)
			rel = rel_groupby(sql, rel, NULL);
	}
	query_processed(query);
	return rel;
}

static sql_rel*
rel_having_limits_nodes(sql_query *query, sql_rel *rel, SelectNode *sn, exp_kind ek, int group_totals)
{
	mvc *sql = query->sql;
	sql_rel *inner = NULL;
	int single_value = 1;

	if (is_project(rel->op) && rel->l) {
		inner = rel->l;
		single_value = 0;
	}

	if (sn->having) {
		if (inner && is_groupby(inner->op))
			set_processed(inner);
		if (!(inner = rel_logical_exp(query, inner, sn->having, sql_having | group_totals)))
			return NULL;
		if (inner->exps && exps_card(inner->exps) > CARD_AGGR)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: cannot compare sets with values, probably an aggregate function missing");
		if (!single_value)
			rel->l = inner;
	}

	if (rel && sn->distinct)
		rel = rel_distinct(rel);

	if (rel && sn->orderby) {
		list *obe = NULL;
		sql_rel *sel = NULL, *l = rel->l;

		/* project( select ) */
		if (sn->having && is_select(l->op)) {
			sel = l;
			rel->l = l->l;
		}
		rel = rel_orderby(sql, rel);
		set_processed(rel);
		obe = rel_order_by(query, &rel, sn->orderby, sn->distinct, sql_orderby | group_totals);
		if (!obe)
			return NULL;
		rel->r = obe;
		if (sel) {
			sql_rel *o = rel, *p = o->l;
			p->l = sel;
		}
	}
	if (!rel)
		return NULL;

	if (sn->limit || sn->offset) {
		sql_subtype *lng = sql_bind_localtype("lng");
		list *exps = new_exp_list(sql->sa);

		if (sn->limit) {
			sql_exp *l = rel_value_exp(query, NULL, sn->limit, 0, ek);

			if (!l || !(l=exp_check_type(sql, lng, NULL, l, type_equal)))
				return NULL;
			if ((ek.card != card_relation && sn->limit) &&
				(ek.card == card_value && sn->limit)) {
				sql_subfunc *zero_or_one = sql_bind_func(sql, "sys", "zero_or_one", exp_subtype(l), NULL, F_AGGR, true, true);
				l = exp_aggr1(sql->sa, l, zero_or_one, 0, 0, CARD_ATOM, has_nil(l));
			}
			list_append(exps, l);
		} else
			list_append(exps, exp_atom(sql->sa, atom_general(sql->sa, lng, NULL, 0)));
		if (sn->offset) {
			sql_exp *o = rel_value_exp( query, NULL, sn->offset, 0, ek);
			if (!o || !(o=exp_check_type(sql, lng, NULL, o, type_equal)))
				return NULL;
			list_append(exps, o);
		}
		rel = rel_topn(sql->sa, rel, exps);
	}

	if (sn->sample || sn->seed) {
		list *exps = new_exp_list(sql->sa);

		if (sn->sample) {
			sql_exp *s = rel_value_exp(query, NULL, sn->sample, 0, ek);
			if (!s)
				return NULL;
			if (!exp_subtype(s) && rel_set_type_param(sql, sql_bind_localtype("lng"), NULL, s, 0) < 0)
				return NULL;
			list_append(exps, s);
		} else {
			assert(sn->seed);
			return sql_error(sql, 02, SQLSTATE(42000) "SEED: cannot have SEED without SAMPLE");
		}
		if (sn->seed) {
			sql_exp *e = rel_value_exp(query, NULL, sn->seed, 0, ek);
			if (!e || !(e=exp_check_type(sql, sql_bind_localtype("int"), NULL, e, type_equal)))
				return NULL;
			list_append(exps, e);
		}
		rel = rel_sample(sql->sa, rel, exps);
	}

	/* after parsing the current query, set the group by relation as processed */
	if (!sn->having && inner && is_groupby(inner->op))
		set_processed(inner);
	if (rel)
		set_processed(rel);
	return rel;
}

static sql_rel *
join_on_column_name(sql_query *query, sql_rel *rel, sql_rel *t1, sql_rel *t2, int op, int l_nil, int r_nil)
{
	mvc *sql = query->sql;
	int found = 0, full = (op == op_full), right = (op == op_right);
	list *exps = rel_projections(sql, t1, NULL, 1, 0);
	list *r_exps = rel_projections(sql, t2, NULL, 1, 0);
	list *outexps = new_exp_list(sql->sa);

	if (!exps || !r_exps)
		return NULL;
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *le = n->data;
		int multi = 0;
		const char *rname = exp_relname(le), *name = exp_name(le);
		sql_exp *re = exps_bind_column(r_exps, name, NULL, &multi, 0);

		if (re) {
			if (multi)
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "NATURAL JOIN: common column name '%s' appears more than once in right table", rname);
			multi = 0;
			le = exps_bind_column(exps, name, NULL, &multi, 0);
			if (multi)
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "NATURAL JOIN: common column name '%s' appears more than once in left table", rname);

			found = 1;
			if (!(rel = rel_compare_exp(query, rel, le, re, "=", TRUE, 0, 0, 0, 0)))
				return NULL;
			list_remove_data(r_exps, NULL, re);
			if (full) {
				sql_exp *cond = rel_unop_(sql, rel, le, "sys", "isnull", card_value);
				if (!cond)
					return NULL;
				set_has_no_nil(cond);
				if (rel_convert_types(sql, NULL, NULL, &le, &re, 1, type_equal_no_any) < 0)
					return NULL;
				if (!(le = rel_nop_(sql, rel, cond, re, le, NULL, "sys", "ifthenelse", card_value)))
					return NULL;
			} else if (right) {
				le = re;
			}
			exp_setname(sql, le, rname, name);
			set_not_unique(le);
			append(outexps, le);
		} else {
			if (l_nil)
				set_has_nil(le);
			set_not_unique(le);
			append(outexps, le);
		}
	}
	if (!found)
		return sql_error(sql, 02, SQLSTATE(42000) "JOIN: no columns of tables '%s' and '%s' match", rel_name(t1)?rel_name(t1):"", rel_name(t2)?rel_name(t2):"");
	for (node *n = r_exps->h; n; n = n->next) {
		sql_exp *re = n->data;
		if (r_nil)
			set_has_nil(re);
		set_not_unique(re);
		append(outexps, re);
	}
	rel = rel_project(sql->sa, rel, outexps);
	return rel;
}

static sql_rel *
rel_select_exp(sql_query *query, sql_rel *rel, SelectNode *sn, exp_kind ek)
{
	mvc *sql = query->sql;
	sql_rel *inner = NULL;
	int group_totals = 0;
	list *pexps = NULL;

	assert(sn->s.token == SQL_SELECT);
	if (!sn->selection)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: the selection or from part is missing");

	if (!rel)
		rel = rel_project_exp(sql, exp_atom_bool(sql->sa, 1));
	rel = rel_where_groupby_nodes(query, rel, sn, &group_totals);
	if (sql->session->status) /* rel might be NULL as input, so we have to check for the session status for errors */
		return NULL;

	inner = rel;
	pexps = sa_list(sql->sa);
	for (dnode *n = sn->selection->h; n; n = n->next) {
		/* Here we could get real column expressions
		 * (including single atoms) but also table results.
		 * Therefore we try both rel_column_exp
		 * and rel_table_exp.
		 */
		list *te = NULL;
		sql_exp *ce = rel_column_exp(query, &inner, n->data.sym, sql_sel | group_totals | (ek.aggr?sql_aggr:0));

		if (ce) {
			if (inner && inner->flag && is_groupby(inner->op)) {
				int found = 0;
				list *gbe = inner->r;
				/* flag == 2 just add to group by/ aggrs and ref-to pexps*/
				/* flag == 1 find group by exp referencing this column nr */
				if (inner->flag == 2) {
					if (ce->card > CARD_AGGR) {
						if (!gbe)
							inner->r = gbe = sa_list(sql->sa);
						append(gbe, ce);
						ce = exp_ref(sql, ce);
						ce->card = CARD_AGGR;
						list_append(inner->exps, ce);
						ce = exp_ref(sql, ce);
						found = 1;
					}
				} else {
					for(node *n = gbe->h; n && !found; n = n->next) {
						sql_exp *e = n->data;
						if (is_atom(e->type) && !e->alias.name) {
							atom *a = e->l;
							int nr = (int)atom_get_int(a);
							if (nr == (list_length(pexps) + 1)) {
								n->data = ce;
								ce = exp_ref(sql, ce);
								ce->card = CARD_AGGR;
								list_append(inner->exps, ce);
								ce = exp_ref(sql, ce);
								found = 1;
							}
						}
					}
				}
			}
			pexps = append(pexps, ce);
			rel = inner;
			continue;
		} else {
			te = rel_table_exp(query, &rel, n->data.sym, !list_length(pexps) && !n->next);
		}
		if (!ce && !te) {
			if (sql->errstr[0])
				return NULL;
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery result missing");
		}
		/* here we should merge the column expressions we
		 * obtained so far with the table expression, ie
		 * t1.* or a subquery.
		 */
		pexps = list_merge(pexps, te, (fdup)NULL);
	}
	if (rel && is_groupby(rel->op) && rel->flag) {
		list *gbe = rel->r;
		if (!list_empty(gbe)) {
			for (node *n=gbe->h; n; n = n->next) {
				sql_exp *e = n->data;
				if (rel->flag == 1 && is_atom(e->type) && !e->alias.name) {
					atom *a = e->l;
					int nr = (int)atom_get_int(a);
					return sql_error(sql, 02, SQLSTATE(42000) "SELECT: GROUP BY position %d is not in select list", nr);
				}
				if (exp_has_aggr(rel, e))
					return sql_error(sql, 02, SQLSTATE(42000) "SELECT: aggregate functions are not allowed in GROUP BY");
			}
		}
	}
	if (rel && is_groupby(rel->op) && (!sn->groupby || rel->flag) && !is_processed(rel)) {
		for (node *n=pexps->h; n; n = n->next) {
			sql_exp *ce = n->data;
			if (rel->card < ce->card && !exp_is_aggr(rel, ce)) {
				if (exp_name(ce) && !has_label(ce))
					return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(ce));
				return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
			}
		}
		set_processed(rel);
	}
	rel = rel_project(sql->sa, rel, pexps);

	rel = rel_having_limits_nodes(query, rel, sn, ek, group_totals);
	return rel;
}

static sql_rel*
rel_unique_names(mvc *sql, sql_rel *rel)
{
	list *l;

	if (!is_project(rel->op))
		return rel;
	l = sa_list(sql->sa);
	for (node *n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		const char *name = exp_name(e);

		/* If there are two identical expression names, there will be ambiguity */
		if (!name || exps_bind_column(l, name, NULL, NULL, 0))
			exp_label(sql->sa, e, ++sql->label);
		append(l,e);
	}
	rel->exps = l;
	return rel;
}

static sql_rel *
rel_query(sql_query *query, symbol *sq, exp_kind ek)
{
	mvc *sql = query->sql;
	sql_rel *res = NULL;
	SelectNode *sn = NULL;

	if (sq->token != SQL_SELECT)
		return table_ref(query, sq, 0, NULL);

	/* select ... into is currently not handled here ! */
	sn = (SelectNode *) sq;
	if (sn->into)
		return NULL;

	if (ek.card != card_relation && sn->orderby)
		return sql_error(sql, 01, SQLSTATE(42000) "SELECT: ORDER BY only allowed on outermost SELECT");

	if (sn->window) {
		dlist *wl = sn->window->data.lval;
		for (dnode *n = wl->h; n ; n = n->next) {
			dlist *wd = n->data.sym->data.lval;
			const char *name = wd->h->data.sval;
			dlist *wdef = wd->h->next->data.lval;
			if (frame_get_window_def(sql, name, NULL)) {
				return sql_error(sql, 01, SQLSTATE(42000) "SELECT: Redefinition of window '%s'", name);
			} else if (!frame_push_window_def(sql, name, wdef)) {
				return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	}

	if (sn->from) {
		dlist *fl = sn->from->data.lval;
		sql_rel *fnd = NULL;
		list *refs = new_exp_list(sql->sa); /* Keep list of relation names in order to test for duplicates */

		for (dnode *n = fl->h; n ; n = n->next) {
			int lateral = check_is_lateral(n->data.sym);

			/* just used current expression */
			if (lateral && res)
				query_push_outer(query, res, sql_from);
			fnd = table_ref(query, n->data.sym, lateral, refs);
			if (lateral && res)
				res = query_pop_outer(query);
			if (!fnd)
				break;
			if (res) {
				res = rel_crossproduct(sql->sa, res, fnd, op_join);
				if (lateral)
					set_dependent(res);
			} else {
				res = fnd;
			}
		}
		if (!fnd) {
			if (res)
				rel_destroy(res);
			return NULL;
		}
	} else if (!query_has_outer(query) || !res) {/* only on top level query */
		return rel_select_exp(query, NULL, sn, ek);
	}

	sql_rel *rel = NULL;
	if (res)
		rel = rel_select_exp(query, res, sn, ek);
	if (!rel && res)
		rel_destroy(res);
	return rel;
}

/* NOTE: does NOT "set" query but instead generate set ops (union, except, intersect) */
static sql_rel *
rel_setquery_corresponding(sql_query *query, sql_rel *l, sql_rel *r, dlist *cols, int op, int outer, bool n_ary_op)
{
	mvc *sql = query->sql;
	const char *opname = op==SQL_EXCEPT?"EXCEPT":op==SQL_INTERSECT?"INTERSECT":outer?"OUTER UNION":"UNION";
	list *lexps = sa_list(query->sql->sa), *rexps = sa_list(query->sql->sa);
	if (!lexps || !rexps)
		return NULL;
	assert(cols);
	if (dlist_length(cols)) {
		for (dnode *dn = cols->h; dn; dn = dn ->next) {
			char *nm = dn->data.sym->data.lval->h->data.sval;
			sql_exp *ls, *rs;

			if (!(ls = rel_bind_column(sql, l, nm, sql_where | sql_join, 0)) && sql->session->status == -ERR_AMBIGUOUS)
				return NULL;
			if (!(rs = rel_bind_column(sql, r, nm, sql_where | sql_join, 0)) && sql->session->status == -ERR_AMBIGUOUS)
				return NULL;
			if ((!outer && (!ls || !rs)) || (outer && !ls && !rs))
				return sql_error(sql, 02, SQLSTATE(42000) "%s: tables '%s' and '%s' do not have a matching column '%s'", opname, rel_name(l)?rel_name(l):"", rel_name(r)?rel_name(r):"", nm);
			if (outer && !ls)
				ls = exp_null(sql->sa, exp_subtype(rs));
			if (outer && !rs)
				rs = exp_null(sql->sa, exp_subtype(ls));
			append(lexps, ls);
			append(rexps, rs);
		}
	} else {
		int found = 0;
		list *exps = rel_projections(sql, l, NULL, 1, 0), *r_exps = rel_projections(sql, r, NULL, 1, 0);
		if (!exps || !r_exps)
			return NULL;
		/* find cols which exist on both sides */
		for (node *n = exps->h; n; n = n->next) {
			sql_exp *le = n->data;
			int multi = 0;
			const char *rname = exp_relname(le), *name = exp_name(le);
			sql_exp *re = exps_bind_column(r_exps, name, NULL, &multi, 0);
			if (re) {
				if (multi)
					return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "%s: common column name '%s' appears more than once in right table", opname, rname);
				multi = 0;
				le = exps_bind_column(exps, name, NULL, &multi, 0);
				if (multi)
					return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "%s: common column name '%s' appears more than once in left table", opname, rname);

				found = 1;
				append(lexps, le);
				append(rexps, re);
				list_remove_data(r_exps, NULL, re);
			} else if (outer) {
				append(lexps, le);
				re = exp_null(sql->sa, exp_subtype(le));
				append(rexps, re); /* nils */
			}
		}
		if (!found)
			return sql_error(sql, 02, SQLSTATE(42000) "%s: no columns of tables '%s' and '%s' match", opname, rel_name(l)?rel_name(l):"", rel_name(r)?rel_name(r):"");
		if (outer) {
			for (node *n = r_exps->h; n; n = n->next) {
				sql_exp *re = n->data, *le;
				append(rexps, re);
				le = exp_null(sql->sa, exp_subtype(re));
				append(lexps, le); /* nils */
			}
		}
	}
	return n_ary_op ?
		rel_setop_n_ary_check_types(sql, l, r, lexps, rexps, (operator_type)op) :
		rel_setop_check_types(sql, l, r, lexps, rexps, (operator_type)op);
}

static sql_rel *
rel_setquery_(sql_query *query, sql_rel *l, sql_rel *r, dlist *cols, int op, int outer)
{
	mvc *sql = query->sql;
	sql_rel *rel;

	if (outer && !cols)
			return sql_error(sql, 02, SQLSTATE(42000) "UNION: OUTER needs to be combined with CORRESPONDING [ BY ( column list ) ]");
	if (!cols) {
		list *ls, *rs;

		l = rel_unique_names(sql, l);
		r = rel_unique_names(sql, r);
		ls = rel_projections(sql, l, NULL, 0, 1);
		rs = rel_projections(sql, r, NULL, 0, 1);
		rel = rel_setop_check_types(sql, l, r, ls, rs, (operator_type)op);
	} else {
		rel = rel_setquery_corresponding(query, l, r, cols, op, outer, false);
	}
	if (rel) {
		rel_setop_set_exps(sql, rel, rel_projections(sql, rel, NULL, 0, 1), false);
		set_processed(rel);
	}
	return rel;
}

/* Generate n-ary set operator */
static sql_rel *
rel_setquery_n_ary_(sql_query *query, sql_rel *l, sql_rel *r, dlist *cols, int op, int outer)
{
	/* even though this is for a general n-ary operators in this phase of the query
	 * processing we gonna have only two operands (so technically it's binary). In
	 * general this op supports arbitrary number of operands.
	 */
	// TODO: for now we support only multi-union
	assert(op == op_munion);

	mvc *sql = query->sql;
	sql_rel *rel;

	if (outer && !cols)
			return sql_error(sql, 02, SQLSTATE(42000) "UNION: OUTER needs to be combined with CORRESPONDING [ BY ( column list ) ]");
	if (!cols) {
		// TODO: make rel_setop_n_ary_check_types to accept a list of rels
		// and a list of lists of exps
		list *ls, *rs;

		l = rel_unique_names(sql, l);
		r = rel_unique_names(sql, r);
		ls = rel_projections(sql, l, NULL, 0, 1);
		rs = rel_projections(sql, r, NULL, 0, 1);
		rel = rel_setop_n_ary_check_types(sql, l, r, ls, rs, (operator_type)op);
	} else {
		rel = rel_setquery_corresponding(query, l, r, cols, op, outer, true);
	}

	if (rel) {
		rel_setop_n_ary_set_exps(sql, rel, rel_projections(sql, rel, NULL, 0, 1), false);
		set_processed(rel);
	}

	return rel;

}

static sql_rel *
rel_setquery(sql_query *query, symbol *q)
{
	mvc *sql = query->sql;
	sql_rel *res = NULL;
	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	int distinct = n->next->data.i_val;
	dlist *corresponding = n->next->next->data.lval;
	symbol *tab_ref2 = n->next->next->next->data.sym;
	sql_rel *t1, *t2;

	assert(n->next->type == type_int);
	t1 = table_ref(query, tab_ref1, 0, NULL);
	if (!t1)
		return NULL;
	t2 = table_ref(query, tab_ref2, 0, NULL);
	if (!t2)
		return NULL;

	rel_remove_internal_exp(t1);
	rel_remove_internal_exp(t2);
	if (!corresponding && list_length(t1->exps) != list_length(t2->exps)) {
		int t1nrcols = list_length(t1->exps);
		int t2nrcols = list_length(t2->exps);
		const char *op = "UNION";
		if (q->token == SQL_EXCEPT)
			op = "EXCEPT";
		else if (q->token == SQL_INTERSECT)
			op = "INTERSECT";
		rel_destroy(t1);
		rel_destroy(t2);
		return sql_error(sql, 02, SQLSTATE(42000) "%s: column counts (%d and %d) do not match", op, t1nrcols, t2nrcols);
	}
	if ( q->token == SQL_UNION) {
		int outer = n->next->next->next->next->data.i_val;
		/* For EXCEPT/INTERSECT the group by is always done within the implementation */
		/* TODO add those later in an optimizer ! */
		if (t1 && distinct)
			t1 = rel_distinct(t1);
		if (t2 && distinct)
			t2 = rel_distinct(t2);
		// TODO: this has to be fixed
		/*res = rel_setquery_(query, t1, t2, corresponding, op_union, outer);*/
		res = rel_setquery_n_ary_(query, t1, t2, corresponding, op_munion, outer);
	} else if ( q->token == SQL_EXCEPT)
		res = rel_setquery_(query, t1, t2, corresponding, op_except, 0);
	else if ( q->token == SQL_INTERSECT)
		res = rel_setquery_(query, t1, t2, corresponding, op_inter, 0);
	if (res) {
		set_processed(res);
		if (distinct)
			res = rel_distinct(res);
	}
	return res;
}

static sql_rel *
rel_joinquery_(sql_query *query, symbol *tab1, int natural, jt jointype, symbol *tab2, symbol *js, list *refs)
{
	mvc *sql = query->sql;
	operator_type op = op_join;
	sql_rel *t1 = NULL, *t2 = NULL, *inner, *rel = NULL;
	int l_nil = 0, r_nil = 0, lateral = 0;

	switch(jointype) {
	case jt_inner:
	case jt_cross: op = op_join;
		break;
	case jt_left: op = op_left;
		r_nil = 1;
		break;
	case jt_right: op = op_right;
		l_nil = 1;
		break;
	case jt_full: op = op_full;
		l_nil = 1;
		r_nil = 1;
		break;
	}

	/* a dependent join cannot depend on the right side, so disable lateral check for right and full joins */
	lateral = (op == op_join || op == op_left) && check_is_lateral(tab2);
	t1 = table_ref(query, tab1, 0, refs);
	if (t1) {
		if (!lateral) {
			t2 = table_ref(query, tab2, 0, refs);
		} else {
			query_processed(query);
			query_push_outer(query, t1, sql_from);
			t2 = table_ref(query, tab2, 0, refs);
			t1 = query_pop_outer(query);
		}
	}
	if (!t1 || !t2)
		return NULL;

	query_processed(query);
	if (strcmp(rel_name(t1), rel_name(t2)) == 0) {
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: ERROR:  table name '%s' specified more than once", rel_name(t1));
	}
	inner = rel = rel_crossproduct(sql->sa, t1, t2, op);
	if (!rel)
		return NULL;
	if (lateral)
		set_dependent(rel);

	assert(jointype != jt_cross || (!natural && !js)); /* there are no natural cross joins, or cross joins with conditions */
	if (js && natural)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: cannot have a NATURAL JOIN with a join specification (ON or USING)");
	if (jointype != jt_cross && !js && !natural)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: must have NATURAL JOIN or a JOIN with a join specification (ON or USING)");

	if (js && js->token != SQL_USING) {	/* On sql_logical_exp */
		rel = rel_logical_exp(query, rel, js, sql_where | sql_join);
	} else if (js) {	/* using */
		char rname[16], *rnme;
		dnode *n = js->data.lval->h;
		list *outexps = new_exp_list(sql->sa), *exps;
		node *m;

		rnme = sa_strdup(sql->sa, number2name(rname, sizeof(rname), ++sql->label));
		for (; n; n = n->next) {
			char *nm = n->data.sval;
			sql_exp *cond, *ls, *rs;

			if (!(ls = rel_bind_column(sql, t1, nm, sql_where | sql_join, 0)) && sql->session->status == -ERR_AMBIGUOUS)
				return NULL;
			if (!(rs = rel_bind_column(sql, t2, nm, sql_where | sql_join, 0)) && sql->session->status == -ERR_AMBIGUOUS)
				return NULL;
			if (!ls || !rs)
				return sql_error(sql, 02, SQLSTATE(42000) "JOIN: tables '%s' and '%s' do not have a matching column '%s'", rel_name(t1)?rel_name(t1):"", rel_name(t2)?rel_name(t2):"", nm);
			if (!(rel = rel_compare_exp(query, rel, ls, rs, "=", TRUE, 0, 0, 0, 0)))
				return NULL;
			if (op != op_join) {
				if (!(cond = rel_unop_(sql, rel, ls, "sys", "isnull", card_value)))
					return NULL;
				set_has_no_nil(cond);
				if (rel_convert_types(sql, t1, t2, &ls, &rs, 1, type_equal) < 0)
					return NULL;
				if (!(ls = rel_nop_(sql, rel, cond, rs, ls, NULL, "sys", "ifthenelse", card_value)))
					return NULL;
			}
			exp_setname(sql, ls, rnme, nm);
			append(outexps, ls);
			if (!rel)
				return NULL;
		}
		exps = rel_projections(sql, t1, NULL, 1, 1);
		for (m = exps->h; m; m = m->next) {
			const char *nm = exp_name(m->data);
			int fnd = 0;

			for (n = js->data.lval->h; n; n = n->next) {
				if (strcmp(nm, n->data.sval) == 0) {
					fnd = 1;
					break;
				}
			}
			if (!fnd) {
				sql_exp *ls = m->data;
				if (l_nil)
					set_has_nil(ls);
				set_not_unique(ls);
				append(outexps, ls);
			}
		}
		exps = rel_projections(sql, t2, NULL, 1, 1);
		for (m = exps->h; m; m = m->next) {
			const char *nm = exp_name(m->data);
			int fnd = 0;

			for (n = js->data.lval->h; n; n = n->next) {
				if (strcmp(nm, n->data.sval) == 0) {
					fnd = 1;
					break;
				}
			}
			if (!fnd) {
				sql_exp *rs = m->data;
				if (r_nil)
					set_has_nil(rs);
				set_not_unique(rs);
				append(outexps, rs);
			}
		}
		rel = rel_project(sql->sa, rel, outexps);
	} else if (jointype != jt_cross) {		/* ! js -> natural join */
		rel = join_on_column_name(query, rel, t1, t2, op, l_nil, r_nil);
	}
	if (!rel)
		return NULL;
	if (inner && is_outerjoin(inner->op))
		set_processed(inner);
	set_processed(rel);
	query_processed(query);
	return rel;
}

static sql_rel *
rel_joinquery(sql_query *query, symbol *q, list *refs)
{
	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	int natural = n->next->data.i_val;
	jt jointype = (jt) n->next->next->data.i_val;
	symbol *tab_ref2 = n->next->next->next->data.sym;
	symbol *joinspec = n->next->next->next->next->data.sym;

	assert(n->next->type == type_int);
	assert(n->next->next->type == type_int);
	return rel_joinquery_(query, tab_ref1, natural, jointype, tab_ref2, joinspec, refs);
}

sql_rel *
rel_subquery(sql_query *query, symbol *sq, exp_kind ek)
{
	mvc *sql = query->sql;

	query_processed(query);
	if (!stack_push_frame(sql, NULL))
		return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	sql_rel *rel = rel_query(query, sq, ek);
	stack_pop_frame(sql);
	if (!query_has_outer(query))
		query_processed(query);
	if (rel && ek.type == type_relation && ek.card < card_set && rel->card >= CARD_AGGR)
		return rel_zero_or_one(sql, rel, ek);
	return rel;
}

sql_rel *
rel_selects(sql_query *query, symbol *s)
{
	mvc *sql = query->sql;
	sql_rel *ret = NULL;

	switch (s->token) {
	case SQL_WITH:
		ret = rel_with_query(query, s);
		sql->type = Q_TABLE;
		break;
	case SQL_VALUES:
		ret = rel_values(query, s, NULL);
		sql->type = Q_TABLE;
		break;
	case SQL_SELECT: {
		exp_kind ek = {type_value, card_relation, TRUE};
		SelectNode *sn = (SelectNode *) s;

		if (sn->into) {
			sql->type = Q_SCHEMA;
			ret = rel_select_with_into(query, s);
		} else {
			ret = rel_subquery(query, s, ek);
			sql->type = Q_TABLE;
		}
	}	break;
	case SQL_JOIN:
		ret = rel_joinquery(query, s, NULL);
		sql->type = Q_TABLE;
		break;
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT:
		ret = rel_setquery(query, s);
		sql->type = Q_TABLE;
		break;
	default:
		return NULL;
	}
	if (!ret && sql->errstr[0] == 0)
		(void) sql_error(sql, 02, SQLSTATE(42000) "relational query without result");
	return ret;
}

sql_rel *
schema_selects(sql_query *query, sql_schema *schema, symbol *s)
{
	mvc *sql = query->sql;
	sql_rel *res;
	sql_schema *os = cur_schema(sql);

	sql->session->schema = schema;
	res = rel_selects(query, s);
	sql->session->schema = os;
	return res;
}

sql_rel *
rel_loader_function(sql_query *query, symbol* fcall, list *fexps, sql_subfunc **loader_function)
{
	mvc *sql = query->sql;
	sql_rel *sq = NULL;
	dnode *l = fcall->data.lval->h;
	char *sname = qname_schema(l->data.lval);
	char *fname = qname_schema_object(l->data.lval);

	list *tl = sa_list(sql->sa);
	list *exps = sa_list(sql->sa);
	if (l->next)
		l = l->next; /* skip distinct */
	if (l->next) { /* table call with subquery */
		if (l->next->type == type_symbol || l->next->type == type_list) {
			int count = 0;
			symbol *subquery = NULL;
			dnode *n = NULL;

			if (l->next->type == type_symbol)
				n = l->next;
			else
				n = l->next->data.lval?l->next->data.lval->h:NULL;

			for (dnode *m = n; m; m = m->next) {
				if (m->type == type_symbol && m->data.sym->token == SQL_SELECT)
					subquery = m->data.sym;
				count++;
			}
			if (subquery && count > 1)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: The input for the loader function '%s' must be either a single sub query, or a list of values", fname);

			if (subquery) {
				exp_kind ek = { type_value, card_relation, TRUE };
				if (!(sq = rel_subquery(query, subquery, ek)))
					return NULL;
			} else {
				exp_kind ek = { type_value, card_column, TRUE };
				list *exps = sa_list(sql->sa);
				for ( ; n; n = n->next) {
					sql_exp *e = rel_value_exp(query, NULL, n->data.sym, sql_sel | sql_from, ek);

					if (!e)
						return NULL;
					append(exps, e);
				}
				sq = rel_project(sql->sa, NULL, exps);
			}
		}
		if (!sq)
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: no such loader function %s%s%s'%s'", sname ? "'":"", sname ? sname : "", sname ? "'.":"", fname);
		for (node *en = sq->exps->h; en; en = en->next) {
			sql_exp *e = en->data;

			append(exps, e = exp_ref(sql, e));
			append(tl, exp_subtype(e));
		}
	}

	sql_exp *e = NULL;
	if (!(e = find_table_function(sql, sname, fname, exps, tl, F_LOADER)))
		return NULL;
	sql_subfunc *sf = e->f;
	if (sq) {
		for (node *n = sq->exps->h, *m = sf->func->ops->h ; n && m ; n = n->next, m = m->next) {
			sql_exp *e = (sql_exp*) n->data;
			sql_arg *a = (sql_arg*) m->data;
			if (!exp_subtype(e) && rel_set_type_param(sql, &(a->type), sq, e, 0) < 0)
				return NULL;
		}
	}

	if (loader_function)
		*loader_function = sf;

	return rel_table_func(sql->sa, sq, e, fexps, (sq)?TABLE_FROM_RELATION:TABLE_PROD_FUNC);
}
