/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*#define DEBUG*/

#include "monetdb_config.h"
#include "sql_decimal.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"
#include "rel_prop.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_select.h"
#include "rel_remote.h"
#include "rel_rewriter.h"
#include "sql_query.h"
#include "mal_errors.h" /* for SQLSTATE() */

/* some unnesting steps use the 'used' flag to avoid further rewrites. List them here, so only one reset flag iteration will be used */
#define rewrite_fix_count_used (1 << 0)
#define rewrite_values_used    (1 << 1)

#define is_rewrite_fix_count_used(X) ((X & rewrite_fix_count_used) == rewrite_fix_count_used)
#define is_rewrite_values_used(X)    ((X & rewrite_values_used) == rewrite_values_used)

static void
exp_set_freevar(mvc *sql, sql_exp *e, sql_rel *r)
{
	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			exps_set_freevar(sql, e->l, r);
			exps_set_freevar(sql, e->r, r);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			exp_set_freevar(sql, e->l, r);
			exps_set_freevar(sql, e->r, r);
		} else {
			exp_set_freevar(sql, e->l, r);
			exp_set_freevar(sql, e->r, r);
			if (e->f)
				exp_set_freevar(sql, e->f, r);
		}
		break;
	case e_convert:
		exp_set_freevar(sql, e->l, r);
		break;
	case e_func:
	case e_aggr:
		if (e->l)
			exps_set_freevar(sql, e->l, r);
		break;
	case e_column:
		if ((e->l && rel_bind_column2(sql, r, e->l, e->r, 0)) ||
		    (!e->l && rel_bind_column(sql, r, e->r, 0, 1)))
			return;
		set_freevar(e, 0);
		break;
	case e_atom:
		if (e->f)
			exps_set_freevar(sql, e->f, r);
		break;
	case e_psm:
		break;
	}
}

void
exps_set_freevar(mvc *sql, list *exps, sql_rel *r)
{
	node *n;

	if (list_empty(exps))
		return;
	for(n = exps->h; n; n = n->next)
		exp_set_freevar(sql, n->data, r);
}

/* check if the set is distinct for the set of free variables */
static int
is_distinct_set(mvc *sql, sql_rel *rel, list *ad)
{
	int distinct = 0;
	if (ad && exps_unique(sql, rel, ad ))
		return 1;
	if (ad && is_groupby(rel->op) && exp_match_list(rel->r, ad))
		return 1;
	distinct = need_distinct(rel);
	if (is_project(rel->op) && rel->l && !distinct)
		distinct = is_distinct_set(sql, rel->l, ad);
	return distinct;
}

int
exp_has_freevar(mvc *sql, sql_exp *e)
{
	if (mvc_highwater(sql)) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return 0;
	}

	if (is_freevar(e))
		return is_freevar(e);
	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			return (exps_have_freevar(sql, e->l) || exps_have_freevar(sql, e->r));
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			return (exp_has_freevar(sql, e->l) || exps_have_freevar(sql, e->r));
		} else {
			return (exp_has_freevar(sql, e->l) || exp_has_freevar(sql, e->r) ||
			    (e->f && exp_has_freevar(sql, e->f)));
		}
		break;
	case e_convert:
		return exp_has_freevar(sql, e->l);
	case e_func:
	case e_aggr:
		if (e->l)
			return exps_have_freevar(sql, e->l);
		/* fall through */
	case e_psm:
		if (exp_is_rel(e))
			return rel_has_freevar(sql, e->l);
		break;
	case e_atom:
		if (e->f)
			return exps_have_freevar(sql, e->f);
		break;
	case e_column:
	default:
		return 0;
	}
	return 0;
}

int
exps_have_freevar(mvc *sql, list *exps)
{
	if (mvc_highwater(sql)) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return 0;
	}
	if (!exps)
		return 0;
	for (node *n = exps->h; n; n = n->next) {
		int vf = 0;
		sql_exp *e = n->data;
		if ((vf =exp_has_freevar(sql, e)) != 0)
			return vf;
	}
	return 0;
}

int
rel_has_freevar(mvc *sql, sql_rel *rel)
{
	if (mvc_highwater(sql)) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return 0;
	}

	if (is_basetable(rel->op)) {
		return 0;
	} else if (is_base(rel->op)) {
		return exps_have_freevar(sql, rel->exps) ||
			(rel->l && rel_has_freevar(sql, rel->l));
	} else if (is_simple_project(rel->op) || is_groupby(rel->op) || is_select(rel->op) || is_topn(rel->op) || is_sample(rel->op)) {
		if ((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->r && exps_have_freevar(sql, rel->r))
			return 1;
		return exps_have_freevar(sql, rel->exps) ||
			(rel->l && rel_has_freevar(sql, rel->l));
	} else if (is_join(rel->op) || is_set(rel->op) || is_semi(rel->op) || is_modify(rel->op)) {
		return exps_have_freevar(sql, rel->exps) ||
			rel_has_freevar(sql, rel->l) || rel_has_freevar(sql, rel->r);
	}
	return 0;
}

static void exps_only_freevar(sql_query *query, list *exps, bool *arguments_correlated, bool *found_one_freevar, list **ungrouped_cols);
static void rel_only_freevar(sql_query *query, sql_rel *rel, bool *arguments_correlated, bool *found_one_freevar, list **ungrouped_cols);

void /* look for expressions with either only freevars or atoms */
exp_only_freevar(sql_query *query, sql_exp *e, bool *arguments_correlated, bool *found_one_freevar, list **ungrouped_cols)
{
	if (mvc_highwater(query->sql)) {
		(void) sql_error(query->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return ;
	}

	if (is_freevar(e)) {
		sql_rel *outer;

		*found_one_freevar = true;
		if (e->type == e_column) {
			if ((outer = query_fetch_outer(query, is_freevar(e)-1))) {
				sql_exp *a = rel_find_exp(outer, e);
				if (!a || !is_aggr(a->type)) {
					if (!*ungrouped_cols)
						*ungrouped_cols = new_exp_list(query->sql->sa);
					list_append(*ungrouped_cols, e);
				}
			}
		}
		return ;
	}
	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			exps_only_freevar(query, e->l, arguments_correlated, found_one_freevar, ungrouped_cols);
			exps_only_freevar(query, e->r, arguments_correlated, found_one_freevar, ungrouped_cols);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			exp_only_freevar(query, e->l, arguments_correlated, found_one_freevar, ungrouped_cols);
			exps_only_freevar(query, e->r, arguments_correlated, found_one_freevar, ungrouped_cols);
		} else {
			exp_only_freevar(query, e->l, arguments_correlated, found_one_freevar, ungrouped_cols);
			exp_only_freevar(query, e->r, arguments_correlated, found_one_freevar, ungrouped_cols);
			if (e->f)
				exp_only_freevar(query, e->f, arguments_correlated, found_one_freevar, ungrouped_cols);
		}
		break;
	case e_convert:
		exp_only_freevar(query, e->l, arguments_correlated, found_one_freevar, ungrouped_cols);
		break;
	case e_func:
	case e_aggr:
		if (e->l)
			exps_only_freevar(query, e->l, arguments_correlated, found_one_freevar, ungrouped_cols);
		break;
	case e_psm:
		if (exp_is_rel(e))
			rel_only_freevar(query, e->l, arguments_correlated, found_one_freevar, ungrouped_cols);
		break;
	case e_atom:
		if (e->f)
			exps_only_freevar(query, e->f, arguments_correlated, found_one_freevar, ungrouped_cols);
		break;
	case e_column:
		*arguments_correlated = 0;
		break;
	}
}

void
exps_only_freevar(sql_query *query, list *exps, bool *arguments_correlated, bool *found_one_freevar, list **ungrouped_cols)
{
	if (mvc_highwater(query->sql)) {
		(void) sql_error(query->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return ;
	}
	if (!exps)
		return ;
	for (node *n = exps->h; n ; n = n->next)
		exp_only_freevar(query, n->data, arguments_correlated, found_one_freevar, ungrouped_cols);
}

void
rel_only_freevar(sql_query *query, sql_rel *rel, bool *arguments_correlated, bool *found_one_freevar, list **ungrouped_cols)
{
	if (mvc_highwater(query->sql)) {
		(void) sql_error(query->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return ;
	}

	if (is_basetable(rel->op)) {
		return ;
	} else if (is_base(rel->op)) {
		exps_only_freevar(query, rel->exps, arguments_correlated, found_one_freevar, ungrouped_cols);
		if (rel->r)
			rel_only_freevar(query, rel->r, arguments_correlated, found_one_freevar, ungrouped_cols);
	} else if (is_simple_project(rel->op) || is_groupby(rel->op) || is_select(rel->op) || is_topn(rel->op) || is_sample(rel->op)) {
		if ((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->r)
			exps_only_freevar(query, rel->r, arguments_correlated, found_one_freevar, ungrouped_cols);
		exps_only_freevar(query, rel->exps, arguments_correlated, found_one_freevar, ungrouped_cols);
		if (rel->l)
			rel_only_freevar(query, rel->l, arguments_correlated, found_one_freevar, ungrouped_cols);
	} else if (is_join(rel->op) || is_set(rel->op) || is_semi(rel->op) || is_modify(rel->op)) {
		exps_only_freevar(query, rel->exps, arguments_correlated, found_one_freevar, ungrouped_cols);
		rel_only_freevar(query, rel->l, arguments_correlated, found_one_freevar, ungrouped_cols);
		rel_only_freevar(query, rel->r, arguments_correlated, found_one_freevar, ungrouped_cols);
	}
	return ;
}

static int
freevar_equal( sql_exp *e1, sql_exp *e2)
{
	assert(e1 && e2 && e1->freevar && e2->freevar);
	if (e1 == e2)
		return 0;
	if (e1->type != e_column || e2->type != e_column)
		return -1;
	if (e1->l && e2->l && strcmp(e1->l, e2->l) == 0)
		return strcmp(e1->r, e2->r);
	if (!e1->l && !e2->l)
		return strcmp(e1->r, e2->r);
	return -1;
}

static list *
merge_freevar(list *l, list *r)
{
	if (!l)
		return r;
	if (!r)
		return l;
	return list_distinct(list_merge(l, r, (fdup)NULL), (fcmp)freevar_equal, (fdup)NULL);
}

static list * exps_freevar(mvc *sql, list *exps);
static list * rel_freevar(mvc *sql, sql_rel *rel);

static list *
exp_freevar(mvc *sql, sql_exp *e)
{
	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	switch(e->type) {
	case e_column:
		if (e->freevar)
			return append(sa_list(sql->sa), e);
		break;
	case e_convert:
		return exp_freevar(sql, e->l);
	case e_aggr:
	case e_func:
		if (e->l)
			return exps_freevar(sql, e->l);
		break;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			list *l = exps_freevar(sql, e->l);
			list *r = exps_freevar(sql, e->r);
			return merge_freevar(l, r);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			list *l = exp_freevar(sql, e->l);
			list *r = exps_freevar(sql, e->r);
			return merge_freevar(l, r);
		} else {
			list *l = exp_freevar(sql, e->l);
			list *r = exp_freevar(sql, e->r);
			l = merge_freevar(l, r);
			if (e->f) {
				r = exp_freevar(sql, e->f);
				return merge_freevar(l, r);
			}
			return l;
		}
		break;
	case e_psm:
		if (exp_is_rel(e))
			if (rel_has_freevar(sql, e->l))
				return rel_freevar(sql, e->l);
		return NULL;
	case e_atom:
		if (e->f)
			return exps_freevar(sql, e->f);
		return NULL;
	default:
		return NULL;
	}
	return NULL;
}

static list *
exps_freevar(mvc *sql, list *exps)
{
	node *n;
	list *c = NULL;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
	if (!exps)
		return NULL;
	for (n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		list *var = exp_freevar(sql, e);

		c = merge_freevar(c,var);
	}
	return c;
}

static list *
rel_freevar(mvc *sql, sql_rel *rel)
{
	list *lexps = NULL, *rexps = NULL, *exps = NULL;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
	if (!rel)
		return NULL;
	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		exps = exps_freevar(sql, rel->exps);
		lexps = rel_freevar(sql, rel->l);
		rexps = rel_freevar(sql, rel->r);
		lexps = merge_freevar(lexps, rexps);
		exps = merge_freevar(exps, lexps);
		return exps;

	case op_basetable:
		return NULL;
	case op_table: {
		sql_exp *call = rel->r;
		if (rel->flag != TRIGGER_WRAPPER && rel->l)
			lexps = rel_freevar(sql, rel->l);
		exps = (rel->flag != TRIGGER_WRAPPER && call)?exps_freevar(sql, call->l):NULL;
		return merge_freevar(exps, lexps);
	}
	case op_union:
	case op_except:
	case op_inter:
		exps = exps_freevar(sql, rel->exps);
		lexps = rel_freevar(sql, rel->l);
		rexps = rel_freevar(sql, rel->r);
		lexps = merge_freevar(lexps, rexps);
		exps = merge_freevar(exps, lexps);
		return exps;
	case op_ddl:
	case op_semi:
	case op_anti:

	case op_select:
	case op_topn:
	case op_sample:

	case op_groupby:
	case op_project:
		exps = exps_freevar(sql, rel->exps);
		lexps = rel_freevar(sql, rel->l);
		if (rel->r) {
			if (is_groupby(rel->op) || is_simple_project(rel->op))
				rexps = exps_freevar(sql, rel->r);
			else
				rexps = rel_freevar(sql, rel->r);
			lexps = merge_freevar(lexps, rexps);
		}
		exps = merge_freevar(exps, lexps);
		return exps;
	default:
		return NULL;
	}

}

static list *
rel_dependent_var(mvc *sql, sql_rel *l, sql_rel *r)
{
	list *res = NULL;

	if (rel_has_freevar(sql, r)){
		list *freevar = rel_freevar(sql, r);
		if (freevar) {
			node *n;
			list *boundvar = rel_projections(sql, l, NULL, 1, 0);

			for(n = freevar->h; n; n = n->next) {
				sql_exp *e = n->data, *ne = NULL;
				/* each freevar should be an e_column */
				if (e->l) {
					ne = exps_bind_column2(boundvar, e->l, e->r, NULL);
				} else {
					ne = exps_bind_column(boundvar, e->r, NULL, NULL, 1);
				}
				if (ne) {
					if (!res)
						res = sa_list(sql->sa);
					append(res, ne);
				}
			}
		}
	}
	return res;
}

/*
 * try to bind any freevar in the expression e
 */
void
rel_bind_var(mvc *sql, sql_rel *rel, sql_exp *e)
{
	list *fvs = exp_freevar(sql, e);

	if (fvs) {
		node *n;

		for(n = fvs->h; n; n=n->next) {
			sql_exp *e = n->data;

			if (e->freevar && (exp_is_atom(e) || rel_find_exp(rel,e)))
				reset_freevar(e);
		}
	}
}

static sql_exp * push_up_project_exp(mvc *sql, sql_rel *rel, sql_exp *e);

static list *
push_up_project_exps(mvc *sql, sql_rel *rel, list *exps)
{
	node *n;

	if (!exps)
		return exps;

	for(n=exps->h; n; n=n->next) {
		sql_exp *e = n->data;

		n->data = push_up_project_exp(sql, rel, e);
	}
	list_hash_clear(exps);
	return exps;
}

static sql_exp *
push_up_project_exp(mvc *sql, sql_rel *rel, sql_exp *e)
{
	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			e->l = push_up_project_exps(sql, rel, e->l);
			e->r = push_up_project_exps(sql, rel, e->r);
			return e;
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = push_up_project_exp(sql, rel, e->l);
			e->r = push_up_project_exps(sql, rel, e->r);
			return e;
		} else {
			e->l = push_up_project_exp(sql, rel, e->l);
			e->r = push_up_project_exp(sql, rel, e->r);
			if (e->f)
				e->f = push_up_project_exp(sql, rel, e->f);
		}
		break;
	case e_convert:
		e->l = push_up_project_exp(sql, rel, e->l);
		break;
	case e_func:
	case e_aggr:
		if (e->l)
			e->l = push_up_project_exps(sql, rel, e->l);
		break;
	case e_column:
		{
			sql_exp *ne;

			/* include project or just lookup */
			if (e->l) {
				ne = exps_bind_column2(rel->exps, e->l, e->r, NULL);
			} else {
				ne = exps_bind_column(rel->exps, e->r, NULL, NULL, 1);
			}
			if (ne) {
				if (ne->type == e_column) {
					/* deref alias */
					e->l = ne->l;
					e->r = ne->r;
				} else {
					return push_up_project_exp(sql, rel, ne);
				}
			}
		} break;
	case e_atom:
		if (e->f)
			e->f = push_up_project_exps(sql, rel, e->f);
		break;
	case e_psm:
		break;
	}
	return e;
}

static sql_exp *exp_rewrite(mvc *sql, sql_rel *rel, sql_exp *e, list *ad);

static list *
exps_rewrite(mvc *sql, sql_rel *rel, list *exps, list *ad)
{
	list *nexps;
	node *n;

	if (list_empty(exps))
		return exps;
	nexps = sa_list(sql->sa);
	for(n=exps->h; n; n = n->next)
		append(nexps, exp_rewrite(sql, rel, n->data, ad));
	return nexps;
}

/* recursively rewrite some functions */
static sql_exp *
exp_rewrite(mvc *sql, sql_rel *rel, sql_exp *e, list *ad)
{
	sql_subfunc *sf;

	if (e->type == e_convert) {
		e->l = exp_rewrite(sql, rel, e->l, ad);
		return e;
	}
	if (e->type != e_func)
		return e;
	e->l = exps_rewrite(sql, rel, e->l, ad);
	sf = e->f;
	/* window functions need to be run per freevars */
	if (sf->func->type == F_ANALYTIC && strcmp(sf->func->base.name, "window_bound") != 0 && strcmp(sf->func->base.name, "diff") != 0 && ad) {
		sql_subtype *bt = sql_bind_localtype("bit");
		list *rankopargs = e->l, *gbe = ((list*)e->r)->h->data;
		sql_exp *pe = list_empty(gbe) ? NULL : (sql_exp*)gbe->t->data, *last;
		bool has_pe = pe != NULL;
		int i = 0;

		if (!pe || pe->type != e_func || strcmp(((sql_subfunc *)pe->f)->func->base.name, "diff") != 0)
			pe = NULL;

		for(node *d = ad->h; d; d=d->next) {
			sql_subfunc *df;
			sql_exp *de = d->data;
			list *args = sa_list(sql->sa);
			if (pe) {
				df = sql_bind_func(sql, NULL, "diff", bt, exp_subtype(de), F_ANALYTIC);
				append(args, pe);
			} else {
				df = sql_bind_func(sql, NULL, "diff", exp_subtype(de), NULL, F_ANALYTIC);
			}
			assert(df);
			append(args, de);
			pe = exp_op(sql->sa, args, df);
		}

		for (node *n = rankopargs->h; n ; n = n->next, i++) { /* at rel_select pe is added right after the function's arguments */
			if (i == list_length(sf->func->ops)) {
				n->data = pe;
				break;
			}
		}
		last = rankopargs->t->data; /* if the window function has bounds calls, update them */
		if (last && last->type == e_func && !strcmp(((sql_subfunc *)last->f)->func->base.name, "window_bound")) {
			sql_exp *window1 = list_fetch(rankopargs, list_length(rankopargs) - 2), *window2 = list_fetch(rankopargs, list_length(rankopargs) - 1);
			list *lw1 = window1->l, *lw2 = window2->l; /* the value functions require bound functions always */

			if (has_pe) {
				assert(list_length(window1->l) == 6);
				lw1->h->data = exp_copy(sql, pe);
				lw2->h->data = exp_copy(sql, pe);
			} else {
				window1->l = list_prepend(lw1, exp_copy(sql, pe));
				window2->l = list_prepend(lw2, exp_copy(sql, pe));
			}
		}
	}
	return e;
}

static sql_exp *
rel_reduce2one_exp(mvc *sql, sql_rel *sq)
{
	sql_exp *e = NULL;

	if (list_empty(sq->exps))
		return NULL;
	if (list_length(sq->exps) == 1)
		return sq->exps->t->data;
	for(node *n = sq->exps->h; n && !e; n = n->next) {
		sql_exp *t = n->data;

		if (!is_freevar(t))
			e = t;
	}
	if (!e)
		e = sq->exps->t->data;
	sq->exps = append(sa_list(sql->sa), e);
	return e;
}

static sql_exp *
rel_bound_exp(mvc *sql, sql_rel *rel )
{
	while (rel->l) {
		rel = rel->l;
		if (is_base(rel->op) || is_project(rel->op))
			break;
	}

	if (rel) {
		node *n;
		for(n = rel->exps->h; n; n = n->next){
			sql_exp *e = n->data;

			if (exp_is_atom(e))
				return e;
			if (!is_freevar(e))
				return exp_ref(sql, e);
		}
	}
	if (rel && is_project(rel->op)) /* add dummy expression */
		return rel_project_add_exp(sql, rel, exp_atom_bool(sql->sa, 1));
	return NULL;
}

/*
 * join j was just rewriten, but some join expressions may now
 * be too low in de relation rel. These need to move up.
 * */
static void
move_join_exps(mvc *sql, sql_rel *j, sql_rel *rel)
{
	node *n;
	list *exps = rel->exps;

	if (!exps)
		return;
	rel->exps = sa_list(sql->sa);
	if (!j->exps)
		j->exps = sa_list(sql->sa);
	for(n = exps->h; n; n = n->next){
		sql_exp *e = n->data;

		if (rel_find_exp(rel, e)) {
			if (exp_has_freevar(sql, e))
				rel_bind_var(sql, rel->l, e);
			append(rel->exps, e);
		} else {
			if (exp_has_freevar(sql, e))
				rel_bind_var(sql, j->l, e);
			append(j->exps, e);
		}
	}
}

static sql_rel *
rel_general_unnest(mvc *sql, sql_rel *rel, list *ad)
{
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel) && ad) {
		list *fd;
		node *n, *m;
		int nr;

		sql_rel *l = rel->l, *r = rel->r, *inner_r;

		/* cleanup empty selects (should be done before any rel_dup(l) */
		if (l && is_select(l->op) && list_empty(l->exps) && !rel_is_ref(l)) {
			rel->l = l->l;
			l->l = NULL;
			rel_destroy(l);
			l = rel->l;
		}
		/* rewrite T1 dependent join T2 -> T1 join D dependent join T2, where the T1/D join adds (equality) predicates (for the Domain (ad)) and D is are the distinct(projected(ad) from T1)  */
		sql_rel *D = rel_project(sql->sa, rel_dup(l), exps_copy(sql, ad));
		set_distinct(D);

		int single = is_single(r);
		reset_single(r);
		sql_rel *or = r;
		r = rel_crossproduct(sql->sa, D, r, rel->op);
		if (single)
			set_single(or);
		r->op = op_join;
		move_join_exps(sql, rel, r);
		set_dependent(r);
		inner_r = r;
		r = rel_project(sql->sa, r, (is_semi(inner_r->op))?sa_list(sql->sa):rel_projections(sql, r->r, NULL, 1, 1));

		if (!is_semi(inner_r->op))  { /* skip the free vars */
			list *exps = sa_list(sql->sa);

			for(node *n=r->exps->h; n; n = n->next) {
				sql_exp *e = n->data, *ne = NULL;

				if (e->l) {
					ne = exps_bind_column2(ad, e->l, e->r, NULL);
				} else {
					ne = exps_bind_column(ad, e->r, NULL, NULL, 1);
				}
				if (!ne)
					append(exps,e);
			}
			r->exps = exps;
		}

		/* append ad + rename */
		nr = sql->label+1;
		sql->label += list_length(ad);
		fd = exps_label(sql->sa, exps_copy(sql, ad), nr);
		for (n = ad->h, m = fd->h; n && m; n = n->next, m = m->next) {
			sql_exp *l = n->data, *r = m->data, *e;

			l = exp_ref(sql, l);
			r = exp_ref(sql, r);
			e = exp_compare(sql->sa, l, r, cmp_equal);
			set_semantics(e);
			if (!rel->exps)
				rel->exps = sa_list(sql->sa);
			append(rel->exps, e);
		}
		list_merge(r->exps, fd, (fdup)NULL);
		rel->r = r;
		reset_dependent(rel);
		return rel;
	}
	return rel;
}

static sql_rel *
push_up_project(mvc *sql, sql_rel *rel, list *ad)
{
	sql_rel *r = rel->r;

	if (rel_is_ref(r)) {
		sql_rel *nr = rel_project(sql->sa, rel_dup(r->l), exps_copy(sql, r->exps));
		rel_destroy(r);
		rel->r = r = nr;
	}

	/* input rel is dependent outerjoin with on the right a project, we first try to push inner side expressions down (because these cannot be pushed up) */
	if (rel && is_outerjoin(rel->op) && is_dependent(rel)) {
		sql_rel *r = rel->r;

		/* find constant expressions and move these down */
		if (r && r->op == op_project) {
			node *n;
			list *nexps = NULL;
			list *cexps = NULL;
			sql_rel *l = r->l;

			if (l && is_select(l->op) && !rel_is_ref(l)) {
				for(n=r->exps->h; n; n=n->next) {
					sql_exp *e = n->data;

					if (exp_is_atom(e) || rel_find_exp(l,e)) { /* move down */
						if (!cexps)
							cexps = sa_list(sql->sa);
						append(cexps, e);
					} else {
						if (!nexps)
							nexps = sa_list(sql->sa);
						append(nexps, e);
					}
				}
				if (cexps) {
					sql_rel *p = l->l = rel_project( sql->sa, l->l,
						rel_projections(sql, l->l, NULL, 1, 1));
					p->exps = list_merge(p->exps, cexps, (fdup)NULL);
					if (list_empty(nexps)) {
						rel->r = l; /* remove empty project */
					} else {
						for (n = cexps->h; n; n = n->next) { /* add pushed down renamed expressions */
							sql_exp *e = n->data;
							append(nexps, exp_ref(sql, e));
						}
						r->exps = nexps;
					}
				}
			}
		}
	}
	/* input rel is dependent join with on the right a project */
	if (rel && is_join(rel->op) && is_dependent(rel)) {
		sql_rel *r = rel->r;

		if (r && r->op == op_project) {
			sql_exp *id = NULL;
			node *m;
			/* move project up, ie all attributes of left + the old expression list */
			sql_rel *n = rel_project( sql->sa, (r->l)?rel:rel->l,
					rel_projections(sql, rel->l, NULL, 1, 1));

			/* only pass bound variables */
			if (is_left(rel->op) && exps_have_freevar(sql, r->exps)) {
				id = rel_bound_exp(sql, r);
				id = rel_project_add_exp(sql, n, id);
			}
			for (m=r->exps->h; m; m = m->next) {
				sql_exp *e = m->data;

				if (!e->freevar || exp_name(e)) { /* only skip full freevars */
					if (exp_has_freevar(sql, e)) {
						rel_bind_var(sql, rel->l, e);
						if (is_left(rel->op)) { /* add ifthenelse */
							/* need bound var from r */
							/* if id is NULL then NULL else e */
							sql_exp *ne = rel_unop_(sql, NULL, exp_copy(sql, id), "sys", "isnull", card_value);
							set_has_no_nil(ne);
							ne = rel_nop_(sql, NULL, ne, exp_null(sql->sa, exp_subtype(e)), e, NULL, "sys", "ifthenelse", card_value);
							exp_prop_alias(sql->sa, ne, e);
							e = ne;
						}
					}
				}
				if (r->l)
					e = exp_rewrite(sql, r->l, e, ad);
				append(n->exps, e);
			}
			if (r->r) {
				list *exps = r->r, *oexps = n->r = sa_list(sql->sa);

				for (m=exps->h; m; m = m->next) {
					sql_exp *e = m->data;

					if (!e->freevar || exp_name(e)) { /* only skip full freevars */
						if (exp_has_freevar(sql, e))
							rel_bind_var(sql, rel->l, e);
					}
					append(oexps, e);
				}
			}
			/* remove old project */
			if (r->l) {
				rel->r = r->l;
				r->l = NULL;
			}
			rel_destroy(r);
			return n;
		}
	}
	/* a dependent semi/anti join with a project on the right side, could be removed */
	if (rel && is_semi(rel->op) && is_dependent(rel)) {
		sql_rel *r = rel->r;

		/* merge project expressions into the join expressions  */
		rel->exps = push_up_project_exps(sql, r, rel->exps);

		if (r && r->op == op_project && r->l) {
			/* remove old project */
			rel->r = rel_dup(r->l);
			rel_destroy(r);
			return rel;
		} else if (r && r->op == op_project) {
			/* remove freevars from projection */
			list *exps = r->exps, *nexps = sa_list(sql->sa);
			node *m;

			for (m=exps->h; m; m = m->next) {
				sql_exp *e = m->data;

				if (!exp_has_freevar(sql, e))
					append(nexps, e);
			}
			if (list_empty(nexps)) {
				assert(!r->l);
				/* remove old project and change outer into select */
				rel->r = NULL;
				rel_destroy(r);
				rel->op = op_select;
				return rel;
			}
			r->exps = nexps;
		}
	}
	return rel;
}

static sql_rel *
push_up_topn_and_sample(mvc *sql, sql_rel *rel)
{
	/* a dependent semi/anti join with a project on the right side, could be removed */
	if (rel && (is_semi(rel->op) || is_join(rel->op)) && is_dependent(rel)) {
		sql_rel *r = rel->r;

		if (r && (is_topn(r->op) || is_sample(r->op))) {
			/* remove old topn/sample */
			sql_rel *(*func) (sql_allocator *, sql_rel *, list *) = is_topn(r->op) ? rel_topn : rel_sample;
			rel->r = rel_dup(r->l);
			rel = func(sql->sa, rel, r->exps);
			rel_destroy(r);
			return rel;
		}
	}
	return rel;
}

static sql_rel *
push_up_select(mvc *sql, sql_rel *rel, list *ad)
{
	sql_rel *d = rel->l;
	sql_rel *r = rel->r;
	int inner = 0;

	if (rel && is_dependent(rel) && r && is_select(r->op)) {
		sql_rel *rl = r->l;

		if (rl && rel_has_freevar(sql, rl)) {
			list *inner_ad = rel_dependent_var(sql, d, rl);

			inner = !list_empty(inner_ad);
		}
	}
	if (inner && is_left(rel->op) && !need_distinct(d))
		return rel_general_unnest(sql, rel, ad);
	/* input rel is dependent join with on the right a select */
	if ((!inner || is_semi(rel->op)) && rel && is_dependent(rel)) {
		sql_rel *r = rel->r;

		if (r && is_select(r->op)) { /* move into join */
			node *n;

			for (n=r->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				e = exp_copy(sql, e);
				if (exp_has_freevar(sql, e))
					rel_bind_var(sql, rel->l, e);
				rel_join_add_exp(sql->sa, rel, e);
			}
			/* remove select */
			rel->r = rel_dup(r->l);
			rel_destroy(r);
			r = rel->r;
			if (is_single(r)) {
				set_single(rel);
				rel->op = op_left;
			}
			if (!inner)
				reset_dependent(rel);
		}
	} else if (rel && is_join(rel->op) && is_dependent(rel)) {
		int cp = rel_is_ref(r);
		sql_rel *r = rel->r;
		list *exps = r->exps;

		/* remove select */
		rel->r = rel_dup(r->l);
		rel = rel_select(sql->sa, rel, NULL);
		rel->exps = !cp?exps:exps_copy(sql, exps);
		rel_destroy(r);
	}
	return rel;
}

static int
exps_is_constant( list *exps )
{
	sql_exp *e;

	if (!exps || list_empty(exps))
		return 1;
	if (list_length(exps) > 1)
		return 0;
	e = exps->h->data;
	return exp_is_atom(e);
}

static int
exp_is_count(sql_exp *e, sql_rel *rel)
{
	if (!e || !rel)
		return 0;
	if (is_alias(e->type) && is_project(rel->op)) {
		sql_exp *ne = rel_find_exp(rel->l, e);
		return exp_is_count(ne, rel->l);
	}
	if (is_convert(e->type))
		return exp_is_count(e->l, rel);
	if (is_aggr(e->type) && exp_aggr_is_count(e))
		return 1;
	return 0;
}

static sql_rel *
push_up_groupby(mvc *sql, sql_rel *rel, list *ad)
{
	/* input rel is dependent join with on the right a groupby */
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel)) {
		sql_rel *l = rel->l, *r = rel->r;

		/* left of rel should be a set */
		if (l && is_distinct_set(sql, l, ad) && r && is_groupby(r->op)) {
			list *sexps, *jexps, *a = rel_projections(sql, rel->l, NULL, 1, 1);
			node *n;
			sql_exp *id = NULL;

			/* move groupby up, ie add attributes of left + the old expression list */

			if (l && list_length(a) > 1 && !need_distinct(l)) { /* add identity call only if there's more than one column in the groupby */
				rel->l = rel_add_identity(sql, l, &id); /* add identity call for group by */
				assert(id);
			}

			assert(rel->op != op_anti);
			if (rel->op == op_semi && !need_distinct(l))
				rel->op = op_join;

			for (n = r->exps->h; n; n = n->next ) {
				sql_exp *e = n->data;

				/* count_nil(* or constant) -> count(t.TID) */
				if (exp_is_count(e, r) && (!e->l || exps_is_constant(e->l))) {
					sql_exp *col;
					sql_rel *p = r->l; /* ugh */

					if (!is_project(p->op))
						r->l = p = rel_project(sql->sa, p, rel_projections(sql, p, NULL, 1, 1));
					col = p->exps->t->data;
					if (strcmp(exp_name(col), TID) != 0) {
						col = exp_ref(sql, col);
						col = exp_unop(sql->sa, col, sql_bind_func(sql, "sys", "identity", exp_subtype(col), NULL, F_FUNC));
						col = exp_label(sql->sa, col, ++sql->label);
						append(p->exps, col);
					}
					col = exp_ref(sql, col);
					append(e->l=sa_list(sql->sa), col);
					set_no_nil(e);
				}
				if (exp_has_freevar(sql, e))
					rel_bind_var(sql, rel->l, e);
			}
			r->exps = list_merge(r->exps, a, (fdup)NULL);
			if (!r->r) {
				if (id)
					r->r = list_append(sa_list(sql->sa), exp_ref(sql, id));
				else
					r->r = exps_copy(sql, a);
				r->card = CARD_AGGR;
				/* After the unnesting, the cardinality of the aggregate function becomes larger */
				for(node *n = r->exps->h; n; n = n->next) {
					sql_exp *e = n->data;

					e->card = CARD_AGGR;
				}
			} else {
				for (n = ((list*)r->r)->h; n; n = n->next ) {
					sql_exp *e = n->data;

					if (exp_has_freevar(sql, e))
						rel_bind_var(sql, rel->l, e);
				}
				if (id)
					list_append(r->r, exp_ref(sql, id));
				else
					r->r = list_distinct(list_merge(r->r, exps_copy(sql, a), (fdup)NULL), (fcmp)exp_equal, (fdup)NULL);
			}

			if (!r->l) {
				r->l = rel->l;
				rel->l = NULL;
				rel->r = NULL;
				rel_destroy(rel);
				/* merge (distinct) projects / group by (over the same group by cols) */
				while (r->l && exps_have_freevar(sql, r->exps)) {
					sql_rel *l = r->l;

					if (!is_project(l->op))
						break;
					if (l->op == op_project && need_distinct(l)) { /* TODO: check if group by exps and distinct list are equal */
						r->l = rel_dup(l->l);
						rel_destroy(l);
					}
					if (is_groupby(l->op)) { /* TODO: check if group by exps and distinct list are equal */
						/* add aggr exps of r too l, replace r by l */
						node *n;
						for(n = r->exps->h; n; n = n->next) {
							sql_exp *e = n->data;

							if (e->type == e_aggr)
								append(l->exps, e);
							if (exp_has_freevar(sql, e))
								rel_bind_var(sql, l, e);
						}
						r->l = NULL;
						rel_destroy(r);
						r = l;
					}
				}
				return r;
			} else {
				rel->r = r->l;
				r->l = rel;
			}
			/* check if a join expression needs to be moved above the group by (into a select) */
			sexps = sa_list(sql->sa);
			jexps = sa_list(sql->sa);
			if (rel->exps) {
				for (n = rel->exps->h; n; n = n->next ) {
					sql_exp *e = n->data;

					if (rel_find_exp(rel, e)) {
						append(jexps, e);
					} else {
						append(sexps, e);
					}
				}
			}
			rel->exps = jexps;
			if (list_length(sexps)) {
				r = rel_select(sql->sa, r, NULL);
				r->exps = sexps;
			}
			return r;
		}
	}
	return rel;
}

static sql_rel *
push_up_select_l(mvc *sql, sql_rel *rel)
{
	(void)sql;
	/* input rel is dependent join with on the right a project */
	if (rel && (is_join(rel->op) || is_semi(rel->op))) {
		sql_rel *l = rel->l;

		if (is_select(l->op) && rel_has_freevar(sql, l) && !rel_is_ref(l) ) {
			/* push up select (above join) */
			rel->l = l->l;
			l->l = rel;
			return l;
		}
	}
	return rel;
}

static sql_rel *
push_up_join(mvc *sql, sql_rel *rel, list *ad)
{
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel)) {
		sql_rel *j = rel->r;

		if (j->op == op_join && !rel_is_ref(rel) && !rel_is_ref(j) && j->exps) {
			rel->exps =	rel->exps?list_merge(rel->exps, j->exps, (fdup)NULL):j->exps;
			j->exps = NULL;
			return rel;
		}
	}

	/* input rel is dependent join with on the right a project */
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel)) {
		sql_rel *d = rel->l, *j = rel->r;

		/* left of rel should be a set */
		if (d && is_distinct_set(sql, d, ad) && j && (is_join(j->op) || is_semi(j->op))) {
			sql_rel *jl = j->l, *jr = j->r;
			/* op_join if F(jl) intersect A(D) = empty -> jl join (D djoin jr)
			 * 	      F(jr) intersect A(D) = empty -> (D djoin jl) join jr
			 * 	 else (D djoin jl) natural join (D djoin jr)
			 *
			 * */
			list *rd = NULL, *ld = NULL;

			if (is_semi(j->op) && is_select(jl->op) && rel_has_freevar(sql, jl) && !rel_is_ref(jl)) {
				rel->r = j = push_up_select_l(sql, j);
				return rel; /* ie try again */
			}
			rd = (j->op != op_full && j->op != op_right)?rel_dependent_var(sql, d, jr):(list*)1;
			ld = ((j->op == op_join || j->op == op_right))?rel_dependent_var(sql, d, jl):(list*)1;

			if (ld && rd) {
				node *m;
				sql_rel *n, *nr, *nj;
				list *inner_exps = exps_copy(sql, j->exps);
				list *outer_exps = exps_copy(sql, rel->exps);

				rel->r = rel_dup(jl);
				rel->exps = sa_list(sql->sa);
				nj = rel_crossproduct(sql->sa, rel_dup(d), rel_dup(jr), j->op);
				if (j->single)
					set_single(nj);
				rel_destroy(j);
				j = nj;
				set_dependent(j);
				n = rel_crossproduct(sql->sa, rel, j, j->op);
				n->exps = outer_exps;
				if (!n->exps)
					n->exps = inner_exps;
				else
					n->exps = list_merge(n->exps, inner_exps, (fdup)NULL);
				j->op = rel->op;
				if (is_semi(rel->op)) {
					j->op = op_left;
					rel->op = op_left;
				}
				n->l = rel_project(sql->sa, n->l, rel_projections(sql, n->l, NULL, 1, 1));
				nr = n->r;
				nr = n->r = rel_project(sql->sa, n->r, is_semi(nr->op)?sa_list(sql->sa):rel_projections(sql, nr->r, NULL, 1, 1));
				/* add nr->l exps with labels */
				/* create jexps */
				if (!n->exps)
					n->exps = sa_list(sql->sa);
				for (m = d->exps->h; m; m = m->next) {
					sql_exp *e = m->data, *pe, *je;

					pe = exp_ref(sql, e);
					pe = exp_label(sql->sa, pe, ++sql->label);
					append(nr->exps, pe);
					pe = exp_ref(sql, pe);
					e = exp_ref(sql, e);
					je = exp_compare(sql->sa, e, pe, cmp_equal);
					set_semantics(je);
					append(n->exps, je);
				}
				return n;
			}

			if (!rd) {
				rel->r = rel_dup(jl);
				sql_rel *nj = rel_crossproduct(sql->sa, rel, rel_dup(jr), j->op);
				if (j->single)
					set_single(nj);
				nj->exps = exps_copy(sql, j->exps);
				rel_destroy(j);
				j = nj;
				if (is_semi(rel->op)) {
				//assert(!is_semi(rel->op));
					rel->op = op_left;
				}
				move_join_exps(sql, j, rel);
				return j;
			}
			if (!ld) {
				rel->r = rel_dup(jr);
				sql_rel *nj = rel_crossproduct(sql->sa, rel_dup(jl), rel, j->op);
				if (j->single)
					set_single(nj);
				nj->exps = exps_copy(sql, j->exps);
				rel_destroy(j);
				j = nj;
				if (is_semi(rel->op)) {
				//assert(!is_semi(rel->op));
					rel->op = op_left;
				}
				move_join_exps(sql, j, rel);
				return j;
			}
			assert(0);
			return rel;
		}
	}
	return rel;
}

static sql_rel *
push_up_set(mvc *sql, sql_rel *rel, list *ad)
{
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel)) {
		sql_rel *d = rel->l, *s = rel->r;

		/* left of rel should be a set */
		if (d && is_distinct_set(sql, d, ad) && s && is_set(s->op)) {
			list *sexps;
			sql_rel *sl = s->l, *sr = s->r, *n;

			sl = rel_project(sql->sa, sl, rel_projections(sql, sl, NULL, 1, 1));
			for (node *n = sl->exps->h, *m = s->exps->h; n && m; n = n->next, m = m->next)
				exp_prop_alias(sql->sa, n->data, m->data);
			sr = rel_project(sql->sa, sr, rel_projections(sql, sr, NULL, 1, 1));
			for (node *n = sr->exps->h, *m = s->exps->h; n && m; n = n->next, m = m->next)
				exp_prop_alias(sql->sa, n->data, m->data);
			/* D djoin (sl setop sr) -> (D djoin sl) setop (D djoin sr) */
			rel->r = sl;
			n = rel_crossproduct(sql->sa, rel_dup(d), sr, rel->op);
			n->exps = exps_copy(sql, rel->exps);
			set_dependent(n);
			s->l = rel;
			s->r = n;
			if (is_join(rel->op)) {
				sexps = sa_list(sql->sa);
				for (node *m = d->exps->h; m; m = m->next) {
					sql_exp *e = m->data, *pe;

					pe = exp_ref(sql, e);
					append(sexps, pe);
				}
				s->exps = list_merge(sexps, s->exps, (fdup)NULL);
			}
			/* add/remove projections to inner parts of the union (as we push a join or semijoin down) */
			s->l = rel_project(sql->sa, s->l, rel_projections(sql, s->l, NULL, 1, 1));
			s->r = rel_project(sql->sa, s->r, rel_projections(sql, s->r, NULL, 1, 1));
			if (is_semi(rel->op))
				s->exps = rel_projections(sql, s->r, NULL, 1, 1);
			return s;
		}
	}
	return rel;
}

static sql_rel * rel_unnest_dependent(mvc *sql, sql_rel *rel);

static sql_rel *
push_up_table(mvc *sql, sql_rel *rel, list *ad)
{
	(void)sql;
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel)) {
		sql_rel *d = rel->l, *tf = rel->r;
		sql_exp *id = NULL;

		/* push d into function */
		if (d && is_distinct_set(sql, d, ad) && tf && is_base(tf->op)) {
			if (tf->l) {
				sql_rel *l = tf->l;

				assert(tf->flag == TABLE_FROM_RELATION || !l->l);
				if (l->l) {
					sql_exp *tfe = tf->r;
					list *ops = tfe->l;
					rel->l = d = rel_add_identity(sql, d, &id);
					id = exp_ref(sql, id);
					l = tf->l = rel_crossproduct(sql->sa, rel_dup(d), l, op_join);
					set_dependent(l);
					tf->l = rel_unnest_dependent(sql, l);
					list_append(ops, id);
					id = exp_ref(sql, id);
				} else {
					l->l = rel_dup(d);
				}
			} else {
				tf->l = rel_dup(d);
			}
			/* we should add the identity in the resulting projection list */
			if (id) {
				sql_exp *ne = exp_copy(sql, id);

				ne = exp_label(sql->sa, ne, ++sql->label);
				ne = exp_ref(sql, ne);
				list_prepend(tf->exps, ne);
				ne = exp_ref(sql, ne);

				ne = exp_compare(sql->sa, id, ne, cmp_equal);
				//set_semantics(e);
				if (!rel->exps)
					rel->exps = sa_list(sql->sa);
				list_append(rel->exps, ne);
			}
			reset_dependent(rel);
			return rel;
		}
	}
	return rel;
}

static sql_rel *
rel_unnest_dependent(mvc *sql, sql_rel *rel)
{
	sql_rel *nrel = rel;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	/* current unnest only possible for equality joins, <, <> etc needs more work */
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel)) {
		/* howto find out the left is a set */
		sql_rel *l, *r;

		l = rel->l;
		r = rel->r;

		if (rel_has_freevar(sql, l)) {
			rel->l = rel_unnest_dependent(sql, rel->l);
			if (rel_has_freevar(sql, rel->l)) {
				if (rel->op == op_right) {
					sql_rel *l = rel->l;

					rel->l = rel->r;
					rel->r = l;
					rel->op = op_left;
					return rel_unnest_dependent(sql, rel);
				}
			}
		}

		if (!rel_has_freevar(sql, r)) {
			reset_dependent(rel);
			return rel;
		}

		/* try to push dependent join down */
		if (rel_has_freevar(sql, r)) {
			list *ad = rel_dependent_var(sql, rel->l, rel->r);

			if (r && is_select(r->op)) {
				sql_rel *l = r->l;

				if (!rel_is_ref(r) && l && !rel_is_ref(l) && l->op == op_join && list_empty(l->exps)) {
					l->exps = r->exps;
					r->l = NULL;
					rel_destroy(r);
					rel->r = l;
					return rel_unnest_dependent(sql, rel);
				}
			}

			if (r && is_simple_project(r->op) && ((!exps_have_freevar(sql, r->exps) && !exps_have_unsafe(r->exps, 1)) || is_distinct_set(sql, l, ad))) {
				rel = push_up_project(sql, rel, ad);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && (is_topn(r->op) || is_sample(r->op))) {
				rel = push_up_topn_and_sample(sql, rel);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && is_select(r->op) && ad) {
				rel = push_up_select(sql, rel, ad);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && is_groupby(r->op) && !is_left(rel->op) && is_distinct_set(sql, l, ad)) {
				rel = push_up_groupby(sql, rel, ad);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && (is_join(r->op) || is_semi(r->op)) && is_distinct_set(sql, l, ad)) {
				rel = push_up_join(sql, rel, ad);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && is_set(r->op) && !is_left(rel->op) && is_distinct_set(sql, l, ad)) {
				rel = push_up_set(sql, rel, ad);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && is_base(r->op) && is_distinct_set(sql, l, ad)) {
				rel = push_up_table(sql, rel, ad);
				return rel;
			}

			/* fallback */
			if (ad != NULL)
				rel = rel_general_unnest(sql, rel, ad);

			/* no dependent variables */
			reset_dependent(rel);
			rel->r = rel_unnest_dependent(sql, rel->r);
		} else {
			rel->l = rel_unnest_dependent(sql, rel->l);
			rel->r = rel_unnest_dependent(sql, rel->r);
		}
	} else {
		if (rel && (is_simple_project(rel->op) || is_groupby(rel->op) || is_select(rel->op) || is_topn(rel->op) || is_sample(rel->op)))
			rel->l = rel_unnest_dependent(sql, rel->l);
		else if (rel && (is_join(rel->op) || is_semi(rel->op) ||  is_set(rel->op) || is_modify(rel->op) || is_ddl(rel->op))) {
			rel->l = rel_unnest_dependent(sql, rel->l);
			rel->r = rel_unnest_dependent(sql, rel->r);
		}
	}
	return nrel;
}

static sql_rel *
_rel_unnest(visitor *v, sql_rel *rel)
{
	if (is_dependent(rel)) {
		rel = rel_unnest_dependent(v->sql, rel);
		v->changes++;
	}
	return rel;
}

static void
reset_has_nil(sql_exp *e)
{
	set_has_nil(e);
	if (is_convert(e->type))
		reset_has_nil(e->l);
}

static sql_exp *
rewrite_inner(mvc *sql, sql_rel *rel, sql_rel *inner, operator_type *op)
{
	int single = is_single(inner);
	sql_rel *d = NULL;

	reset_single(inner);
	if (single && is_project(rel->op))
		*op = op_left;

	if (!is_project(inner->op))
		inner = rel_project(sql->sa, inner, rel_projections(sql, inner, NULL, 1, 1));

	if (is_join(rel->op)){ /* TODO handle set operators etc */
		if (is_right(rel->op))
			d = rel->l = rel_crossproduct(sql->sa, rel->l, inner, *op);
		else
			d = rel->r = rel_crossproduct(sql->sa, rel->r, inner, *op);
		if (single)
			set_single(d);
	} else if (is_project(rel->op)){ /* projection -> op_left */
		if (rel->l || single || *op == op_left) {
			if ((single || *op == op_left) && !rel->l)
				rel->l = rel_project(sql->sa, rel->l, append(sa_list(sql->sa), exp_atom_bool(sql->sa, 1)));
			d = rel->l = rel_crossproduct(sql->sa, rel->l, inner, op_left);
			if (single)
				set_single(d);
		} else {
			d = rel->l = inner;
		}
	} else {
		d = rel->l = rel_crossproduct(sql->sa, rel->l, inner, *op);
		if (single)
			set_single(d);
	}
	if (d) {
		if (rel_has_freevar(sql, inner)) {
			list *dv = rel_dependent_var(sql, d, inner);
			list *fv = rel_freevar(sql, inner);
			/* check if the inner depends on the new join (d) or one leve up */
			if (list_length(dv))
				set_dependent(d);
			if (list_length(fv) != list_length(dv))
				set_dependent(rel);
		}
		*op = d->op;
	}
	return inner->exps->t->data;
}

static sql_exp *
rewrite_exp_rel(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	if (exp_is_rel(e) && is_ddl(rel->op) && rel->flag == ddl_psm) {
		sql_rel *inner = exp_rel_get_rel(v->sql->sa, e);
		if (is_single(inner)) {
			/* use a dummy projection for the single join */
			sql_rel *nrel = rel_project(v->sql->sa, NULL, append(sa_list(v->sql->sa), exp_atom_bool(v->sql->sa, 1)));
			operator_type op = depth?op_left:op_join;

			if (!rewrite_inner(v->sql, nrel, inner, &op))
				return NULL;
			/* has to apply recursively */
			if (!(e->l = rel_exp_visitor_bottomup(v, nrel, &rewrite_exp_rel, true)))
				return NULL;
			v->changes++;
		}
	} else if (exp_has_rel(e) && !is_ddl(rel->op)) {
		operator_type op = depth?op_left:op_join;
		sql_exp *ne = rewrite_inner(v->sql, rel, exp_rel_get_rel(v->sql->sa, e), &op);

		if (!ne)
			return ne;
		if (exp_is_rel(e)) {
			ne = exp_ref(v->sql, ne);
			if (exp_name(e))
				exp_prop_alias(v->sql->sa, ne, e);
			if (!exp_name(ne))
				ne = exp_label(v->sql->sa, ne, ++v->sql->label);
			e = ne;
		} else {
			e = exp_rel_update_exp(v->sql, e);
		}
		if (is_left(op))
			reset_has_nil(e);
		v->changes++;
	}
	return e;
}

/* add an dummy true projection column */
static inline sql_rel *
rewrite_empty_project(visitor *v, sql_rel *rel)
{
	if (is_simple_project(rel->op) && list_empty(rel->exps)) {
		v->changes++;
		append(rel->exps, exp_atom_bool(v->sql->sa, 1));
	} else if (is_groupby(rel->op) && list_empty(rel->exps)) {
		v->changes++;
		append(rel->exps, exp_atom_bool(v->sql->sa, 1));
	}
	return rel;
}

#define is_anyequal_func(sf) (strcmp((sf)->func->base.name, "sql_anyequal") == 0 || strcmp((sf)->func->base.name, "sql_not_anyequal") == 0)
#define is_anyequal(sf) (strcmp((sf)->func->base.name, "sql_anyequal") == 0)
#define is_not_anyequal(sf) (strcmp((sf)->func->base.name, "sql_not_anyequal") == 0)

static int exps_have_not_anyequal(list *exps);

static int
exp_has_not_anyequal(sql_exp *e)
{
	if (!e)
		return 0;
	switch(e->type){
	case e_func:
	case e_aggr: {
		list *args = e->l;
		sql_subfunc *f = e->f;

		if (f && f->func && is_not_anyequal(f) && exps_have_rel_exp(args))
			return 1;
		return exps_have_not_anyequal(e->l);
	}
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter)
			return (exps_have_not_anyequal(e->l) || exps_have_not_anyequal(e->r));
		if (e->flag == cmp_in || e->flag == cmp_notin)
			return (exp_has_not_anyequal(e->l) || exps_have_not_anyequal(e->r));
		return (exp_has_not_anyequal(e->l) || exp_has_not_anyequal(e->r) || (e->f && exp_has_not_anyequal(e->f)));
	case e_convert:
		return exp_has_not_anyequal(e->l);
	case e_atom:
		return (e->f && exp_has_not_anyequal(e->f));
	case e_psm:
	case e_column:
		return 0;
	}
	return 0;
}

static int
exps_have_not_anyequal(list *exps)
{
	if (list_empty(exps))
		return 0;
	for(node *n=exps->h; n; n=n->next) {
		sql_exp *e = n->data;

		if (exp_has_not_anyequal(e))
			return 1;
	}
	return 0;
}

/* introduce extra selects for (not) anyequal + subquery */
static sql_rel *
not_anyequal_helper(visitor *v, sql_rel *rel)
{
	if (is_innerjoin(rel->op) && exps_have_not_anyequal(rel->exps)) {
		sql_rel *nrel = rel_select(v->sql->sa, rel, NULL);
		nrel->exps = rel->exps;
		rel->exps = NULL;
		rel = nrel;
		v->changes++;
	}
	return rel;
}

/*
 * For decimals and intervals we need to adjust the scale for some operations.
 *
 * TODO move the decimal scale handling to this function.
 */
#define is_division(sf) (strcmp(sf->func->base.name, "sql_div") == 0)
#define is_multiplication(sf) (strcmp(sf->func->base.name, "sql_mul") == 0)

static inline sql_exp *
exp_physical_types(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void)rel;
	(void)depth;
	sql_exp *ne = e;

	if (!e || (e->type != e_func && e->type != e_convert) || !e->l)
		return e;

	if (e->type == e_convert) {
		sql_subtype *ft = exp_fromtype(e);
		sql_subtype *tt = exp_totype(e);

		/* complex conversion matrix */
		if (ft->type->eclass == EC_SEC && tt->type->eclass == EC_SEC && ft->type->digits > tt->type->digits) {
			/* no conversion needed, just time adjustment */
			ne = e->l;
			ne->tpe = *tt; // ugh
		}
	} else {
		list *args = e->l;
		sql_subfunc *f = e->f;

		/* multiplication and division on decimals */
		if (is_multiplication(f) && list_length(args) == 2) {
			sql_exp *le = args->h->data;
			sql_subtype *lt = exp_subtype(le);

			if (lt->type->eclass == EC_SEC || lt->type->eclass == EC_MONTH) {
				sql_exp *re = args->h->next->data;
				sql_subtype *rt = exp_subtype(re);

				if (rt->type->eclass == EC_DEC && rt->scale) {
					int scale = (int) rt->scale; /* shift with scale */
					sql_subtype *it = sql_bind_localtype(lt->type->base.name);
					sql_subfunc *c = sql_bind_func(v->sql, "sys", "scale_down", lt, it, F_FUNC);

					if (!c) {
						TRC_CRITICAL(SQL_PARSER, "scale_down missing (%s)\n", lt->type->base.name);
						return NULL;
					}
#ifdef HAVE_HGE
					hge val = scale2value(scale);
#else
					lng val = scale2value(scale);
#endif
					atom *a = atom_int(v->sql->sa, it, val);
					ne = exp_binop(v->sql->sa, e, exp_atom(v->sql->sa, a), c);
				}
			}
		} else if (is_division(f) && list_length(args) == 2) {
			sql_exp *le = args->h->data;
			sql_subtype *lt = exp_subtype(le);

			if (lt->type->eclass == EC_SEC || lt->type->eclass == EC_MONTH) {
				sql_exp *re = args->h->next->data;
				sql_subtype *rt = exp_subtype(re);

				if (rt->type->eclass == EC_DEC && rt->scale) {
					int scale = (int) rt->scale; /* shift with scale */
#ifdef HAVE_HGE
					hge val = scale2value(scale);
#else
					lng val = scale2value(scale);
#endif

					if (lt->type->eclass == EC_SEC) {
						sql_subtype *it = sql_bind_localtype(lt->type->base.name);
						sql_subfunc *c = sql_bind_func(v->sql, "sys", "scale_up", lt, it, F_FUNC);

						if (!c) {
							TRC_CRITICAL(SQL_PARSER, "scale_up missing (%s)\n", lt->type->base.name);
							return NULL;
						}
						atom *a = atom_int(v->sql->sa, it, val);
						ne = exp_binop(v->sql->sa, e, exp_atom(v->sql->sa, a), c);
					} else { /* EC_MONTH */
						sql_subtype *it = sql_bind_localtype(rt->type->base.name);
						sql_subfunc *c = sql_bind_func(v->sql, "sys", "scale_down", rt, it, F_FUNC);

						if (!c) {
							TRC_CRITICAL(SQL_PARSER, "scale_down missing (%s)\n", lt->type->base.name);
							return NULL;
						}
						atom *a = atom_int(v->sql->sa, it, val);
						args->h->next->data = exp_binop(v->sql->sa, args->h->next->data, exp_atom(v->sql->sa, a), c);
					}
				}
			}
		}
	}
	if (ne != e) {
		if (exp_name(e))
			exp_prop_alias(v->sql->sa, ne, e);
		v->changes++;
	}
	return ne;
}

static sql_exp *
exp_reset_card_and_freevar_set_physical_type(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	if (e->type == e_func && e->r) /* mark as normal (analytic) function now */
		e->r = NULL;
	reset_freevar(e); /* unnesting is done, we can remove the freevar flag */

	if (is_values(e) && list_length(exp_get_values(e))==1) { /* list of values with 1 element are redundant */
		sql_exp *val = ((list*)e->f)->h->data;
		if (exp_name(e))
			exp_prop_alias(v->sql->sa, val, e);
		e = val;
	}
	if (!(e = exp_physical_types(v, rel, e, depth))) /* for decimals and intervals we need to adjust the scale for some operations */
		return NULL;
	if (!rel->l)
		return e;

	switch(rel->op){
	case op_select:
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
	case op_project:
	case op_union:
	case op_inter:
	case op_except: {
		switch(e->type) {
		case e_aggr:
		case e_func: {
			e->card = exps_card(e->l);
		} break;
		case e_column: {
			sql_exp *le = NULL, *re = NULL;
			bool underjoinl = false, underjoinr = false;

			le = rel_find_exp_and_corresponding_rel(rel->l, e, NULL, &underjoinl);
			if (!is_simple_project(rel->op) && !is_inter(rel->op) && !is_except(rel->op) && !is_semi(rel->op) && rel->r) {
				re = rel_find_exp_and_corresponding_rel(rel->r, e, NULL, &underjoinr);
				/* if the expression is found under a join, the cardinality expands to multi */
				e->card = MAX(le?underjoinl?CARD_MULTI:le->card:CARD_ATOM, re?underjoinr?CARD_MULTI:re->card:CARD_ATOM);
			} else if (e->card == CARD_ATOM) { /* unnested columns vs atoms */
				e->card = le?underjoinl?CARD_MULTI:le->card:CARD_ATOM;
			} else { /* general case */
				e->card = (le && !underjoinl)?le->card:CARD_MULTI;
			}
			} break;
		case e_convert: {
			e->card = exp_card(e->l);
		} break;
		case e_cmp: {
			if (e->flag == cmp_or || e->flag == cmp_filter) {
				e->card = MAX(exps_card(e->l), exps_card(e->r));
			} else if (e->flag == cmp_in || e->flag == cmp_notin) {
				e->card = MAX(exp_card(e->l), exps_card(e->r));
			} else {
				e->card = MAX(exp_card(e->l), exp_card(e->r));
				if (e->f)
					e->card = MAX(e->card, exp_card(e->f));
			}
		} break;
		case e_atom:
		case e_psm:
			break;
		}
	} break;
	case op_groupby: {
		switch(e->type) {
		case e_aggr:
			e->card = rel->card;
			break;
		case e_column: {
			if (e->card == CARD_ATOM) { /* unnested columns vs atoms */
				sql_exp *le = rel_find_exp(rel->l, e);
				/* if it's from the left relation, it's either a constant or column, so set to min between le->card and aggr */
				e->card = le?MIN(le->card, CARD_AGGR):CARD_ATOM;
			} else {
				e->card = rel->card;
			}
		} break;
		default:
			break;
		}
	} break;
	default:
		break;
	}
	if (is_simple_project(rel->op) && need_distinct(rel)) /* Need distinct, all expressions should have CARD_AGGR at max */
		e->card = MIN(e->card, CARD_AGGR);
	if (!is_groupby(rel->op) || !list_empty(rel->r)) /* global groupings have atomic cardinality */
		rel->card = MAX(e->card, rel->card); /* the relation cardinality may get updated too */
	return e;
}

static list*
aggrs_split_args(mvc *sql, list *aggrs, list *exps, int is_groupby_list)
{
	bool clear_hash = false;

	if (list_empty(aggrs))
		return aggrs;
	for (node *n=aggrs->h; n; n = n->next) {
		sql_exp *a = n->data;

		if (is_func(a->type) && !is_groupby_list)
			continue;
		if (!is_aggr(a->type)) {
			sql_exp *e1 = a, *found = NULL;

			for (node *nn = exps->h; nn && !found; nn = nn->next) {
				sql_exp *e2 = nn->data;

				if (!exp_equal(e1, e2))
					found = e2;
			}
			if (!found) {
				if (!exp_name(e1))
					e1 = exp_label(sql->sa, e1, ++sql->label);
				append(exps, e1);
			} else {
				e1 = found;
			}
			e1 = exp_ref(sql, e1);
			n->data = e1; /* replace by reference */
			clear_hash = true;
			continue;
		}
		list *args = a->l;

		if (!list_empty(args)) {
			for (node *an = args->h; an; an = an->next) {
				sql_exp *e1 = an->data, *found = NULL, *eo = e1;
				/* we keep converts as they reuse names of inner columns */
				int convert = is_convert(e1->type);

				if (convert)
					e1 = e1->l;
				for (node *nn = exps->h; nn && !found; nn = nn->next) {
					sql_exp *e2 = nn->data;

					if (!exp_equal(e1, e2))
						found = e2;
				}
				if (!found) {
					if (!exp_name(e1))
						e1 = exp_label(sql->sa, e1, ++sql->label);
					append(exps, e1);
				} else {
					e1 = found;
				}
				e1 = exp_ref(sql, e1);
				/* replace by reference */
				if (convert) {
					eo->l = e1;
				} else {
					an->data = e1;
					clear_hash = true;
				}
			}
		}
	}
	if (clear_hash)
		list_hash_clear(aggrs);
	return aggrs;
}

static int
exps_complex(list *exps)
{
	if (list_empty(exps))
		return 0;
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type != e_column && e->type != e_atom)
			return 1;
	}
	return 0;
}

static int
aggrs_complex(list *exps)
{
	if (list_empty(exps))
		return 0;
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_aggr && exps_complex(e->l))
				return 1;
	}
	return 0;
}

/* simplify aggregates, ie push functions under the groupby relation */
/* rel visitor */
static inline sql_rel *
rewrite_aggregates(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) && (exps_complex(rel->r) || aggrs_complex(rel->exps))) {
		list *exps = sa_list(v->sql->sa);

		rel->r = aggrs_split_args(v->sql, rel->r, exps, 1);
		rel->exps = aggrs_split_args(v->sql, rel->exps, exps, 0);
		rel->l = rel_project(v->sql->sa, rel->l, exps);
		v->changes++;
		return rel;
	}
	return rel;
}

/* remove or expressions with subqueries */
static sql_rel *
rewrite_or_exp(visitor *v, sql_rel *rel)
{
	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps)) {
		for(node *n=rel->exps->h; n; n=n->next) {
			sql_exp *e = n->data, *id;

			if (is_compare(e->type) && e->flag == cmp_or) {
				/* check for exp_is_rel */
				if (exps_have_rel_exp(e->l) || exps_have_rel_exp(e->r)) {
					/* rewrite into setop */
					list_remove_node(rel->exps, NULL, n); /* remove or expression */
					if (is_select(rel->op) && list_empty(rel->exps) && !(rel_is_ref(rel))) { /* remove empty select if that's the case */
						sql_rel *l = rel->l;
						rel->l = NULL;
						rel_destroy(rel);
						rel = l;
					}
					rel = rel_add_identity(v->sql, rel, &id); /* identity function needed */
					(void) id;
					assert(id);

					sql_rel *l = rel;
					sql_rel *r = rel_dup(rel);
					list *exps = rel_projections(v->sql, rel, NULL, 1, 1);

					l = rel_select(v->sql->sa, l, NULL);
					l->exps = e->l;
					if (!(l = rewrite_or_exp(v, l)))
						return NULL;
					r = rel_select(v->sql->sa, r, NULL);
					r->exps = e->r;
					if (!(r = rewrite_or_exp(v, r)))
						return NULL;

					list *ls = rel_projections(v->sql, rel, NULL, 1, 1);
					list *rs = rel_projections(v->sql, rel, NULL, 1, 1);
					if (!(rel = rel_setop_check_types(v->sql, l, r, ls, rs, op_union)))
						return NULL;
					rel_setop_set_exps(v->sql, rel, exps);
					set_processed(rel);
					rel = rel_distinct(rel);
					v->changes++;
					return rel;
				}
			}
		}
		if (is_join(rel->op) && list_empty(rel->exps))
			rel->exps = NULL; /* crossproduct */
		return try_remove_empty_select(v, rel);
	}
	return rel;
}

static inline sql_rel *
rewrite_split_select_exps(visitor *v, sql_rel *rel)
{
	if (is_select(rel->op) && !list_empty(rel->exps)) {
		int i = 0;
		bool has_complex_exps = false, has_simple_exps = false, *complex_exps = SA_NEW_ARRAY(v->sql->ta, bool, list_length(rel->exps));

		for (node *n = rel->exps->h ; n ; n = n->next) {
			sql_exp *e = n->data;

			if (exp_has_rel(e) || exp_has_freevar(v->sql, e)) {
				complex_exps[i] = true;
				has_complex_exps = true;
			} else {
				complex_exps[i] = false;
				has_simple_exps = true;
			}
			i++;
		}

		if (has_complex_exps && has_simple_exps) {
			sql_rel *nsel = rel_select_copy(v->sql->sa, rel->l, NULL);
			rel->l = nsel;

			i = 0;
			for (node *n = rel->exps->h ; n ; ) {
				node *nxt = n->next;

				if (!complex_exps[i]) {
					rel_select_add_exp(v->sql->sa, nsel, n->data);
					list_remove_node(rel->exps, NULL, n);
				}
				n = nxt;
				i++;
			}
			v->changes++;
		}
	}
	return rel;
}

static void /* replace diff arguments to avoid duplicate work. The arguments must be iterated in this order! */
diff_replace_arguments(mvc *sql, sql_exp *e, list *ordering, int *pos, int *i)
{
	if (e->type == e_func && !strcmp(((sql_subfunc*)e->f)->func->base.name, "diff")) {
		list *args = (list*)e->l;
		sql_exp *first = args->h->data, *second = list_length(args) == 2 ? args->h->next->data : NULL;

		if (first->type == e_func && !strcmp(((sql_subfunc*)first->f)->func->base.name, "diff")) {
			diff_replace_arguments(sql, first, ordering, pos, i);
		} else {
			sql_exp *ne = args->h->data = exp_ref(sql, list_fetch(ordering, pos[*i]));
			set_descending(ne);
			set_nulls_first(ne);
			*i = *i + 1;
		}
		if (second && second->type == e_func && !strcmp(((sql_subfunc*)second->f)->func->base.name, "diff")) {
			diff_replace_arguments(sql, second, ordering, pos, i);
		} else if (second) {
			sql_exp *ne = args->h->next->data = exp_ref(sql, list_fetch(ordering, pos[*i]));
			set_descending(ne);
			set_nulls_first(ne);
			*i = *i + 1;
		}
	}
}

/* exp visitor */
static inline sql_exp *
rewrite_rank(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	sql_rel *rell = NULL;
	int needed = 0;

	if (!is_simple_project(rel->op) || e->type != e_func || list_length(e->r) < 2 /* e->r means window function */)
		return e;

	(void)depth;
	/* ranks/window functions only exist in the projection */
	list *l = e->l, *r = e->r, *gbe = r->h->data, *obe = r->h->next->data;
	e->card = (rel->card == CARD_AGGR) ? CARD_AGGR : CARD_MULTI; /* After the unnesting, the cardinality of the window function becomes larger */

	needed = (gbe || obe);
	if (l)
		for (node *n = l->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;
			needed = e->ref;
		}

	if (needed) {
		rell = rel->l = rel_project(v->sql->sa, rel->l, rel_projections(v->sql, rel->l, NULL, 1, 1));
		for (node *n = l->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->ref) {
				e->ref = 0;
				append(rell->exps, e);
				n->data = exp_ref(v->sql, e);
			}
		}
	}

	/* The following array remembers the original positions of gbe and obe expressions to replace them in order later at diff_replace_arguments */
	int gbeoffset = list_length(gbe), i = 0, added = 0;
	int *pos = SA_NEW_ARRAY(v->sql->ta, int, gbeoffset + list_length(obe));

	if (gbe || obe) {
		if (gbe)
			for (i = 0 ; i < gbeoffset ; i++)
				pos[i] = i;

		if (gbe && obe) {
			gbe = list_merge(sa_list(v->sql->sa), gbe, (fdup)NULL); /* make sure the p->r is a different list than the gbe list */
			i = 0;
			for(node *n = obe->h ; n ; n = n->next, i++) {
				sql_exp *e1 = n->data;
				bool found = false;
				int j = 0;

				for(node *nn = gbe->h ; nn ; nn = nn->next, j++) {
					sql_exp *e2 = nn->data;
					/* the partition expression order should be the same as the one in the order by clause (if it's in there as well) */
					if (exp_match(e1, e2)) {
						if (is_ascending(e1))
							set_ascending(e2);
						else
							set_descending(e2);
						if (nulls_last(e1))
							set_nulls_last(e2);
						else
							set_nulls_first(e2);
						found = true;
						break;
					}
				}
				if (!found) {
					pos[gbeoffset + i] = gbeoffset + added;
					added++;
					append(gbe, e1);
				} else {
					pos[gbeoffset + i] = j;
				}
			}
		} else if (obe) {
			assert(!gbe);
			i = 0;
			for(node *n = obe->h ; n ; n = n->next, i++) {
				sql_exp *oe = n->data;
				if (!exps_find_exp(rell->exps, oe)) {
					sql_exp *ne = exp_ref(v->sql, oe);

					if (is_ascending(oe))
						set_ascending(ne);
					if (nulls_last(oe))
						set_nulls_last(ne);
					/* disable sorting info (ie back too defaults) */
					set_descending(oe);
					set_nulls_first(oe);
					n->data = ne;
					append(rell->exps, oe);
				}
				pos[i] = i;
			}
			gbe = obe;
		}

		list *ordering = sa_list(v->sql->sa); /* add exps from gbe and obe as ordering expressions */
		for(node *n = gbe->h ; n ; n = n->next) {
			sql_exp *next = n->data;
			sql_exp *found = exps_find_exp(rell->exps, next);
			sql_exp *ref = exp_ref(v->sql, found ? found : next);

			if (is_ascending(next))
				set_ascending(ref);
			if (nulls_last(next))
				set_nulls_last(ref);
			set_descending(next);
			set_nulls_first(next);
			if (!found)
				list_append(rell->exps, next);
			list_append(ordering, ref);
		}
		rell = rel_project(v->sql->sa, rell, rel_projections(v->sql, rell, NULL, 1, 1));
		rell->r = ordering;
		rel->l = rell;

		/* remove obe argument, so this function won't be called again on this expression */
		list_remove_node(r, NULL, r->t);

		/* add project with rank */
		rell = rel->l = rel_project(v->sql->sa, rel->l, rel_projections(v->sql, rell->l, NULL, 1, 1));
		i = 0;

		for (node *n = l->h; n ; n = n->next) { /* replace the updated arguments */
			sql_exp *e = n->data;

			if (e->type == e_func && !strcmp(((sql_subfunc*)e->f)->func->base.name, "window_bound"))
				continue;
			diff_replace_arguments(v->sql, e, ordering, pos, &i);
		}

		sql_exp *b1 = (sql_exp*) list_fetch(l, list_length(l) - 2); /* the 'window_bound' calls are added after the function arguments and frame type */
		sql_exp *b2 = (sql_exp*) list_fetch(l, list_length(l) - 1);

		if (b1 && b1->type == e_func && !strcmp(((sql_subfunc*)b1->f)->func->base.name, "window_bound")) {
			list *ll = b1->l;
			rell = rel->l = rel_project(v->sql->sa, rell, rel_projections(v->sql, rell, NULL, 1, 1));

			int pe_pos = list_length(l) - 5; /* append the new partition expression to the list of expressions */
			sql_exp *pe = (sql_exp*) list_fetch(l, pe_pos);
			list_append(rell->exps, pe);

			if (list_length(ll) == 6) { /* update partition definition for window function input if that's the case */
				((list*)b1->l)->h->data = exp_ref(v->sql, pe);
				((list*)b2->l)->h->data = exp_ref(v->sql, pe);
			}
			i = 0; /* the partition may get a new reference, update it on the window function list of arguments as well */
			for (node *n = l->h; n ; n = n->next, i++) {
				if (i == pe_pos) {
					n->data = exp_ref(v->sql, pe);
					break;
				}
			}

			sql_exp *frame_type = (sql_exp*) list_fetch(l, list_length(l) - 3);
			atom *a = frame_type->l;
			int nr = (int)atom_get_int(a);

			if (nr == FRAME_RANGE && obe) { /* for range we pass the last order by column (otherwise it's either invalid or is a special case)*/
				int oe_pos = list_length(ll) - 5;
				sql_exp *oe = (sql_exp*) list_fetch(ll, oe_pos);
				if (oe->type != e_column && oe->type != e_atom) {
					sql_exp *common	 = list_fetch(ordering, pos[gbeoffset + list_length(obe) - 1]);

					if (list_length(ll) == 5) {
						((list*)b1->l)->h->data = exp_ref(v->sql, common);
						((list*)b2->l)->h->data = exp_ref(v->sql, common);
					} else {
						((list*)b1->l)->h->next->data = exp_ref(v->sql, common);
						((list*)b2->l)->h->next->data = exp_ref(v->sql, common);
					}
				}
			} else if (nr == FRAME_ROWS || nr == FRAME_GROUPS) {
				int oe_pos = list_length(l) - 4; /* for groups and rows, we push the ordering diff call, reference it back */
				/* now this is the tricky, part, the ordering expression, may be a column, or any projection, only the later requires the push down */
				sql_exp *oe = (sql_exp*) list_fetch(l, oe_pos);
				if (oe->type != e_column && oe->type != e_atom) {
					list_append(rell->exps, oe);

					if (list_length(ll) == 5) {
						((list*)b1->l)->h->data = exp_ref(v->sql, oe);
						((list*)b2->l)->h->data = exp_ref(v->sql, oe);
					} else {
						((list*)b1->l)->h->next->data = exp_ref(v->sql, oe);
						((list*)b2->l)->h->next->data = exp_ref(v->sql, oe);
					}
				}
			}
		}

		/* move rank down add ref */
		if (!exp_name(e))
			e = exp_label(v->sql->sa, e, ++v->sql->label);
		append(rell->exps, e);
		e = exp_ref(v->sql, e);
		v->changes++;
	} else {
		/* remove obe argument, so this function won't be called again on this expression */
		list_remove_node(r, NULL, r->t);
		v->changes++;
	}
	return e;
}

static sql_rel *
rel_union_exps(mvc *sql, sql_exp **l, list *vals, int is_tuple)
{
	sql_rel *u = NULL;
	list *exps = NULL;
	int freevar = 0;

	for (node *n=vals->h; n; n = n->next) {
		sql_exp *ve = n->data, *r, *s;
		sql_rel *sq = NULL;

		if (exp_has_rel(ve)) {
			sq = exp_rel_get_rel(sql->sa, ve); /* get subquery */
			if (sq)
				freevar = rel_has_freevar(sql,sq);
		} else {
			sq = rel_project(sql->sa, NULL, append(sa_list(sql->sa), ve));
			if (!exp_is_atom(ve))
				freevar = 1;
			set_processed(sq);
		}
		if (is_tuple) { /* cast each one */
			for (node *m=sq->exps->h, *o = ((list *)(*l)->f)->h; m && o; m = m->next, o = o->next) {
				r = m->data;
				s = o->data;
				if (rel_convert_types(sql, NULL, NULL, &s, &r, 1, type_equal) < 0)
					return NULL;
				m->data = r;
			}
		} else {
			sq->nrcols = list_length(sq->exps);
			/* union a project[(values(a),..,(b),(c)]  with freevars */
			if (sq->card > CARD_ATOM && rel_has_freevar(sql, sq) && is_project(sq->op) && !sq->l && sq->nrcols==1) {
				/* needs check on projection */
				sql_exp *vals = sq->exps->h->data;
				if (!(sq = rel_union_exps(sql, l, exp_get_values(vals), is_tuple)))
					return NULL;
			} else {
				if (rel_convert_types(sql, NULL, NULL, l, &ve, 1, type_equal) < 0)
					return NULL;
				/* flatten expressions */
				if (exp_has_rel(ve)) {
					ve = exp_rel_update_exp(sql, ve);
					sq = rel_project(sql->sa, sq, append(sa_list(sql->sa), ve));
					set_processed(sq);
				}
				if (freevar)
					exp_set_freevar(sql, ve, sq);
			}
		}
		if (!u) {
			u = sq;
		} else {
			u = rel_setop(sql->sa, u, sq, op_union);
			rel_setop_set_exps(sql, u, exps);
			set_processed(u);
		}
		exps = rel_projections(sql, sq, NULL, 1/*keep names */, 1);
	}
	return u;
}

static sql_exp *
exp_in_project(mvc *sql, sql_exp **l, list *vals, int anyequal)
{
	sql_exp *e = NULL;

	for(node *n=vals->h; n; n = n->next) {
		sql_exp *r = n->data, *ne;

		if (rel_convert_types(sql, NULL, NULL, l, &r, 1, type_equal_no_any) < 0)
			return NULL;
		if (anyequal)
			ne = rel_binop_(sql, NULL, *l, r, "sys", "=", card_value);
		else
			ne = rel_binop_(sql, NULL, *l, r, "sys", "<>", card_value);
		if (!e) {
			e = ne;
		} else if (anyequal) {
			e = rel_binop_(sql, NULL, e, ne, "sys", "or", card_value);
		} else {
			e = rel_binop_(sql, NULL, e, ne, "sys", "and", card_value);
		}
	}
	return e;
}

static sql_exp *
exp_in_compare(mvc *sql, sql_exp **l, list *vals, int anyequal)
{
	int vals_only = 1;

	for(node *n=vals->h; n; n = n->next) {
		sql_exp *r = n->data;

		if (rel_convert_types(sql, NULL, NULL, l, &r, 1, type_equal_no_any) < 0)
			return NULL;
		n->data = r;
		if (!exp_is_atom(r))
			vals_only = 0;
	}
	if (vals_only)
		return exp_in(sql->sa, *l, vals, anyequal?cmp_in:cmp_notin);

	if (!(*l = exp_in_project(sql, l, vals, anyequal)))
		return NULL;
	return exp_compare(sql->sa, *l, exp_atom_bool(sql->sa, 1), cmp_equal);
}

/* exp visitor */
static inline sql_exp *
rewrite_anyequal(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	sql_subfunc *sf;
	if (e->type != e_func)
		return e;

	sf = e->f;
	if (is_ddl(rel->op))
		return e;
	if (is_anyequal_func(sf) && !list_empty(e->l)) {
		list *l = e->l;
		mvc *sql = v->sql;

		if (list_length(l) == 2) { /* input is a set */

			sql_exp *ile = l->h->data, *le, *re = l->h->next->data;
			sql_rel *lsq = NULL, *rsq = NULL;
			int is_tuple = 0;

			/* possibly this is already done ? */
			if (exp_has_rel(ile))
				lsq = exp_rel_get_rel(sql->sa, ile); /* get subquery */

			if (lsq)
				le = exp_rel_update_exp(sql, ile);
			else
				le = ile;

			if (is_values(le)) /* exp_values */
				is_tuple = 1;

			/* re should be a values list */
			if (!lsq && !is_tuple && is_values(re) && !exps_have_rel_exp(re->f)) { /* exp_values */
				list *vals = re->f;

				if (depth == 0 && is_select(rel->op)) {
					v->changes++;
					return exp_in_compare(sql, &le, vals, is_anyequal(sf));
				} else {
					le = exp_in_project(sql, &le, vals, is_anyequal(sf));
					if (le && exp_name(e))
						exp_prop_alias(sql->sa, le, e);
					v->changes++;
					return le;
				}
			} else if (!lsq && is_tuple && is_values(re) && !exps_have_rel_exp(re->f)) {
				return e; /* leave as is, handled later */
			}

			if (is_atom(re->type) && re->f) { /* exp_values */
				/* flatten using unions */
				rsq = rel_union_exps(sql, &le, re->f, is_tuple);
				if (!rsq)
					return NULL;
				re = rsq->exps->t->data;

				if (rsq && lsq)
					exp_set_freevar(sql, re, rsq);
				if (!is_tuple && !is_freevar(re)) {
					re = exp_label(sql->sa, re, ++sql->label); /* unique name */
					list_hash_clear(rsq->exps);
					re = exp_ref(sql, re);
				} else if (has_label(re)) {
					re = exp_ref(sql, re);
				}
				set_processed(rsq);
			}

			if (is_project(rel->op) || (is_select(rel->op) && (depth > 0 || (!is_tuple && rsq && rel_has_freevar(sql, rsq) && !is_anyequal(sf))))) {
				list *exps = NULL;
				sql_exp *rid, *lid, *a = NULL;
				sql_rel *sq = lsq;
				sql_subfunc *ea = sql_bind_func(sql, "sys", is_anyequal(sf)?"anyequal":"allnotequal", exp_subtype(re), NULL, F_AGGR);
				int on_right = (lsq != NULL);

				/* we introduced extra selects */
				assert(is_project(rel->op) || is_select(rel->op));
				rsq = rel_add_identity2(sql, rsq, &rid);
				rid = exp_ref(sql, rid);

				if (!lsq)
					lsq = rel->l;
				if (!lsq) { /* single row */
					lid = exp_atom_lng(sql->sa, 1);
				} else {
					exps = rel_projections(sql, lsq, NULL, 1/*keep names */, 1);
					lsq = rel_add_identity(sql, lsq, &lid);
					if (!sq)
						rel->l = lsq;
					lid = exp_ref(sql, lid);
				}

				if (sq) {
					sql_rel *l = NULL;
					operator_type op = op_join;
					(void)rewrite_inner(sql, rel, lsq, &op);
					if (is_left(op))
						reset_has_nil(le);
					l = rel->l;
					if (l && on_right && !is_join(l->op))
						on_right = 0;
				}
				if (rsq) {
					if (on_right) {
						sql_rel *join = rel->l; /* the introduced join */
						join->r = rel_crossproduct(sql->sa, join->r, rsq, depth?op_right:op_join);
						set_dependent(join);
						if (depth)
							reset_has_nil(le);
					} else {
						operator_type op = !is_tuple?((depth>0)?op_left:op_join):is_anyequal(sf)?op_semi:op_anti;
						(void)rewrite_inner(sql, rel, rsq, &op);
						if (is_left(op))
							reset_has_nil(re);
					}
				}

				if (on_right) {
					sql_rel *join = rel->l; /* the introduced join */
					lsq = join->r = rel_groupby(sql, join->r, exp2list(sql->sa, lid));
				} else
					lsq = rel->l = rel_groupby(sql, rel->l, exp2list(sql->sa, lid));
				if (exps)
					lsq->exps = exps;

				if (is_tuple) {
					list *t = le->f;
					/*list *l = sa_list(sql->sa);
					list *r = sa_list(sql->sa);*/
					int s1 = list_length(t), s2 = list_length(rsq->exps) - 1; /* subtract identity column */

					/* find out list of right expression */
					if (s1 != s2)
						return sql_error(sql, 02, SQLSTATE(42000) "Subquery has too %s columns", (s2 < s1) ? "few" : "many");
					/*for (node *n = t->h, *m = rsq->exps->h; n && m; n = n->next, m = m->next ) {
						sql_exp *le = n->data;
						sql_exp *re = m->data;

						append(l, le);
						re = exp_ref(sql, re);
						append(r, re);
					}
					a = exp_aggr1(sql->sa, exp_values(sql->sa, l), ea, 0, 0, CARD_AGGR, has_nil(le));
					append(a->l, exp_values(sql->sa, r));*/
					return sql_error(sql, 02, SQLSTATE(42000) "Tuple matching at projections not implemented in the backend yet");
				} else {
					a = exp_aggr1(sql->sa, le, ea, 0, 0, CARD_AGGR, has_nil(le));
					append(a->l, re);
				}
				append(a->l, rid);
				le = rel_groupby_add_aggr(sql, lsq, a);
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
				set_processed(lsq);
				if (depth == 1 && is_ddl(rel->op)) { /* anyequal is at a ddl statment, it must be inside a relation */
					v->changes++;
					return exp_rel(sql, lsq);
				}
				v->changes++;
				return le;
			} else {
				if (lsq) {
					operator_type op = op_join;
					(void)rewrite_inner(sql, rel, lsq, &op);
					if (is_left(op))
						reset_has_nil(le);
				}
				if (rsq) {
					operator_type op = !is_tuple?op_join:is_anyequal(sf)?op_semi:op_anti;
					(void)rewrite_inner(sql, rel, rsq, &op);
					if (is_left(op))
						reset_has_nil(re);
				}
				if (is_tuple) {
					list *t = le->f;
					list *l = sa_list(sql->sa);
					list *r = sa_list(sql->sa);
					int s1 = list_length(t), s2 = list_length(rsq->exps);

					/* find out list of right expression */
					if (s1 != s2)
						return sql_error(sql, 02, SQLSTATE(42000) "Subquery has too %s columns", (s2 < s1) ? "few" : "many");
					for (node *n = t->h, *m = rsq->exps->h; n && m; n = n->next, m = m->next ) {
						sql_exp *le = n->data;
						sql_exp *re = m->data;

						append(l, le);
						re = exp_ref(sql, re);
						append(r, re);
					}
					v->changes++;
					return exp_in_func(sql, exp_values(sql->sa, l), exp_values(sql->sa, r), is_anyequal(sf), 1);
				} else {
					if (exp_has_freevar(sql, le))
						rel_bind_var(sql, rel, le);
					v->changes++;
					return exp_in_func(sql, le, re, is_anyequal(sf), 0);
				}
			}
		}
	}
	return e;
}

/* exp visitor */
/* rewrite compare expressions including quantifiers any and all */
static inline sql_exp *
rewrite_compare(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	sql_subfunc *sf;
	if (e->type != e_func || is_ddl(rel->op))
		return e;

	sf = e->f;
	if (is_compare_func(sf) && !list_empty(e->l)) {
		list *l = e->l;

		/* TODO handle range expressions */
		if (list_length(l) == 2) { /* input is a set */
			char *op = sf->func->base.name;

			sql_exp *ile = l->h->data, *le, *re = l->h->next->data, *rnull = NULL;
			sql_rel *lsq = NULL, *rsq = NULL;
			int is_tuple = 0; /* TODO add this feature, ie select (1,2) = (1,2) etc */
					 /* cleanup tuple handling by introducing an expression for tuples */
			int quantifier = e->flag;

			/* possibly this is already done ? */
			if (exp_has_rel(ile))
				lsq = exp_rel_get_rel(v->sql->sa, ile); /* get subquery */

			if (lsq)
				le = exp_rel_update_exp(v->sql, ile);
			else
				le = ile;

			if (exp_has_rel(re))
				rsq = exp_rel_get_rel(v->sql->sa, re); /* get subquery */
			if (rsq)
				re = exp_rel_update_exp(v->sql, re);

			if (is_values(le)) /* exp_values */
				is_tuple = 1;

			if (!is_tuple && !lsq && !rsq) { /* trivial case, just re-write into a comparison */
				e->flag = 0; /* remove quantifier */

				if (rel_convert_types(v->sql, NULL, NULL, &le, &re, 1, type_equal) < 0)
					return NULL;
				if (depth == 0 && is_select(rel->op)) {
					v->changes++;
					return exp_compare(v->sql->sa, le, re, compare_str2type(op));
				} else {
					le = exp_compare_func(v->sql, le, re, op, 0);
					if (exp_name(e))
						exp_prop_alias(v->sql->sa, le, e);
					v->changes++;
					return le;
				}
			}
			if (!is_tuple && is_values(re) && !exps_have_rel_exp(re->f)) { /* exp_values */
				list *vals = re->f;

				assert(0);
				if (depth == 0 && is_select(rel->op)) {
					v->changes++;
					return exp_in_compare(v->sql, &le, vals, is_anyequal(sf));
				} else {
					le = exp_in_project(v->sql, &le, vals, is_anyequal(sf));
					if (le && exp_name(e))
						exp_prop_alias(v->sql->sa, le, e);
					v->changes++;
					return le;
				}
			}

			if (is_values(re)) { /* exp_values */
				/* flatten using unions */
				rsq = rel_union_exps(v->sql, &le, re->f, is_tuple);
				if (!rsq)
					return NULL;
				re = rsq->exps->t->data;

				if (!is_tuple) {
					re = exp_label(v->sql->sa, re, ++v->sql->label); /* unique name */
					list_hash_clear(rsq->exps);
					re = exp_ref(v->sql, re);
				}
				set_processed(rsq);
			}

			int is_cnt = 0;
			if (rsq)
				is_cnt = exp_is_count(re, rsq);
			if (is_project(rel->op) || depth > 0 || quantifier || is_cnt) {
				sql_rel *sq = lsq;

				assert(!is_tuple);

				if (!lsq)
					lsq = rel->l;
				if (sq) {
					operator_type op = (depth||quantifier)?op_left:op_join;
					(void)rewrite_inner(v->sql, rel, sq, &op);
					if (is_left(op))
						reset_has_nil(le);
				}
				if (quantifier) {
					sql_subfunc *a;

					rsq = rel_groupby(v->sql, rsq, NULL);
					a = sql_bind_func(v->sql, "sys", "null", exp_subtype(re), NULL, F_AGGR);
					rnull = exp_aggr1(v->sql->sa, re, a, 0, 1, CARD_AGGR, has_nil(re));
					rnull = rel_groupby_add_aggr(v->sql, rsq, rnull);

					if (is_notequal_func(sf))
						op = "=";
					if (op[0] == '<') {
						a = sql_bind_func(v->sql, "sys", (quantifier==1)?"max":"min", exp_subtype(re), NULL, F_AGGR);
					} else if (op[0] == '>') {
						a = sql_bind_func(v->sql, "sys", (quantifier==1)?"min":"max", exp_subtype(re), NULL, F_AGGR);
					} else /* (op[0] == '=')*/ /* only = ALL */ {
						a = sql_bind_func(v->sql, "sys", "all", exp_subtype(re), NULL, F_AGGR);
						is_cnt = 1;
					}
					re = exp_aggr1(v->sql->sa, re, a, 0, 1, CARD_AGGR, has_nil(re));
					re = rel_groupby_add_aggr(v->sql, rsq, re);
					set_processed(rsq);
				}
				if (rsq) {
					operator_type op = ((!quantifier && depth > 0)||is_cnt)?op_left:op_join;
					(void)rewrite_inner(v->sql, rel, rsq, &op);
					if (is_left(op))
						reset_has_nil(re);
				}

				if (rel_convert_types(v->sql, NULL, NULL, &le, &re, 1, type_equal) < 0)
					return NULL;
				if (rnull) { /* complex compare operator */
					sql_exp *lnull = rel_unop_(v->sql, rel, le, "sys", "isnull", card_value);
					set_has_no_nil(lnull);
					le = exp_compare_func(v->sql, le, re, op, 0);
					le = rel_nop_(v->sql, rel, le, lnull, rnull, NULL, "sys", (quantifier==1)?"any":"all", card_value);
					if (is_notequal_func(sf))
						le = rel_unop_(v->sql, rel, le, "sys", "not", card_value);
				} else if (is_project(rel->op) || depth) {
					le = exp_compare_func(v->sql, le, re, op, 0);
				} else {
					v->changes++;
					return exp_compare(v->sql->sa, le, re, compare_str2type(op));
				}
				if (exp_name(e))
					exp_prop_alias(v->sql->sa, le, e);
				v->changes++;
				return le;
			} else {
				if (lsq) {
					operator_type op = op_join;
					(void)rewrite_inner(v->sql, rel, lsq, &op);
					if (is_left(op))
						reset_has_nil(le);
				}
				if (rsq) {
					operator_type op = !is_tuple?op_join:is_anyequal(sf)?op_semi:op_anti;
					(void)rewrite_inner(v->sql, rel, rsq, &op);
					if (is_left(op))
						reset_has_nil(re);
				}
				if (is_tuple) {
					list *t = le->f;
					list *l = sa_list(v->sql->sa);
					list *r = sa_list(v->sql->sa);
					int s1 = list_length(t), s2 = list_length(rsq->exps); /* subtract identity column */

					/* find out list of right expression */
					if (s1 != s2)
						return sql_error(v->sql, 02, SQLSTATE(42000) "Subquery has too %s columns", (s2 < s1) ? "few" : "many");
					for (node *n = t->h, *m = rsq->exps->h; n && m; n = n->next, m = m->next ) {
						sql_exp *le = n->data;
						sql_exp *re = m->data;

						append(l, le);
						append(r, re);
					}
					v->changes++;
					return exp_compare(v->sql->sa, exp_values(v->sql->sa, l), exp_values(v->sql->sa, r), compare_str2type(op));
				} else {
					if (exp_has_freevar(v->sql, le))
						rel_bind_var(v->sql, rel, le);
					if (rel_convert_types(v->sql, NULL, NULL, &le, &re, 1, type_equal) < 0)
						return NULL;
					v->changes++;
					return exp_compare(v->sql->sa, le, re, compare_str2type(op));
				}
			}
		}
	}
	return e;
}

static sql_rel *
rewrite_join2semi(visitor *v, sql_rel *rel)
{
	if (is_select(rel->op) && !list_empty(rel->exps)) {
		sql_rel *j = rel->l;
		int needed=0;

		if (!j || (!is_join(j->op) && !is_semi(j->op)) || !list_empty(j->exps))
			return rel;
		/* if needed first push select exps down under the join */
		for(node *n = rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;

			if (is_func(e->type) && exp_card(e) > CARD_ATOM && is_anyequal_func(sf) && rel_has_all_exps(j->l, e->l))
				needed = 1;
		}
		if (needed) {
			list *exps = sa_list(v->sql->sa);
			sql_rel *l = j->l = rel_select(v->sql->sa, j->l, NULL);

			for (node *n = rel->exps->h; n;) {
				node *next = n->next;
				sql_exp *e = n->data;
				sql_subfunc *sf = e->f;

				if (is_func(e->type) && exp_card(e) > CARD_ATOM && is_anyequal_func(sf) && rel_has_all_exps(j->l, e->l)) {
					append(exps, e);
					list_remove_node(rel->exps, NULL, n);
				}
				n = next;
			}
			l->exps = exps;
			j->l = rewrite_join2semi(v, j->l);
		}

		needed = 0;
		for(node *n = rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;

			if (is_func(e->type) && is_anyequal_func(sf))
				needed = 1;
		}
		if (!needed)
			return try_remove_empty_select(v, rel);
		if (!j->exps)
			j->exps = sa_list(v->sql->sa);
		for (node *n = rel->exps->h; n; ) {
			node *next = n->next;
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;

			/* If the left relation cannot hold the comparison it cannot be pushed to the semi(anti)-join
			   For now I guess only comparisons or anyequal func can appear here */
			assert((is_func(e->type) && is_anyequal_func(sf)) || e->type == e_cmp);
			if ((is_func(e->type) && is_anyequal_func(sf)) || !rel_rebind_exp(v->sql, j->l, e)) {
				if (e->type == e_cmp) {
					append(j->exps, e);
				} else {
					list *args = e->l;
					sql_exp *l, *r;

					assert(list_length(args)==2);
					l = args->h->data;
					r = args->h->next->data;
					j->op = (is_anyequal(sf))?op_semi:op_anti;

					if (is_values(l)) {
						assert(is_values(r));
						list *ll = l->f, *rl = r->f;
						for(node *n=ll->h, *m=rl->h; n && m; n=n->next, m=m->next) {
							e = exp_compare(v->sql->sa, n->data, m->data, j->op == op_semi?mark_in:mark_notin);
							append(j->exps, e);
						}
					} else {
						e = exp_compare(v->sql->sa, l, r, j->op == op_semi?mark_in:mark_notin);
						append(j->exps, e);
					}
				}
				list_remove_node(rel->exps, NULL, n);
			}
			n = next;
		}
		v->changes++;
		rel = try_remove_empty_select(v, rel);
	}
	return rel;
}

#define is_exists_func(sf) (strcmp(sf->func->base.name, "sql_exists") == 0 || strcmp(sf->func->base.name, "sql_not_exists") == 0)
#define is_exists(sf) (strcmp(sf->func->base.name, "sql_exists") == 0)

static sql_exp *
exp_exist(mvc *sql, sql_exp *le, sql_exp *ne, int exists)
{
	sql_subfunc *exists_func = NULL;

	if (exists)
		exists_func = sql_bind_func(sql, "sys", "sql_exists", exp_subtype(le), NULL, F_FUNC);
	else
		exists_func = sql_bind_func(sql, "sys", "sql_not_exists", exp_subtype(le), NULL, F_FUNC);

	if (!exists_func)
		return sql_error(sql, 02, SQLSTATE(42000) "exist operator on type %s missing", exp_subtype(le)->type->sqlname);
	if (ne) { /* correlated case */
		ne = rel_unop_(sql, NULL, ne, "sys", "isnull", card_value);
		set_has_no_nil(ne);
		le = rel_nop_(sql, NULL, ne, exp_atom_bool(sql->sa, !exists), exp_atom_bool(sql->sa, exists), NULL, "sys", "ifthenelse", card_value);
		return le;
	} else {
		sql_exp *res = exp_unop(sql->sa, le, exists_func);
		set_has_no_nil(res);
		return res;
	}
}

/* exp visitor */
static sql_exp *
rewrite_exists(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	sql_subfunc *sf;
	if (e->type != e_func)
		return e;

	sf = e->f;
	if (is_exists_func(sf) && !list_empty(e->l)) {
		list *l = e->l;

		if (list_length(l) == 1) { /* exp_values */
			sql_exp *ne = NULL, *ie = l->h->data, *le;
			sql_rel *sq = NULL;

			if (!exp_is_rel(ie)) { /* exists over a constant or a single value */
				le = exp_atom_bool(v->sql->sa, is_exists(sf)?1:0);
				if (depth == 0 && is_select(rel->op))
					le = exp_compare(v->sql->sa, le, exp_atom_bool(v->sql->sa, 1), cmp_equal);
				else if (exp_name(e))
					exp_prop_alias(v->sql->sa, le, e);
				v->changes++;
				return le;
			}

			sq = exp_rel_get_rel(v->sql->sa, ie); /* get subquery */

			le = rel_reduce2one_exp(v->sql, sq);
			le = exp_ref(v->sql, le);
			if (is_project(rel->op) && is_freevar(le)) {
				sql_exp *re, *jc, *null;

				re = rel_bound_exp(v->sql, sq);
				re = rel_project_add_exp(v->sql, sq, re);
				jc = rel_unop_(v->sql, NULL, re, "sys", "isnull", card_value);
				set_has_no_nil(jc);
				null = exp_null(v->sql->sa, exp_subtype(le));
				le = rel_nop_(v->sql, NULL, jc, null, le, NULL, "sys", "ifthenelse", card_value);
			}

			if (is_project(rel->op) || depth > 0 || is_outerjoin(rel->op)) {
				sql_subfunc *ea = NULL;
				sq = rel_groupby(v->sql, sq, NULL);

				if (exp_is_rel(ie))
					ie->l = sq;
				ea = sql_bind_func(v->sql, "sys", is_exists(sf)?"exist":"not_exist", exp_subtype(le), NULL, F_AGGR);
				le = exp_aggr1(v->sql->sa, le, ea, 0, 0, CARD_AGGR, 0);
				le = rel_groupby_add_aggr(v->sql, sq, le);
				if (rel_has_freevar(v->sql, sq))
					ne = le;

				if (exp_has_rel(ie)) {
					visitor iv = { .sql = v->sql };
					if (!rewrite_exp_rel(&iv, rel, ie, depth))
						return NULL;
				}

				if (is_project(rel->op) && rel_has_freevar(v->sql, sq))
					if (!(le = exp_exist(v->sql, le, ne, is_exists(sf))))
						return NULL;
				if (exp_name(e))
					exp_prop_alias(v->sql->sa, le, e);
				set_processed(sq);
				if (depth == 1 && is_ddl(rel->op)) { /* exists is at a ddl statment, it must be inside a relation */
					v->changes++;
					return exp_rel(v->sql, sq);
				}
			} else { /* rewrite into semi/anti join */
				operator_type op = is_exists(sf)?op_semi:op_anti;
				(void)rewrite_inner(v->sql, rel, sq, &op);
				v->changes++;
				return exp_compare(v->sql->sa, exp_atom_bool(v->sql->sa, 1), exp_atom_bool(v->sql->sa, 1), cmp_equal);
			}
			v->changes++;
			return le;
		}
	}
	return e;
}

/* exp visitor */
static sql_exp *
rewrite_ifthenelse(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void)depth;
	/* for ifthenelse and rank flatten referenced inner expressions */
	if (e->ref) {
		sql_rel *r = rel->l = rel_project(v->sql->sa, rel->l, rel_projections(v->sql, rel->l, NULL, 1, 1));

		e->ref = 0;
		set_processed(r);
		append(r->exps, e);
		v->changes++;
		return exp_ref(v->sql, e);
	}

	sql_subfunc *sf;
	if (e->type != e_func)
		return e;

	sf = e->f;
	/* TODO also handle ifthenelse with more than 3 arguments */
	if (is_ifthenelse_func(sf) && !list_empty(e->l) && list_length(e->l) == 3 && rel_has_freevar(v->sql, rel)) {
		list *l = e->l;

		/* remove unecessary = true expressions under ifthenelse */
		for (node *n = l->h ; n ; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_equal && exp_is_true(e) && exp_is_true(e->r))
				n->data = e->l;
		}

		sql_exp *cond = l->h->data;
		sql_exp *then_exp = l->h->next->data;
		sql_exp *else_exp = l->h->next->next->data;

		if (exp_has_rel(then_exp) || exp_has_rel(else_exp)) {
			bool single = false;
			//return sql_error(v->sql, 10, SQLSTATE(42000) "time to rewrite into union\n");
			// union(
			// 	select(
			// 		project [then]
			//	)[cond]
			// 	select(
			// 		project [else]
			//	)[not(cond)]
			//) [ cols ]
			sql_rel *lsq = NULL, *rsq = NULL, *usq = NULL;

			if (exp_has_rel(then_exp)) {
				lsq = exp_rel_get_rel(v->sql->sa, then_exp);
				then_exp = exp_rel_update_exp(v->sql, then_exp);
				if (is_single(lsq))
					single = true;
				reset_single(lsq);
			}
			exp_set_freevar(v->sql, then_exp, lsq);
			lsq = rel_project(v->sql->sa, lsq, append(sa_list(v->sql->sa), then_exp));
			exp_set_freevar(v->sql, cond, lsq);
			set_processed(lsq);
			lsq = rel_select(v->sql->sa, lsq, cond);
			if (exp_has_rel(else_exp)) {
				rsq = exp_rel_get_rel(v->sql->sa, else_exp);
				else_exp = exp_rel_update_exp(v->sql, else_exp);
				if (is_single(rsq))
					single = true;
				reset_single(rsq);
			}
			exp_set_freevar(v->sql, else_exp, rsq);
			rsq = rel_project(v->sql->sa, rsq, append(sa_list(v->sql->sa), else_exp));
			cond = exp_copy(v->sql, cond);
			exp_set_freevar(v->sql, cond, rsq);
			cond = rel_unop_(v->sql, rsq, cond, "sys", "not", card_value);
			set_processed(rsq);
			rsq = rel_select(v->sql->sa, rsq, cond);
			usq = rel_setop(v->sql->sa, lsq, rsq, op_union);
			rel_setop_set_exps(v->sql, usq,append(sa_list(v->sql->sa), exp_ref(v->sql, e)));
			if (single)
				set_single(usq);
			set_processed(usq);
			e = exp_rel(v->sql, usq);
			v->changes++;
		}
	}
	return e;
}

static list *
rewrite_compare_exps(visitor *v, sql_rel *rel, list *exps)
{
	if (list_empty(exps))
		return exps;
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (!is_compare(e->type)) {
			sql_subtype bt;
			sql_find_subtype(&bt, "boolean", 0, 0);
			if (!(e = exp_check_type(v->sql, &bt, rel, e, type_equal)))
				return NULL;
			n->data = e = exp_compare(v->sql->sa, e, exp_atom_bool(v->sql->sa, 1), cmp_equal);
			v->changes++;
		}
		if (is_compare(e->type) && e->flag == cmp_or) {
			if (!(e->l = rewrite_compare_exps(v, rel, e->l)))
				return NULL;
			if (!(e->r = rewrite_compare_exps(v, rel, e->r)))
				return NULL;
		}
	}
	return exps;
}

/* add an dummy true projection column */
static inline sql_rel *
rewrite_compare_exp(visitor *v, sql_rel *rel)
{
	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps))
		if (!(rel->exps = rewrite_compare_exps(v, rel, rel->exps)))
			return NULL;
	return rel;
}

static inline sql_rel *
rewrite_remove_xp_project(visitor *v, sql_rel *rel)
{
	if (rel->op == op_join && list_empty(rel->exps)) {
		sql_rel *r = rel->r;

		if (is_simple_project(r->op) && r->l && !project_unsafe(r, 1)) {
			sql_rel *rl = r->l;

			if (is_simple_project(rl->op) && !rl->l && list_length(rl->exps) == 1) {
				sql_exp *t = rl->exps->h->data;

				if (is_atom(t->type) && !exp_name(t)) { /* atom with out alias cannot be used later */
					rel = rel_project(v->sql->sa, rel->l, rel_projections(v->sql, rel->l, NULL, 1, 1));
					list_merge(rel->exps, r->exps, (fdup)NULL);
					set_processed(rel);
					v->changes++;
				}
			}
		}
	}
	return rel;
}

static inline sql_rel *
rewrite_remove_xp(visitor *v, sql_rel *rel)
{
	if (rel->op == op_join && list_empty(rel->exps)) {
		sql_rel *r = rel->r;

		if (is_simple_project(r->op) && !r->l && list_length(r->exps) == 1) {
			sql_exp *t = r->exps->h->data;

			if (is_atom(t->type) && !exp_name(t)) { /* atom with out alias cannot be used later */
				v->changes++;
				return rel->l;
			}
		}
	}
	return rel;
}

/* rel visitor */
static sql_rel *
rewrite_fix_count(visitor *v, sql_rel *rel)
{
	if (rel->op == op_left && !is_single(rel)) {
		int rel_changes = 0;
		sql_rel *r = rel->r;

		if (!is_rewrite_fix_count_used(r->used)) {
			/* TODO create an exp iterator */
			list *rexps = rel_projections(v->sql, r, NULL, 1, 1), *exps;

			for(node *n = rexps->h; n; n=n->next) {
				sql_exp *e = n->data, *ne;

				if (exp_is_count(e, r)) {
					/* rewrite count in subquery */
					list *args, *targs;
					sql_subfunc *isnil = sql_bind_func(v->sql, "sys", "isnull", exp_subtype(e), NULL, F_FUNC), *ifthen;

					rel_changes = 1;
					ne = exp_unop(v->sql->sa, e, isnil);
					set_has_no_nil(ne);
					targs = sa_list(v->sql->sa);
					append(targs, sql_bind_localtype("bit"));
					append(targs, exp_subtype(e));
					append(targs, exp_subtype(e));
					ifthen = sql_bind_func_(v->sql, "sys", "ifthenelse", targs, F_FUNC);
					args = sa_list(v->sql->sa);
					append(args, ne);
					append(args, exp_atom(v->sql->sa, atom_zero_value(v->sql->sa, exp_subtype(e))));
					append(args, e);
					ne = exp_op(v->sql->sa, args, ifthen);
					if (exp_name(e))
						exp_prop_alias(v->sql->sa, ne, e);
					n->data = ne;
				}
			}
			if (rel_changes) { /* add project */
				exps = list_merge(rel_projections(v->sql, rel->l, NULL, 1, 1), rexps, (fdup)NULL);
				rel = rel_project(v->sql->sa, rel, exps);
				set_processed(rel);
				r->used |= rewrite_fix_count_used;
				v->changes++;
			}
		}
	}
	return rel;
}

static inline sql_rel *
rewrite_groupings(visitor *v, sql_rel *rel)
{
	prop *found;

	if (is_groupby(rel->op)) {
		/* ROLLUP, CUBE, GROUPING SETS cases */
		if ((found = find_prop(rel->p, PROP_GROUPINGS))) {
			list *sets = (list*) found->value;
			sql_rel *unions = NULL;

			for (node *n = sets->h ; n ; n = n->next) {
				sql_rel *nrel;
				list *l = (list*) n->data, *exps = sa_list(v->sql->sa), *pexps = sa_list(v->sql->sa);

				l = list_flaten(l);
				nrel = rel_groupby(v->sql, unions ? rel_dup(rel->l) : rel->l, l);

				for (node *m = rel->exps->h ; m ; m = m->next) {
					sql_exp *e = (sql_exp*) m->data, *ne = NULL;
					sql_subfunc *agr = (sql_subfunc*) e->f;

					if (e->type == e_aggr && !agr->func->s && !strcmp(agr->func->base.name, "grouping")) {
						/* replace grouping aggregate calls with constants */
						sql_subtype tpe = ((sql_arg*) agr->func->res->h->data)->type;
						list *groups = (list*) e->l;
						atom *a = atom_int(v->sql->sa, &tpe, 0);
#ifdef HAVE_HGE
						hge counter = (hge) list_length(groups) - 1;
#else
						lng counter = (lng) list_length(groups) - 1;
#endif
						assert(groups && list_length(groups) > 0);

						for (node *nn = groups->h ; nn ; nn = nn->next) {
							sql_exp *exp = (sql_exp*) nn->data;
							if (!exps_find_exp(l, exp)) {
								switch (ATOMstorage(a->data.vtype)) {
									case TYPE_bte:
										a->data.val.btval += (bte) (1 << counter);
										break;
									case TYPE_sht:
										a->data.val.shval += (sht) (1 << counter);
										break;
									case TYPE_int:
										a->data.val.ival += (int) (1 << counter);
										break;
									case TYPE_lng:
										a->data.val.lval += (lng) (1 << counter);
										break;
#ifdef HAVE_HGE
									case TYPE_hge:
										a->data.val.hval += (hge) (1 << counter);
										break;
#endif
									default:
										assert(0);
								}
							}
							counter--;
						}

						ne = exp_atom(v->sql->sa, a);
						if (exp_name(e))
							exp_prop_alias(v->sql->sa, ne, e);
					} else if (e->type == e_column && !exps_find_exp(l, e) && !has_label(e)) {
						/* do not include in the output of the group by, but add to the project as null */
						ne = exp_atom(v->sql->sa, atom_general(v->sql->sa, exp_subtype(e), NULL));
						if (exp_name(e))
							exp_prop_alias(v->sql->sa, ne, e);
					} else {
						ne = exp_ref(v->sql, e);
						append(exps, e);
					}
					append(pexps, ne);
				}
				if (list_empty(exps))
					append(exps, exp_atom_bool(v->sql->sa, 1)); /* protection against empty projections */
				nrel->exps = exps;
				set_processed(nrel);
				if (list_empty(pexps))
					append(pexps, exp_atom_bool(v->sql->sa, 1)); /* protection against empty projections */
				nrel = rel_project(v->sql->sa, nrel, pexps);
				set_processed(nrel);

				if (!unions)
					unions = nrel;
				else {
					unions = rel_setop(v->sql->sa, unions, nrel, op_union);
					rel_setop_set_exps(v->sql, unions, rel_projections(v->sql, rel, NULL, 1, 1));
					set_processed(unions);
				}
				if (!unions)
					return unions;
			}
			v->changes++;
			return unions;
		} else {
			bool found_grouping = false;
			for (node *n = rel->exps->h ; n ; n = n->next) {
				sql_exp *e = (sql_exp*) n->data;
				sql_subfunc *agr = (sql_subfunc*) e->f;

				if (e->type == e_aggr && !agr->func->s && !strcmp(agr->func->base.name, "grouping")) {
					found_grouping = true;
					break;
				}
			}
			if (found_grouping) {
				/* replace grouping calls with constants of value 0 */
				sql_rel *nrel = rel_groupby(v->sql, rel->l, rel->r);
				list *exps = sa_list(v->sql->sa), *pexps = sa_list(v->sql->sa);
				sql_subtype *bt = sql_bind_localtype("bte");

				for (node *n = rel->exps->h ; n ; n = n->next) {
					sql_exp *e = (sql_exp*) n->data, *ne;
					sql_subfunc *agr = (sql_subfunc*) e->f;

					if (e->type == e_aggr && !agr->func->s && !strcmp(agr->func->base.name, "grouping")) {
						ne = exp_atom(v->sql->sa, atom_int(v->sql->sa, bt, 0));
						if (exp_name(e))
							exp_prop_alias(v->sql->sa, ne, e);
					} else {
						ne = exp_ref(v->sql, e);
						append(exps, e);
					}
					append(pexps, ne);
				}
				if (list_empty(exps))
					append(exps, exp_atom_bool(v->sql->sa, 1)); /* protection against empty projections */
				nrel->exps = exps;
				set_processed(nrel);
				v->changes++;
				if (list_empty(pexps))
					append(pexps, exp_atom_bool(v->sql->sa, 1)); /* protection against empty projections */
				nrel = rel_project(v->sql->sa, nrel, pexps);
				set_processed(nrel);
				return nrel;
			}
		}
	}
	return rel;
}

static int
include_tid(sql_rel *r)
{
	if (is_basetable(r->op))
		r->nrcols = list_length(r->exps);
	return r->nrcols;
}

static inline sql_rel *
rewrite_outer2inner_union(visitor *v, sql_rel *rel)
{
	if (is_outerjoin(rel->op) && !list_empty(rel->exps) && (((is_left(rel->op) || is_full(rel->op)) && rel_has_freevar(v->sql,rel->l)) ||
		((is_right(rel->op) || is_full(rel->op)) && rel_has_freevar(v->sql,rel->r)) || exps_have_freevar(v->sql, rel->exps) || exps_have_rel_exp(rel->exps))) {
		sql_exp *f = exp_atom_bool(v->sql->sa, 0);
		int nrcols = rel->nrcols;

		nrcols = include_tid(rel->l);
		nrcols += include_tid(rel->r);
		rel->nrcols = nrcols;
		if (is_left(rel->op)) {
			sql_rel *prel = rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 1, 1));
			sql_rel *except = rel_setop(v->sql->sa,
					rel_project(v->sql->sa, rel_dup(rel->l), rel_projections(v->sql, rel->l, NULL, 1, 1)),
					rel_project(v->sql->sa, rel_dup(prel), rel_projections(v->sql, rel->l, NULL, 1, 1)), op_except);
			rel_setop_set_exps(v->sql, except, rel_projections(v->sql, rel->l, NULL, 1, 1));
			set_processed(except);
			sql_rel *nrel = rel_crossproduct(v->sql->sa, except, rel_dup(rel->r),  op_left);
			rel_join_add_exp(v->sql->sa, nrel, f);
			rel->op = op_join;
			nrel = rel_setop(v->sql->sa,
					prel,
					rel_project(v->sql->sa, nrel, rel_projections(v->sql, nrel, NULL, 1, 1)),
					op_union);
			rel_setop_set_exps(v->sql, nrel, rel_projections(v->sql, rel, NULL, 1, 1));
			set_processed(nrel);
			v->changes++;
			return nrel;
		} else if (is_right(rel->op)) {
			sql_rel *prel = rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 1, 1));
			sql_rel *except = rel_setop(v->sql->sa,
					rel_project(v->sql->sa, rel_dup(rel->r), rel_projections(v->sql, rel->r, NULL, 1, 1)),
					rel_project(v->sql->sa, rel_dup(prel), rel_projections(v->sql, rel->r, NULL, 1, 1)), op_except);
			rel_setop_set_exps(v->sql, except, rel_projections(v->sql, rel->r, NULL, 1, 1));
			set_processed(except);
			sql_rel *nrel = rel_crossproduct(v->sql->sa, rel_dup(rel->l), except, op_right);
			rel_join_add_exp(v->sql->sa, nrel, f);
			rel->op = op_join;
			nrel = rel_setop(v->sql->sa,
					prel,
					rel_project(v->sql->sa, nrel, rel_projections(v->sql, nrel, NULL, 1, 1)),
					op_union);
			rel_setop_set_exps(v->sql, nrel, rel_projections(v->sql, rel, NULL, 1, 1));
			set_processed(nrel);
			v->changes++;
			return nrel;
		} else if (is_full(rel->op)) {
			sql_rel *prel = rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 1, 1));
			sql_rel *except = rel_setop(v->sql->sa,
					rel_project(v->sql->sa, rel_dup(rel->l), rel_projections(v->sql, rel->l, NULL, 1, 1)),
					rel_project(v->sql->sa, rel_dup(prel), rel_projections(v->sql, rel->l, NULL, 1, 1)), op_except);
			rel_setop_set_exps(v->sql, except, rel_projections(v->sql, rel->l, NULL, 1, 1));
			set_processed(except);
			sql_rel *lrel = rel_crossproduct(v->sql->sa, except, rel_dup(rel->r),  op_left);
			rel_join_add_exp(v->sql->sa, lrel, f);

			except = rel_setop(v->sql->sa,
					rel_project(v->sql->sa, rel_dup(rel->r), rel_projections(v->sql, rel->r, NULL, 1, 1)),
					rel_project(v->sql->sa, rel_dup(prel), rel_projections(v->sql, rel->r, NULL, 1, 1)), op_except);
			rel_setop_set_exps(v->sql, except, rel_projections(v->sql, rel->r, NULL, 1, 1));
			set_processed(except);
			sql_rel *rrel = rel_crossproduct(v->sql->sa, rel_dup(rel->l), except, op_right);
			rel_join_add_exp(v->sql->sa, rrel, f);
			lrel = rel_setop(v->sql->sa,
					rel_project(v->sql->sa, lrel,  rel_projections(v->sql, lrel, NULL, 1, 1)),
					rel_project(v->sql->sa, rrel, rel_projections(v->sql, rrel, NULL, 1, 1)),
					op_union);
			rel_setop_set_exps(v->sql, lrel, rel_projections(v->sql, rel, NULL, 1, 1));
			set_processed(lrel);
			rel->op = op_join;
			lrel = rel_setop(v->sql->sa,
					rel_project(v->sql->sa, prel,  rel_projections(v->sql, rel, NULL, 1, 1)),
					rel_project(v->sql->sa, lrel, rel_projections(v->sql, lrel, NULL, 1, 1)),
					op_union);
			rel_setop_set_exps(v->sql, lrel, rel_projections(v->sql, rel, NULL, 1, 1));
			set_processed(lrel);
			v->changes++;
			return lrel;
		}
	}
	return rel;
}

static sql_exp *
rewrite_complex(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	sql_exp *res = NULL;

	if (e->type != e_func)
		return e;

	res = rewrite_anyequal(v, rel, e, depth);
	if (!res || res != e)
		return res;
	res = rewrite_exists(v, rel, e, depth);
	if (!res || res != e)
		return res;
	res = rewrite_compare(v, rel, e, depth);
	if (!res || res != e)
		return res;
	return e;
}

/* rewrite project [ [multi values], [multi values2] , .. [] ] -> union ) */
static inline sql_rel *
rewrite_values(visitor *v, sql_rel *rel)
{
	int single = is_single(rel);
	if (!is_simple_project(rel->op) || list_empty(rel->exps))
		return rel;

	if (rel_is_ref(rel) && !is_rewrite_values_used(rel->used)) { /* need extra project */
		rel->l = rel_project(v->sql->sa, rel->l, rel->exps);
		rel->exps = rel_projections(v->sql, rel->l, NULL, 1, 1);
		((sql_rel*)rel->l)->r = rel->r; /* propagate order by exps */
		rel->r = NULL;
		rel->used |= rewrite_values_used;
		v->changes++;
		return rel;
	}
	sql_exp *e = rel->exps->h->data;

	if (!is_values(e) || list_length(exp_get_values(e))<=1 || (!exp_has_freevar(v->sql, e) && !exp_has_rel(e)))
		return rel;

	list *exps = sa_list(v->sql->sa);
	sql_rel *cur = NULL;
	list *vals = exp_get_values(e);
	if (vals) {
		for(int i = 0; i<list_length(vals); i++) {
			sql_rel *nrel = rel_project(v->sql->sa, NULL, sa_list(v->sql->sa));
			set_processed(nrel);
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				list *vals = exp_get_values(e);

				if (vals) {
					if (i == 0)
						append(exps, exp_ref(v->sql, e));
					sql_exp *v = list_fetch(vals, i);
					append(nrel->exps, v);
					rel_set_exps(nrel, nrel->exps);
				}
			}
			if (cur) {
				nrel = rel_setop(v->sql->sa, cur, nrel, op_union);
				rel_setop_set_exps(v->sql, nrel, exps);
				set_processed(nrel);
			}
			cur = nrel;
		}
		rel = cur;
		if (single)
			set_single(rel);
	}
	v->changes++;
	return rel;
}

/* add an dummy true projection column */
static sql_rel *
rel_unnest_simplify(visitor *v, sql_rel *rel)
{
	/* at rel_select.c explicit cross-products generate empty selects, if these are not used, they can be removed at rewrite_simplify */
	if (rel)
		rel = rewrite_basetable(v->sql, rel);	/* add proper exps lists */
	if (rel)
		rel = rewrite_empty_project(v, rel); /* remove empty project/groupby */
	if (rel)
		rel = rewrite_simplify(v, rel);
	if (rel)
		rel = rewrite_split_select_exps(v, rel); /* has to run before rewrite_complex */
	if (rel)
		rel = rewrite_outer2inner_union(v, rel); /* has to run before rewrite_or_exp */
	if (rel)
		rel = rewrite_or_exp(v, rel);
	if (rel)
		rel = rewrite_aggregates(v, rel);
	if (rel)
		rel = rewrite_values(v, rel);
	return rel;
}

static sql_rel *
rel_unnest_projects(visitor *v, sql_rel *rel)
{
	/* both rewrite_values and rewrite_fix_count use 'used' property from sql_rel, reset it, so make sure rewrite_reset_used is called first */
	if (rel)
		rel = rewrite_reset_used(v, rel);
	if (rel)
		rel = rewrite_remove_xp(v, rel);	/* remove crossproducts with project [ atom ] */
	if (rel)
		rel = rewrite_groupings(v, rel);	/* transform group combinations into union of group relations */
	return rel;
}

static sql_rel *
rel_unnest_comparison_rewriters(visitor *v, sql_rel *rel)
{
	if (rel)
		rel = rewrite_join2semi(v, rel);	/* where possible convert anyequal functions into marks */
	if (rel)
		rel = rewrite_compare_exp(v, rel);	/* only allow for e_cmp in selects and  handling */
	if (rel)
		rel = rewrite_remove_xp_project(v, rel);	/* remove crossproducts with project ( project [ atom ] ) [ etc ] */
	if (rel)
		rel = rewrite_simplify(v, rel);		/* as expressions got merged before, lets try to simplify again */
	return rel;
}

static sql_exp *
rel_simplify_exp_and_rank(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	if (e)
		e = rewrite_simplify_exp(v, rel, e, depth);
	if (e)
		e = rewrite_rank(v, rel, e, depth);
	return e;
}

sql_rel *
rel_unnest(mvc *sql, sql_rel *rel)
{
	int level = 0;
	visitor v = { .sql = sql, .data = &level }; /* make it compatible with rel_optimizer, so set the level to 0 */

	rel = rel_exp_visitor_bottomup(&v, rel, &rel_simplify_exp_and_rank, false);
	rel = rel_visitor_bottomup(&v, rel, &rel_unnest_simplify);
	rel = rel_visitor_bottomup(&v, rel, &not_anyequal_helper);

	rel = rel_exp_visitor_bottomup(&v, rel, &rewrite_complex, true);
	rel = rel_exp_visitor_bottomup(&v, rel, &rewrite_ifthenelse, false);	/* add isnull handling */
	rel = rel_exp_visitor_bottomup(&v, rel, &rewrite_exp_rel, true);

	rel = rel_visitor_bottomup(&v, rel, &rel_unnest_comparison_rewriters);
	rel = rel_visitor_bottomup(&v, rel, &_rel_unnest);
	rel = rel_visitor_bottomup(&v, rel, &rewrite_fix_count);	/* fix count inside a left join (adds a project (if (cnt IS null) then (0) else (cnt)) */
	rel = rel_visitor_bottomup(&v, rel, &rel_unnest_projects);
	rel = rel_exp_visitor_bottomup(&v, rel, &exp_reset_card_and_freevar_set_physical_type, false);
	return rel;
}
