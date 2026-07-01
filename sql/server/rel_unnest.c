/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "rel_optimizer_private.h"
#include "sql_decimal.h"
#include "rel_unnest.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_select.h"
#include "rel_rewriter.h"
#include "rel_optimizer.h"

#define rewrite_no_freevar       (1 << 8)
#define is_independent(X)        ((X & rewrite_no_freevar) == rewrite_no_freevar)
#define set_has_freevar(r)		 if (r) (r->used &= ~rewrite_no_freevar)

static void
exp_set_freevar(mvc *sql, sql_exp *e, sql_rel *r)
{
	set_has_freevar(r);
	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_filter) {
			exps_set_freevar(sql, e->l, r);
			exps_set_freevar(sql, e->r, r);
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			exps_set_freevar(sql, e->l, r);
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
		if (rel_find_nid(r, e->nid))
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
		if (e->flag == cmp_filter) {
			return (exps_have_freevar(sql, e->l) || exps_have_freevar(sql, e->r));
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			return exps_have_freevar(sql, e->l);
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
		if (e && ((vf =exp_has_freevar(sql, e)) != 0))
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
	assert(rel);
	if (!rel)
		return 0;
	if (is_independent(rel->used))
		return 0;

	int res = 0;
	if (is_basetable(rel->op)) {
		res = 0;
	} else if (is_base(rel->op)) {
		res = exps_have_freevar(sql, rel->exps) || (rel->l && rel_has_freevar(sql, rel->l));
	} else if (is_simple_project(rel->op) || is_groupby(rel->op) || is_select(rel->op) || is_topn(rel->op) || is_sample(rel->op)) {
		if ((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->r && exps_have_freevar(sql, rel->r))
			return 1;
		res = exps_have_freevar(sql, rel->exps) || (rel->l && rel_has_freevar(sql, rel->l));
	} else if (is_join(rel->op) || is_set(rel->op) || is_semi(rel->op) || is_modify(rel->op)) {
		res = exps_have_freevar(sql, rel->exps) ||
			(rel->l && rel_has_freevar(sql, rel->l)) || (rel->r && rel_has_freevar(sql, rel->r));
	} else if (is_munion(rel->op)) {
		int v = exps_have_freevar(sql, rel->exps);
		list *l = rel->l;
		for (node *n = l->h; n && !v; n = n->next)
			v = rel_has_freevar(sql, n->data);
		res = v;
	}
	if (res == 0)
		rel->used |= rewrite_no_freevar;
	return res;
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
		if (e->flag == cmp_filter) {
			exps_only_freevar(query, e->l, arguments_correlated, found_one_freevar, ungrouped_cols);
			exps_only_freevar(query, e->r, arguments_correlated, found_one_freevar, ungrouped_cols);
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			exps_only_freevar(query, e->l, arguments_correlated, found_one_freevar, ungrouped_cols);
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
		if (rel->card > CARD_ATOM)
			exps_only_freevar(query, rel->exps, arguments_correlated, found_one_freevar, ungrouped_cols);
		if (rel->l)
			rel_only_freevar(query, rel->l, arguments_correlated, found_one_freevar, ungrouped_cols);
	} else if (is_join(rel->op) || is_set(rel->op) || is_semi(rel->op) || is_modify(rel->op)) {
		exps_only_freevar(query, rel->exps, arguments_correlated, found_one_freevar, ungrouped_cols);
		rel_only_freevar(query, rel->l, arguments_correlated, found_one_freevar, ungrouped_cols);
		rel_only_freevar(query, rel->r, arguments_correlated, found_one_freevar, ungrouped_cols);
	} else if (is_munion(rel->op)) {
		exps_only_freevar(query, rel->exps, arguments_correlated, found_one_freevar, ungrouped_cols);
		list *l = rel->l;
		for (node *n = l->h; n; n = n->next)
			rel_only_freevar(query, n->data, arguments_correlated, found_one_freevar, ungrouped_cols);
	}
	return ;
}

static int
freevar_equal( sql_exp *e1, sql_exp *e2)
{
	assert(e1 && e2 && is_freevar(e1) && is_freevar(e2));
	if (e1 == e2)
		return 0;
	if (e1->type != e_column || e2->type != e_column)
		return -1;
	return (e1->nid != e2->nid);
}

static list *
merge_freevar(list *l, list *r, bool all)
{
	if (!l)
		return r;
	if (!r)
		return l;
	r  = list_join(l, r);
	if (all)
		return r;
	return list_distinct(r, (fcmp)freevar_equal, (fdup)NULL);
}

static list * exps_freevar(mvc *sql, list *exps, bool all);
static list * rel_freevar(mvc *sql, sql_rel *rel, sql_rel *ref);

static list *
exp_freevar(mvc *sql, sql_exp *e, bool all)
{
	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	switch(e->type) {
	case e_column:
		if (is_freevar(e))
			return append(sa_list(sql->sa), e);
		break;
	case e_convert:
		return exp_freevar(sql, e->l, all);
	case e_aggr:
	case e_func:
		if (e->l)
			return exps_freevar(sql, e->l, all);
		break;
	case e_cmp:
		if (e->flag == cmp_filter) {
			list *l = exps_freevar(sql, e->l, all);
			list *r = exps_freevar(sql, e->r, all);
			return merge_freevar(l, r, all);
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			return exps_freevar(sql, e->l, all);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			list *l = exp_freevar(sql, e->l, all);
			list *r = exps_freevar(sql, e->r, all);
			return merge_freevar(l, r, all);
		} else {
			list *l = exp_freevar(sql, e->l, all);
			list *r = exp_freevar(sql, e->r, all);
			l = merge_freevar(l, r, all);
			if (e->f) {
				r = exp_freevar(sql, e->f, all);
				return merge_freevar(l, r, all);
			}
			return l;
		}
		break;
	case e_psm:
		if (exp_is_rel(e))
			if (rel_has_freevar(sql, e->l))
				return rel_freevar(sql, e->l, NULL);
		return NULL;
	case e_atom:
		if (e->f)
			return exps_freevar(sql, e->f, all);
		return NULL;
	default:
		return NULL;
	}
	return NULL;
}

static list *
exps_freevar(mvc *sql, list *exps, bool all)
{
	node *n;
	list *c = NULL;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
	if (!exps)
		return NULL;
	for (n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		list *var = exp_freevar(sql, e, all);

		c = merge_freevar(c,var, all);
	}
	return c;
}

static list *
rel_freevar(mvc *sql, sql_rel *rel, sql_rel *ref)
{
	list *lexps = NULL, *rexps = NULL, *exps = NULL;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
	if (!rel || rel == ref)
		return NULL;
	if (is_independent(rel->used))
		return NULL;
	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		exps = exps_freevar(sql, rel->exps, false);
		lexps = rel_freevar(sql, rel->l, ref);
		rexps = rel_freevar(sql, rel->r, ref);
		lexps = merge_freevar(lexps, rexps, false);
		exps = merge_freevar(exps, lexps, false);
		return exps;

	case op_basetable:
		return NULL;
	case op_table: {
		sql_exp *call = rel->r;
		if (rel->flag != TRIGGER_WRAPPER && rel->l)
			lexps = rel_freevar(sql, rel->l, ref);
		exps = (rel->flag != TRIGGER_WRAPPER && call)?exps_freevar(sql, call->l, false):NULL;
		return merge_freevar(exps, lexps, false);
	}
	case op_except:
	case op_inter:
		exps = exps_freevar(sql, rel->exps, false);
		lexps = rel_freevar(sql, rel->l, ref);
		rexps = rel_freevar(sql, rel->r, ref);
		lexps = merge_freevar(lexps, rexps, false);
		exps = merge_freevar(exps, lexps, false);
		return exps;
	case op_munion:
		exps = exps_freevar(sql, rel->exps, false);
		for (node *n = ((list*)rel->l)->h; n; n = n->next) {
			lexps = rel_freevar(sql, n->data, ref);
			exps = merge_freevar(exps, lexps, false);
		}
		return exps;
	case op_ddl:
	case op_semi:
	case op_anti:

	case op_select:
	case op_topn:
	case op_sample:

	case op_groupby:
	case op_project:
		exps = exps_freevar(sql, rel->exps, false);
		lexps = rel_freevar(sql, rel->l, ref);
		if (rel->r) {
			if (is_groupby(rel->op) || is_simple_project(rel->op))
				rexps = exps_freevar(sql, rel->r, false);
			else
				rexps = rel_freevar(sql, rel->r, ref);
			lexps = merge_freevar(lexps, rexps, false);
		}
		exps = merge_freevar(exps, lexps, false);
		return exps;
	default:
		return NULL;
	}

}

static list *
rel_dependent_var(mvc *sql, sql_rel *l, sql_rel *r)
{
	/* somehow make sure we only flag the least common ancestor */
	list *res = NULL;

	if (rel_has_freevar(sql, r)){
		list *freevar = rel_freevar(sql, r, l);
		if (freevar) {
			node *n;
			list *boundvar = rel_projections(sql, l, NULL, 1, 0);

			for(n = freevar->h; n; n = n->next) {
				sql_exp *e = n->data, *ne = NULL;
				/* each freevar should be an e_column */
				ne = exps_bind_nid(boundvar, e->nid);
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

struct access_info {
	list *accessed_by;	/* list of relations using outer refs from this relation */
	list *outer_refs;	/* list of outer_refs */
};

static struct access_info *
access_merge(struct access_info *lai, struct access_info *rai)
{
	if (!rai)
		return lai;
	if (!lai)
		return rai;
	if (!lai->outer_refs)
		lai->outer_refs = rai->outer_refs;
	else if (rai->outer_refs) {
		lai->outer_refs = list_join(lai->outer_refs, rai->outer_refs);
		lai->outer_refs = list_distinct(lai->outer_refs, NULL, NULL);
	}
	if (!lai->accessed_by)
		lai->accessed_by = rai->accessed_by;
	else if (rai->accessed_by)
		lai->accessed_by = list_join(lai->accessed_by, rai->accessed_by);
	return lai;
}

static struct access_info *
access_append_outer(mvc *sql, struct access_info *acc, sql_exp *exp)
{
	if (!acc)
		acc = SA_ZNEW(sql->sa, struct access_info);
	if (!acc->outer_refs || !list_find(acc->outer_refs, exp, NULL))
		acc->outer_refs = sa_list_prepend(sql->sa, acc->outer_refs, exp);
	return acc;
}

static struct access_info *
access_append_accessed(mvc *sql, struct access_info *acc, sql_rel *rel)
{
	if (!acc)
		acc = SA_ZNEW(sql->sa, struct access_info);
	if (!acc->accessed_by || !list_find(acc->accessed_by, rel, NULL))
		acc->accessed_by = sa_list_prepend(sql->sa, acc->accessed_by, rel);
	return acc;
}

static bool exps_accessing_ad(mvc *sql, list *exps, list *ad, struct access_info **acc);

static sql_exp *
cexps_find_exp(list *exps, sql_exp *e)
{
	if (!exps)
		return NULL;
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *ne = n->data;
		if (e->nid == ne->alias.label)
			return ne;
	}
	return NULL;
}

static bool
exp_accessing_ad(mvc *sql, sql_exp *e, list *ad, struct access_info **acc)
{
	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	switch(e->type) {
	case e_column:
		if (is_freevar(e)) {
			sql_exp *ne = cexps_find_exp(ad, e);
			if (ne)
				*acc = access_append_outer(sql, *acc, ne);
			return ne != NULL;
		}
		break;
	case e_convert:
		return exp_accessing_ad(sql, e->l, ad, acc);
	case e_aggr:
	case e_func:
		if (e->l)
			return exps_accessing_ad(sql, e->l, ad, acc);
		break;
	case e_cmp:
		if (e->flag == cmp_filter) {
			bool fnd1 = exps_accessing_ad(sql, e->l, ad, acc);
			bool fnd2 = exps_accessing_ad(sql, e->r, ad, acc);
			if (fnd1 || fnd2)
				return true;
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			return exps_accessing_ad(sql, e->l, ad, acc);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			bool fnd1 = exp_accessing_ad(sql, e->l, ad, acc);
			bool fnd2 = exps_accessing_ad(sql, e->r, ad, acc);
			if (fnd1 || fnd2)
				return true;
		} else {
			bool fnd1 = exp_accessing_ad(sql, e->l, ad, acc);
			bool fnd2 = exp_accessing_ad(sql, e->r, ad, acc);
			bool fnd3 = (e->f && exp_accessing_ad(sql, e->f, ad, acc));
			if (fnd1 || fnd2 || fnd3)
				return true;
		}
		break;
	case e_psm:
		if (exp_is_rel(e))
			if (rel_has_freevar(sql, e->l))
				return rel_freevar(sql, e->l, NULL);
		break;
	case e_atom:
		if (e->f)
			return  exps_accessing_ad(sql, e->f, ad, acc);
		break;
	default:
		return false;
	}
	return false;
}

static bool
exps_accessing_ad(mvc *sql, list *exps, list *ad, struct access_info **acc)
{
	bool found = false;
	if (!list_empty(exps) && !list_empty(ad)) {
		for (node *n = exps->h; n; n = n->next) {
			if (exp_accessing_ad(sql, n->data, ad, acc))
				found = true;
		}
	}
	return found;
}

static bool
refs_find(list *refs, sql_exp *exp)
{
	if (!list_empty(refs)) {
		for (node *n = refs->h; n; n = n->next) {
			sql_exp *r = n->data;
			if (r->nid == exp->nid)
				return true;
		}
	}
	return false;
}

static struct access_info *
rel_accessing_ad(mvc *sql, sql_rel *rel, sql_rel *ref, list *ad)
{
	struct access_info *lrels = NULL, *rrels = NULL;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
	if (!rel || rel == ref)
		return NULL;
	if (is_independent(rel->used))
		return NULL;
	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:

	case op_semi:
	case op_anti:

	case op_except:
	case op_inter:
		if (is_dependent(rel)) {
			prop *p = find_prop(rel->p, PROP_UNNESTING);
			if (p) {
				struct access_info *ai = p->value.pval;
				list *nad = sa_list(sql->sa);
				for(node *n = ad->h; n; n = n->next) {
					if (!refs_find(ai->outer_refs, n->data)) {
						append(nad, n->data);
					}
				}
				ad = nad;
			}
		}
		lrels = rel_accessing_ad(sql, rel->l, ref, ad);
		rrels = rel_accessing_ad(sql, rel->r, ref, ad);
		lrels = access_merge(lrels, rrels);
		if (exps_accessing_ad(sql, rel->exps, ad, &lrels))
			lrels = access_append_accessed(sql, lrels, rel);
		return lrels;

	case op_basetable:
		return NULL;
	case op_table: {
		sql_exp *call = rel->r;
		if (rel->flag != TRIGGER_WRAPPER && rel->l)
			lrels = rel_accessing_ad(sql, rel->l, ref, ad);
		if (rel->flag != TRIGGER_WRAPPER && call && exps_accessing_ad(sql, rel->exps, ad, &lrels))
			lrels = access_append_accessed(sql, lrels, rel);
		return lrels;
	}
	case op_munion:
		for (node *n = ((list*)rel->l)->h; n; n = n->next) {
			rrels = rel_accessing_ad(sql, n->data, ref, ad);
			lrels = access_merge(lrels, rrels);
		}
		if (exps_accessing_ad(sql, rel->exps, ad, &lrels))
			lrels = access_append_accessed(sql, lrels, rel);
		return lrels;
	case op_ddl:

	case op_select:
	case op_topn:
	case op_sample:

	case op_groupby:
	case op_project:
		lrels = rel_accessing_ad(sql, rel->l, ref, ad);
		if (rel->r && exps_accessing_ad(sql, rel->r, ad, &lrels)) {
			lrels = access_append_accessed(sql, lrels, rel);
			return lrels;
		}
		if (exps_accessing_ad(sql, rel->exps, ad, &lrels))
			lrels = access_append_accessed(sql, lrels, rel);
		return lrels;
	default:
		return NULL;
	}
}

static struct access_info *
rel_accessing(mvc *sql, sql_rel *l, sql_rel *r)
{
	/* return list of sub-relations of r which are accessing expressions of l */
	struct access_info *res = NULL;

	if (rel_has_freevar(sql, r)){
		list *boundvar = rel_boundvar(sql, l);
		res = rel_accessing_ad(sql, r, l, boundvar);
	}
	return res;
}

/*
 * try to bind any freevar in the expression e
 */
void
rel_bind_var(mvc *sql, sql_rel *rel, sql_exp *e)
{
	list *fvs = exp_freevar(sql, e, true);

	if (fvs) {
		node *n;

		for(n = fvs->h; n; n=n->next) {
			sql_exp *e = n->data;

			if (is_freevar(e) && (exp_is_atom(e) || rel_find_exp(rel,e)))
				reset_freevar(e);
		}
	}
}

void
rel_bind_vars(mvc *sql, sql_rel *rel, list *exps)
{
	if (list_empty(exps))
		return;
	for(node *n=exps->h; n; n = n->next)
		rel_bind_var(sql, rel, n->data);
}

static sql_exp * push_up_project_exp(visitor *v, sql_rel *rel, sql_exp *e);

static list *
push_up_project_exps(visitor *v, sql_rel *rel, list *exps)
{
	node *n;

	if (!exps)
		return exps;

	for(n=exps->h; n; n=n->next) {
		sql_exp *e = n->data;

		n->data = push_up_project_exp(v, rel, e);
	}
	list_hash_clear(exps);
	return exps;
}

static sql_exp *
push_up_project_exp(visitor *v, sql_rel *rel, sql_exp *e)
{
	if (mvc_highwater(v->sql))
		return sql_error(v->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_filter) {
			e->l = push_up_project_exps(v, rel, e->l);
			e->r = push_up_project_exps(v, rel, e->r);
			return e;
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			e->l = push_up_project_exps(v, rel, e->l);
			return e;
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = push_up_project_exp(v, rel, e->l);
			e->r = push_up_project_exps(v, rel, e->r);
			return e;
		} else {
			e->l = push_up_project_exp(v, rel, e->l);
			e->r = push_up_project_exp(v, rel, e->r);
			if (e->f)
				e->f = push_up_project_exp(v, rel, e->f);
		}
		break;
	case e_convert:
		e->l = push_up_project_exp(v, rel, e->l);
		break;
	case e_func:
	case e_aggr:
		if (e->l)
			e->l = push_up_project_exps(v, rel, e->l);
		break;
	case e_column:
		{
			sql_exp *ne;

			/* include project or just lookup */
			assert(e->nid);
			ne = exps_bind_nid(rel->exps, e->nid);
			if (ne) {
				if (ne->type == e_column) {
					/* deref alias */
					e->l = ne->l;
					e->r = ne->r;
					e->nid = ne->nid;
					e->freevar = ne->freevar;
				} else {
					ne = exp_copy(v->sql, ne);
					return push_up_project_exp(v, rel, ne);
				}
			}
		} break;
	case e_atom:
		if (e->f)
			e->f = push_up_project_exps(v, rel, e->f);
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
		sql_subtype *bt = sql_fetch_localtype(TYPE_bit);
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
				df = sql_bind_func(sql, NULL, "diff", bt, exp_subtype(de), F_ANALYTIC, true, true);
				append(args, pe);
			} else {
				df = sql_bind_func(sql, NULL, "diff", exp_subtype(de), NULL, F_ANALYTIC, true, true);
			}
			assert(df);
			de = exp_copy(sql, de);
			set_freevar(de, 1);
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
	if (!e) {
		list *exps = rel_projections(sql, sq->l, NULL, 0, 1);
		e = exps->t->data;
	}
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

	if (rel && !list_empty(rel->exps)) {
		for(node *n = rel->exps->h; n; n = n->next){
			sql_exp *e = n->data;

			if (exp_is_atom(e))
				return e;
			if (!exp_has_freevar(sql, e))
				return exp_ref(sql, e);
		}
	}
	if (rel && is_project(rel->op)) /* add dummy expression */
		return rel_project_add_exp(sql, rel, exp_atom_bool(sql->sa, 1));
	return NULL;
}

static int
exp_is_count(sql_exp *e, sql_rel *rel)
{
	if (!e || !rel)
		return 0;
	if (is_alias(e->type) && is_project(rel->op) && !is_set(rel->op)) {
		/* if the relop is n-ary (like munion) we need to retrieve its
		 * first operands which lives in the list at rel->l
		 */
		sql_rel *pr = is_munion(rel->op) ? ((list*)rel->l)->h->data : rel->l;
		sql_exp *ne = rel_find_exp(pr, e);
		return exp_is_count(ne, pr);
	}
	if (is_aggr(e->type) && exp_aggr_is_count(e))
		return 1;
	return 0;
}

static bool
exps_have_rank(list *exps)
{
	if (!exps)
		return false;
	for(node *n=exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (is_analytic(e))
			return true;
	}
	return false;
}

static void exp_reset_props(sql_rel *rel, sql_exp *e, bool setnil);

static void
exps_reset_props(sql_rel *rel, list *exps, bool setnil)
{
	if (!list_empty(exps))
		for(node *n=exps->h; n; n=n->next)
			exp_reset_props(rel, n->data, setnil);
}

static void
exp_reset_props(sql_rel *rel, sql_exp *e, bool setnil)
{
	switch (e->type) {
	case e_column: {
		if (setnil && (((is_right(rel->op) || is_full(rel->op)) && rel_find_exp(rel->l, e) != NULL) ||
			((is_left(rel->op) || is_full(rel->op)) && rel_find_exp(rel->r, e) != NULL)))
			set_has_nil(e);
	} break;
	case e_convert: {
		exp_reset_props(rel, e->l, setnil);
		if (setnil && has_nil((sql_exp*)e->l))
			set_has_nil(e);
	} break;
	case e_func: {
		sql_subfunc *f = e->f;

		exps_reset_props(rel, e->l, setnil);
		if (setnil && !f->func->semantics && e->l && have_nil(e->l))
			set_has_nil(e);
	} break;
	case e_aggr: {
		sql_subfunc *a = e->f;

		exps_reset_props(rel, e->l, setnil);
		if (setnil && (a->func->s || strcmp(a->func->base.name, "count") != 0) && !a->func->semantics && !has_nil(e) && e->l && have_nil(e->l))
			set_has_nil(e);
	} break;
	case e_cmp: {
		if (e->flag == cmp_filter) {
			exps_reset_props(rel, e->l, setnil);
			exps_reset_props(rel, e->r, setnil);
			if (setnil && (have_nil(e->l) || have_nil(e->r)))
				set_has_nil(e);
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			exps_reset_props(rel, e->l, setnil);
			if (setnil && have_nil(e->l))
				set_has_nil(e);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			exp_reset_props(rel, e->l, setnil);
			exps_reset_props(rel, e->r, setnil);
			if (setnil && (has_nil((sql_exp*)e->l) || have_nil(e->r)))
				set_has_nil(e);
		} else {
			exp_reset_props(rel, e->l, setnil);
			exp_reset_props(rel, e->r, setnil);
			if (e->f)
				exp_reset_props(rel, e->f, setnil);
			if (setnil && !is_semantics(e) && (((sql_exp*)e->l) || has_nil((sql_exp*)e->r) || (e->f && has_nil((sql_exp*)e->f))))
				set_has_nil(e);
		}
	} break;
	default:
		break;
	}
	set_not_unique(e);
}

static sql_exp *
rewrite_inner(mvc *sql, sql_rel *rel, sql_rel *inner, operator_type op, sql_rel **rewrite, sql_exp *e)
{
	int single = is_single(inner);
	sql_rel *d = NULL;

	reset_single(inner);
	if (single && is_project(rel->op))
		op = op_left;

	if (is_join(rel->op)){
		if (e) {
			if (rel_rebind_exp(sql, rel->r, e)) {
				d = rel->r = rel_crossproduct(sql->sa, rel->r, inner, op);
			} else if (rel_rebind_exp(sql, rel->l, e)) {
				d = rel->l = rel_crossproduct(sql->sa, rel->l, inner, op);
			}
		}
		if (!d) {
			if (rel_has_freevar(sql, inner)) {
				list *rv = rel_dependent_var(sql, rel->r, inner);
				if (!list_empty(rv))
					d = rel->r = rel_crossproduct(sql->sa, rel->r, inner, op);
				else
					d = rel->l = rel_crossproduct(sql->sa, rel->l, inner, op);
			} else if (is_right(rel->op))
				d = rel->l = rel_crossproduct(sql->sa, rel->l, inner, op);
			else
				d = rel->r = rel_crossproduct(sql->sa, rel->r, inner, op);
		}
		if (single)
			set_single(d);
		set_processed(d);
	} else if (is_project(rel->op)){ /* projection -> op_left */
		if (rel->l || single || op == op_left) {
			if ((single || op == op_left) && !rel->l) {
				sql_exp *e = exp_atom_bool(sql->sa, 1);
				exp_label(sql->sa, e, ++sql->label);
				rel->l = rel_project(sql->sa, rel->l, list_append(sa_list(sql->sa), e));
			}
			d = rel->l = rel_crossproduct(sql->sa, rel->l, inner, op_left);
			if (single)
				set_single(d);
			set_processed(d);
		} else {
			d = rel->l = inner;
		}
	} else {
		d = rel->l = rel_crossproduct(sql->sa, rel->l, inner, op);
		if (single)
			set_single(d);
		set_processed(d);
	}
	assert(d);
	if (rel_has_freevar(sql, inner)) {
		list *dv = rel_dependent_var(sql, d, inner);
		list *fv = rel_freevar(sql, inner, d);
		/* check if the inner depends on the new join (d) or one level up */
		if (list_length(dv) && is_joinop(d->op))
			set_dependent(d);
		if (list_length(fv) != list_length(dv) && is_joinop(rel->op))
			set_dependent(rel);
	}
	if (rewrite)
		*rewrite = d;
	if (is_topn(inner->op))
		inner = inner->l;
	if (is_project(inner->op))
		return inner->exps->t->data;
	return NULL;
}

static sql_exp *
rewrite_exp_rel(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	if (exp_has_rel(e) && !is_ddl(rel->op)) {
		sql_rel *rewrite = NULL;
		sql_exp *ne = rewrite_inner(v->sql, rel, exp_rel_get_rel(v->sql->sa, e), depth?op_left:op_join, &rewrite, NULL);

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
			e = exp_rel_update_exp(v->sql, e, false);
		}
		exp_reset_props(rewrite, e, is_left(rewrite->op));
		v->changes++;
	}
	return e;
}

/* add an dummy true projection column and simplify project[project[true]] */
static inline sql_rel *
rewrite_empty_project(visitor *v, sql_rel *rel)
{
	if ((is_simple_project(rel->op) || is_groupby(rel->op)) && list_empty(rel->exps)) {
		sql_exp *e = exp_atom_bool(v->sql->sa, 1);

		exp_label(v->sql->sa, e, ++v->sql->label);
		list_append(rel->exps, e);
		v->changes++;
	}
	sql_rel *l = rel->l;
	if (l && !l->l && is_simple_project(rel->op) && is_simple_project(l->op) && list_length(l->exps) == 1) {
		list *exps = l->exps;
		sql_exp *e = exps->h->data;
		if (!e->f && exp_is_atom(e) && exp_is_true(e) && !exps_uses_exp(rel->exps, e) && !exps_have_rel_exp(rel->exps) && exps_are_atoms(rel->exps) && !exps_have_rank(rel->exps)) {
			rel_destroy(v->sql, l);
			rel->l = NULL;
			v->changes++;
		}
	}
	if (is_left(rel->op) && rel->attr) { /* group join */
		sql_rel *r = rel->r;
		if (!rel_is_ref(r) && is_simple_project(r->op) && r->l) {
			rel->exps = push_up_project_exps(v, r, rel->exps);
			rel->r = r->l;
			r->l = NULL;
			rel_destroy(v->sql, r);
			v->changes++;
		}
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

static sql_exp *
exp_atom_set_type_null(visitor *v, sql_exp *el, sql_subtype *t)
{
	if (el->type == e_atom && !el->f && !el->r) {
		sql_subtype *f = exp_subtype(el);
		if ((!f || f->type->eclass == EC_ANY) && !el->l) { /* parameter, set type, or return ERR?? */
			sql_arg *a = sql_bind_paramnr(v->sql, el->flag);
			if (!a->type.type || a->type.type->eclass == EC_ANY) {
				a->type = *t;
				el->tpe = a->type;
				v->changes++;
				return el;
			}
		} else if ((!f || f->type->eclass == EC_ANY) && el->l) { /* NULL? */
			atom *a = el->l;
			if (atom_null(a)) {
				atom_cast_inplace(v->sql->sa, a, t);
				el->tpe = *t;
				v->changes++;
				return el;
			}
		}
	}
	return NULL;
}

static inline sql_exp *
exp_physical_types(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void)rel;
	(void)depth;
	sql_exp *ne = e;

	if (!e || e->type != e_func || !e->l)
		return e;

	list *args = e->l;
	sql_subfunc *f = e->f;

	if (list_length(args) == 2) {
		/* multiplication and division on decimals */
		if (is_multiplication(f)) {
			sql_exp *le = args->h->data;
			sql_subtype *lt = exp_subtype(le);

			if (lt->type->eclass == EC_SEC || lt->type->eclass == EC_MONTH) {
				sql_exp *re = args->h->next->data;
				sql_subtype *rt = exp_subtype(re);

				if (rt->type->eclass == EC_DEC && rt->scale) {
					int scale = (int) rt->scale; /* shift with scale */
					sql_subtype *it = sql_fetch_localtype(lt->type->localtype);
					sql_subfunc *c = sql_bind_func(v->sql, "sys", "scale_down", lt, it, F_FUNC, true, true);

					if (!c) {
						TRC_CRITICAL(SQL_PARSER, "scale_down missing (%s)\n", lt->type->impl);
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
		} else if (is_division(f)) {
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
						sql_subtype *it = sql_fetch_localtype(lt->type->localtype);
						sql_subfunc *c = sql_bind_func(v->sql, "sys", "scale_up", lt, it, F_FUNC, true, true);

						if (!c) {
							TRC_CRITICAL(SQL_PARSER, "scale_up missing (%s)\n", lt->type->impl);
							return NULL;
						}
						atom *a = atom_int(v->sql->sa, it, val);
						ne = exp_binop(v->sql->sa, e, exp_atom(v->sql->sa, a), c);
					} else { /* EC_MONTH */
						sql_subtype *it = sql_fetch_localtype(rt->type->localtype);
						sql_subfunc *c = sql_bind_func(v->sql, "sys", "scale_down", rt, it, F_FUNC, true, true);

						if (!c) {
							TRC_CRITICAL(SQL_PARSER, "scale_down missing (%s)\n", lt->type->impl);
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
	case op_project: {
		switch(e->type) {
		case e_aggr:
		case e_func: {
			e->card = list_empty(e->l)?rel?rel->card:(unsigned)CARD_MULTI:exps_card(e->l);
		} break;
		case e_column: {
			sql_exp *le = NULL, *re = NULL;
			bool underjoinl = false, underjoinr = false;

			le = rel_find_exp_and_corresponding_rel(rel->l, e, false, NULL, &underjoinl);
			if (!is_simple_project(rel->op) && !is_inter(rel->op) && !is_except(rel->op) && !is_semi(rel->op) && rel->r) {
				re = rel_find_exp_and_corresponding_rel(rel->r, e, false, NULL, &underjoinr);
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
			if (e->flag == cmp_filter) {
				e->card = MAX(exps_card(e->l), exps_card(e->r));
			} else if (e->flag == cmp_con || e->flag == cmp_dis) {
				e->card = exps_card(e->l);
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
	case op_inter:
	case op_except:
	case op_munion: {
		e->card = CARD_MULTI;
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
	if (!is_mset(rel->op) && (!is_groupby(rel->op) || !list_empty(rel->r))) /* global groupings have atomic cardinality */
		rel->card = MAX(e->card, rel->card); /* the relation cardinality may get updated too */
	return e;
}

static sql_exp *
exp_set_type(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void)rel;
	(void)depth;
	if (!e)
		return e;

	if (e->type == e_atom && !e->f && !e->l && !e->r) {
		sql_subtype *t = exp_subtype(e);
		if ((!t || t->type->eclass == EC_ANY)) { /* parameter, set type, or return ERR?? */
			sql_arg *a = sql_bind_paramnr(v->sql, e->flag);
			if (!a->type.type)
				return sql_error(v->sql, 10, SQLSTATE(42000) "Could not determine type for argument number %d", e->flag+1);
			e->tpe = a->type;
		}
	} else if (e->type == e_cmp && !e->f) {
		if (e->flag == cmp_in || e->flag == cmp_notin) {
			sql_subtype *lt = exp_subtype(e->l);
			e->r = exps_check_type(v->sql, lt, e->r);
		} else if (e->flag == cmp_equal || e->flag == cmp_notequal) {
			sql_subtype *lt = exp_subtype(e->l);
			sql_subtype *rt = exp_subtype(e->r);
			if (!lt || lt->type->eclass == EC_ANY) {
				sql_exp *ne = exp_atom_set_type_null(v, e->l, rt);
				if (ne)
					e->l = ne;
			} else if (!rt || rt->type->eclass == EC_ANY) {
				sql_exp *ne = exp_atom_set_type_null(v, e->r, lt);
				if (ne)
					e->r = ne;
			}
		}
	} else if (e->type == e_convert && !e->f) {
		sql_exp *el = e->l;
		sql_subtype *t = exp_totype(e);
		if (el->type == e_atom && !el->f && !el->l && !el->r) {
			el = exp_atom_set_type_null(v, el, t);
			if (el) {
				exp_prop_alias(v->sql->sa, el, e);
				return el;
			}
		}
	}
	return e;
}

static sql_exp *
exp_set_type_as(mvc *sql, sql_exp *te, sql_exp *e)
{
	if (te->type == e_convert) {
		if (e->type == e_column)  {
				int label = e->alias.label;
				e = exp_convert(sql, e, exp_subtype(e), exp_subtype(te));
				e->alias.label = label;
		} else {
			e->tpe = *exp_subtype(te);
			if (e->l)
				e->l = atom_set_type(sql->sa, e->l, &e->tpe);
		}
	}
	return e;
}

static sql_rel *
rel_set_type(visitor *v, sql_rel *rel)
{
	if (!rel)
		return rel;
	if (is_project(rel->op) && rel->l) {
		bool clear_hash = false;
		if (is_set(rel->op)) {
			sql_rel *l = rel->l, *r = rel->r;
			list *exps = l->exps;
			while(exps) {
				for(node *n = exps->h, *m = rel->exps->h; n && m; n = n->next, m = m->next) {
					sql_exp *e = n->data;
					sql_subtype *t = exp_subtype(e);

					if (t && !t->type->localtype) {
						n->data = exp_set_type_as(v->sql, m->data, e);
						clear_hash = true;
					}
				}
				if (clear_hash)
					list_hash_clear(exps);
				if (exps != r->exps)
					exps = r->exps;
				else
					exps = NULL;
				clear_hash = false;
			}
		} else if (is_munion(rel->op)) {
			list *l = rel->l;
			for(node *m = l->h; m; m = m->next) {
				sql_rel *r = m->data;
				list *exps = r->exps;
				for(node *n = exps->h, *m = rel->exps->h; n && m; n = n->next, m = m->next) {
					sql_exp *e = n->data;
					sql_subtype *t = exp_subtype(e);

					if (t && !t->type->localtype) {
						n->data = exp_set_type_as(v->sql, m->data, e);
						clear_hash = true;
					}
				}
				if (clear_hash)
					list_hash_clear(exps);
			}
		} else if ((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->l) {
			list *exps = rel->exps;
			while(exps) {
				for(node *n = exps->h; n; n = n->next) {
					sql_exp *te = n->data;
					if (te->type == e_convert) {
						sql_exp *l = te->l;
						if (l->type == e_column) {
							sql_rel *sl = rel->l;
							sql_exp *e = rel_find_exp(sl, l);
							if (!e)
								continue;
							if (is_groupby(sl->op) && exp_equal(e, l) == 0) {
								sql_exp *e2 = list_find_exp(sl->r, l);
								if (e2) {
									e = e2;
								}
							}
							sql_subtype *t = exp_subtype(e);

							if (t && !t->type->localtype) {
								if (e && e->type == e_column) {
									sql_rel *l = rel->l;
									if (is_project(l->op)) {
										for(node *n = l->exps->h; n; n = n->next) {
											if (n->data == e) {
												int label = e->alias.label;
												n->data = e = exp_convert(v->sql, e, t, exp_subtype(te));
												e->alias.label = label;
												list_hash_clear(l->exps);
												break;
											}
										}
									}
								} else {
									e->tpe = *exp_subtype(te);
									if (e->l && e->type == e_atom)
										e->l = atom_set_type(v->sql->sa, e->l, &e->tpe);
								}
							}
						}
					} else if (te->type == e_atom && !te->f) {
						sql_subtype *t = exp_subtype(te);
						if (!t && !te->l && !te->r) { /* parameter, set type, or return ERR?? */
						assert(0);
							sql_arg *a = sql_bind_paramnr(v->sql, te->flag);
							if (!a->type.type)
								return sql_error(v->sql, 10, SQLSTATE(42000) "Could not determine type for argument number %d", te->flag+1);
							te->tpe = a->type;
						}
					}
				}
				if (is_groupby(rel->op) && exps != rel->r)
					exps = rel->r;
				else
					exps = NULL;
			}
		}
	}
	return rel;
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

				if (is_referenced_by(e2, e1) || !exp_equal(e1, e2))
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
		list *r = a->r;
		node *rn = r?r->h:NULL;

		while(args) {
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
						if (!e1->alias.label)
							e1 = exp_label(sql->sa, e1, ++sql->label);
						append(exps, e1);
					} else {
						found->ascending = e1->ascending;
						found->nulls_last = e1->nulls_last;
						e1 = found;
					}
					if (!e1->alias.label)
						e1 = exp_label(sql->sa, e1, ++sql->label);
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
			if (rn) {
				args = rn->data;
				rn = rn->next;
			} else {
				args = NULL;
			}
		}
	}
	if (clear_hash)
		list_hash_clear(aggrs);
	return aggrs;
}

/* make sure e_func expressions don't appear on groupby expression lists */
static sql_rel *
aggrs_split_funcs(mvc *sql, sql_rel *rel)
{
	if (!list_empty(rel->exps)) {
		list *projs = NULL;
		for (node *n = rel->exps->h; n;) {
			node *next = n->next;
			sql_exp *e = n->data;

			if (e->type == e_func || exps_find_exp(projs, e)) {
				if (!projs)
					projs = sa_list(sql->sa);
				list_append(projs, e);
				list_remove_node(rel->exps, NULL, n);
			}
			n = next;
		}
		if (!list_empty(projs)) {
			/* the grouping relation may have more than 1 reference, a replacement is needed */
			sql_rel *l = rel_dup_copy(sql->sa, rel);
			list *nexps = list_join(rel_projections(sql, l, NULL, 1, 1), projs);
			rel = rel_inplace_project(sql->sa, rel, l, nexps);
			rel->card = exps_card(nexps);
		}
	}
	return rel;
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

		if (e->type == e_func || (e->type == e_aggr && exps_complex(e->l)))
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
		if (list_empty(exps))
			return rel;
		rel->l = rel_project(v->sql->sa, rel->l, exps);
		rel = aggrs_split_funcs(v->sql, rel);
		v->changes++;
		return rel;
	}
	return rel;
}

static inline sql_rel *
rewrite_split_select_exps(visitor *v, sql_rel *rel)
{
	if (is_select(rel->op) && !list_empty(rel->exps)) {
		allocator *ta = MT_thread_getallocator();
		allocator_state ta_state = ma_open(ta);
		int i = 0;
		bool has_complex_exps = false, has_simple_exps = false, *complex_exps = SA_NEW_ARRAY(ta, bool, list_length(rel->exps));

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
			set_processed(nsel);
			v->changes++;
		}
		ma_close(&ta_state);
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

	if (!is_simple_project(rel->op) || e->type != e_func || list_length(e->r) < 2 /* e->r means window function */)
		return e;

	(void)depth;
	/* ranks/window functions only exist in the projection */
	list *l = e->l, *r = e->r, *gbe = r->h->data, *obe = r->h->next->data;

	int needed = (gbe || obe);
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
	if (gbe || obe) {
		allocator *ta = MT_thread_getallocator();
		allocator_state ta_state = ma_open(ta);
		int gbeoffset = list_length(gbe), i = 0, added = 0;
		int *pos = SA_NEW_ARRAY(ta, int, gbeoffset + list_length(obe));
		if (gbe) {
			for (i = 0 ; i < gbeoffset ; i++)
				pos[i] = i;
			for(node *n = gbe->h ; n ; n = n->next) {
				sql_exp *e = n->data;
				set_partitioning(e);
			}
		}

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
					/* disable sorting info (ie back to defaults) */
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

			if (is_partitioning(next))
				set_partitioning(ref);
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
		ma_close(&ta_state);

		/* move rank down add ref */
		if (!exp_name(e))
			e = exp_label(v->sql->sa, e, ++v->sql->label);
		sql_exp *e_copy = exp_copy(v->sql, e);
		append(rell->exps, e_copy);
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

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	for (node *n=vals->h; n; n = n->next) {
		sql_exp *ve = n->data, *r, *s;
		sql_rel *sq = NULL;
		int freevar = 0;

		exp_label(sql->sa, ve, ++sql->label); /* an alias is needed */
		if (exp_has_rel(ve)) {
			sq = exp_rel_get_rel(sql->sa, ve); /* get subquery */
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
			if (sq->card > CARD_ATOM && rel_has_freevar(sql, sq) && is_project(sq->op) &&
				!sq->l && sq->nrcols == 1 && is_values((sql_exp*)sq->exps->h->data)) {
				/* needs check on projection */
				sql_exp *vals = sq->exps->h->data;
				if (!(sq = rel_union_exps(sql, l, exp_get_values(vals), is_tuple)))
					return NULL;
			} else {
				if (sq->card == CARD_ATOM && rel_has_freevar(sql, sq) && is_project(sq->op) &&
					!sq->l && sq->nrcols == 1 && !is_values((sql_exp*)sq->exps->h->data)) {
					sq->l = rel_project_exp(sql, exp_atom_bool(sql->sa, 1));
				}
				if (rel_convert_types(sql, NULL, NULL, l, &ve, 1, type_equal) < 0)
					return NULL;
				/* flatten expressions */
				if (exp_has_rel(ve)) {
					ve = exp_rel_update_exp(sql, ve, false);
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
			list *urs = sa_list(sql->sa);
			urs = append(urs, u);
			urs = append(urs, sq);
			u = rel_setop_n_ary(sql->sa, urs, op_munion);
			rel_setop_n_ary_set_exps(sql, u, exps, false);
			set_distinct(u);
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
			ne = rel_binop_(sql, NULL, exp_copy(sql, *l), r, "sys", "=", card_value, true);
		else
			ne = rel_binop_(sql, NULL, exp_copy(sql, *l), r, "sys", "<>", card_value, true);
		if (!e) {
			e = ne;
		} else if (anyequal) {
			e = rel_binop_(sql, NULL, e, ne, "sys", "or", card_value, true);
		} else {
			e = rel_binop_(sql, NULL, e, ne, "sys", "and", card_value, true);
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
	assert(e->type == e_func);

	sql_subfunc *sf = e->f;
	if (is_ddl(rel->op))
		return e;
	if (is_anyequal_func(sf) && !list_empty(e->l)) {
		list *l = e->l;
		mvc *sql = v->sql;

		if (list_length(l) == 2) { /* input is a set */

			sql_exp *ile = l->h->data, *le, *re = l->h->next->data;
			sql_rel *lsq = NULL, *rsq = NULL;
			int is_tuple = 0;

			if (exp_has_rel(ile))
				lsq = exp_rel_get_rel(sql->sa, ile); /* get subquery */

			if (lsq)
				le = exp_rel_update_exp(sql, ile, false);
			else
				le = ile;

			if (is_values(le)) /* exp_values */
				is_tuple = 1;

			/* re should be a values list */
			if (!lsq && !is_tuple && is_values(re) && !exps_have_rel_exp(re->f)) { /* exp_values */
				list *vals = re->f;

				rel_bind_var(sql, rel->l, le);
				rel_bind_vars(sql, rel->l, vals);
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
			} else if (!lsq && !exps_have_rel_exp(re->f) && !is_tuple) {
				return e; /* leave as is, handled later */
			} else if (!lsq && re->f && !exps_have_rel_exp(re->f) && exps_are_atoms(re->f) && is_tuple) {
				return exp_in(sql->sa, le, re->f, is_anyequal(sf) ? cmp_in : cmp_notin);
			}

			if (is_atom(re->type) && re->f) { /* exp_values */
				/* flatten using unions */
				rsq = rel_union_exps(sql, &le, re->f, is_tuple);
				if (!rsq)
					return NULL;
				if (!is_tuple) {
					re = rsq->exps->t->data;

					if (rsq && lsq)
						exp_set_freevar(sql, re, rsq);
					if (!is_tuple && !is_freevar(re)) {
						assert(re->alias.label);
						list_hash_clear(rsq->exps);
						re = exp_ref(sql, re);
					} else if (has_label(re)) {
						re = exp_ref(sql, re);
					}
				}
				set_processed(rsq);
			}

			if (is_project(rel->op) || (is_select(rel->op) && depth > 0) || is_outerjoin(rel->op)) {
				/* we introduced extra selects */
				assert(is_project(rel->op) || is_select(rel->op) || is_outerjoin(rel->op));

				sql_rel *join = NULL;
				if (lsq) {
					(void)rewrite_inner(sql, rel, lsq, op_left, &join, NULL);
					exp_reset_props(join, le, is_left(join->op));
				}
				if (rsq) {
					(void)rewrite_inner(sql, rel, rsq, op_left, &join, le);
					exp_reset_props(join, re, is_left(join->op));
				}
				assert(join && is_join(join->op));
				if (join && !join->exps)
					join->exps = sa_list(sql->sa);
				bool use_any = 0;
				if (is_tuple) {
					list *t = le->f;
					int s1 = list_length(t), s2 = rsq?list_length(rsq->exps):0;

					/* find out list of right expression */
					if (s1 != s2)
						return sql_error(sql, 02, SQLSTATE(42000) "Subquery has too %s columns", (s2 < s1) ? "few" : "many");
					for (node *n = t->h, *m = rsq->exps->h; n && m; n = n->next, m = m->next ) {
						sql_exp *le = n->data;
						sql_exp *re = m->data;

						re = exp_ref(sql, re);

						sql_exp *inexp = exp_compare(v->sql->sa, le, re, cmp_equal);
						if (inexp)
							set_any(inexp);
						append(join->exps, inexp);
					}
					return sql_error(sql, 02, SQLSTATE(42000) "Tuple matching at projections not implemented in the backend yet");
				} else {
					use_any = true;
					sql_exp *inexp = exp_compare(v->sql->sa, le, re, cmp_equal);
					if (inexp)
						set_any(inexp);
					exp_set_freevar(sql, le, join);
					rel_bind_var(sql, join, inexp);
					append(join->exps, inexp);
					if (exp_has_freevar(v->sql, inexp) && is_join(rel->op))
						set_dependent(rel);
				}
				v->changes++;
				if (join) {
					if (!join->attr)
						join->attr = sa_list(sql->sa);
					sql_exp *a = exp_atom_bool(v->sql->sa, !use_any?1:is_anyequal(sf));
					if (!e->alias.label)
						a = exp_label(v->sql->sa, a, ++v->sql->label); /* unique name */
					else
						exp_setalias(a, e->alias.label, exp_relname(e), exp_name(e));
					re = exp_ref(sql, a);
					set_has_nil(re); /* outerjoins could have introduced nils */
					re->card = CARD_MULTI; /* mark as multi value, the real attribute is introduced later */
					append(join->attr, a);
					return re;
				}
				set_has_nil(le); /* outer joins could have introduced nils */
				set_has_nil(re); /* outer joins could have introduced nils */
				return exp_compare(v->sql->sa, le, re, is_anyequal(sf)?cmp_equal:cmp_notequal);
			} else {
				sql_rel *rewrite = NULL;
				if (lsq) {
					(void)rewrite_inner(sql, rel, lsq, op_left, &rewrite, NULL);
					exp_reset_props(rewrite, le, is_left(rewrite->op));
				}
				if (rsq) {
					operator_type op = is_anyequal(sf)?op_semi:op_anti;
					(void)rewrite_inner(sql, rel, rsq, op, &rewrite, NULL);
					exp_reset_props(rewrite, re, is_left(rewrite->op));
				}
				if (!rewrite)
					return NULL;
				sql_exp *inexp;
				if (is_tuple) {
					list *t = le->f;
					int s1 = list_length(t), s2 = list_length(rsq->exps);

					/* find out list of right expression */
					if (s1 != s2)
						return sql_error(sql, 02, SQLSTATE(42000) "Subquery has too %s columns", (s2 < s1) ? "few" : "many");
					if (!rewrite->exps)
						rewrite->exps = sa_list(sql->sa);
					for (node *n = t->h, *m = rsq->exps->h; n && m; n = n->next, m = m->next ) {
						append(rewrite->exps, inexp = exp_compare(sql->sa, n->data, exp_ref(sql, m->data), cmp_equal));
						if (inexp)
							set_any(inexp);
					}
					v->changes++;
					return exp_atom_bool(sql->sa, 1);
				} else {
					if (exp_has_freevar(sql, le))
						rel_bind_var(sql, rel, le);
					if (!rewrite)
						return NULL;
					if (!rewrite->exps)
						rewrite->exps = sa_list(sql->sa);
					append(rewrite->exps, inexp=exp_compare(sql->sa, le, exp_ref(sql, re), cmp_equal));
					if (inexp)
						set_any(inexp);
					v->changes++;
					return exp_atom_bool(sql->sa, 1);
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
	assert(e->type == e_func);

	if (is_ddl(rel->op))
		return e;

	sql_subfunc *sf = e->f;
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
			if (exp_has_rel(ile)) {
				depth += exp_rel_depth(ile);
				lsq = exp_rel_get_rel(v->sql->sa, ile); /* get subquery */
			}

			if (lsq)
				le = exp_rel_update_exp(v->sql, ile, false);
			else
				le = ile;

			if (exp_has_rel(re))
				rsq = exp_rel_get_rel(v->sql->sa, re); /* get subquery */
			if (rsq) {
				if (!lsq && is_simple_project(rsq->op) && !rsq->l) {
					sql_exp *ire = rsq->exps->h->data;
					if (!is_values(ire) && !is_values(le)) {
						re = ire;
						rsq = exp_rel_get_rel(v->sql->sa, re);
					} else
					if (is_values(ire) && list_length(ire->f) == 1 && !is_values(le)) {
						list *exps = ire->f;
						re = exps->h->data;
						rsq = exp_rel_get_rel(v->sql->sa, re);
					}
				}
				if (rsq)
					re = exp_rel_update_exp(v->sql, re, false);
			}

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
					return e;
					/*
					le = exp_compare_func(v->sql, le, re, op, 0);
					if (exp_name(e))
						exp_prop_alias(v->sql->sa, le, e);
					v->changes++;
					return le;
					*/
				}
			}
			if (!is_tuple && is_values(re) && !exps_have_rel_exp(re->f)) { /* exp_values */
				list *vals = re->f;

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
				set_processed(rsq);
			}

			int is_cnt = 0;
			if (rsq) {
				sql_exp *r = exps_bind_nid(rsq->exps, re->nid);
				is_cnt = exp_is_count(r, rsq);
			}
			if (is_project(rel->op) || depth > 0 || quantifier || is_cnt) {
				sql_rel *sq = lsq;

				assert(!is_tuple);

				if (!lsq)
					lsq = rel->l;
				if (sq) {
					sql_rel *rewrite = NULL;
					operator_type op = (depth||quantifier)?op_left:op_join;
					(void)rewrite_inner(v->sql, rel, sq, op, &rewrite, NULL);
					exp_reset_props(rewrite, le, is_left(rewrite->op));
					rel_bind_var(v->sql, rel, le);
				}
				if (quantifier) {
					sql_subfunc *a;
					if (rsq == NULL) {
						rsq = rel_project(v->sql->sa, NULL, sa_list(v->sql->sa));
						append(rsq->exps, re);
						re = exp_ref(v->sql, re);
					}
					rsq = rel_groupby(v->sql, rsq, NULL);
					a = sql_bind_func(v->sql, "sys", "null", exp_subtype(re), NULL, F_AGGR, true, true);
					rnull = exp_aggr1(v->sql->sa, re, a, 0, 1, CARD_AGGR, has_nil(re));
					rnull = rel_groupby_add_aggr(v->sql, rsq, rnull);

					if (is_notequal_func(sf))
						op = "=";
					if (op[0] == '<') {
						a = sql_bind_func(v->sql, "sys", (quantifier==1)?"max":"min", exp_subtype(re), NULL, F_AGGR, true, true);
					} else if (op[0] == '>') {
						a = sql_bind_func(v->sql, "sys", (quantifier==1)?"min":"max", exp_subtype(re), NULL, F_AGGR, true, true);
					} else /* (op[0] == '=')*/ /* only = ALL */ {
						a = sql_bind_func(v->sql, "sys", "all", exp_subtype(re), NULL, F_AGGR, true, true);
						is_cnt = 1;
					}
					re = exp_aggr1(v->sql->sa, re, a, 0, 1, CARD_AGGR, has_nil(re));
					re = rel_groupby_add_aggr(v->sql, rsq, re);
					set_processed(rsq);
				}
				if (rsq) {
					sql_rel *rewrite = NULL;
					operator_type op = ((!quantifier && depth > 0)||is_cnt||quantifier)?op_left:op_join;
					(void)rewrite_inner(v->sql, rel, rsq, op, &rewrite, NULL);
					exp_reset_props(rewrite, re, is_left(rewrite->op));
					rel_bind_var(v->sql, rel, re);
				}

				if (rel_convert_types(v->sql, NULL, NULL, &le, &re, 1, type_equal) < 0)
					return NULL;
				if (rnull) { /* complex compare operator */
					sql_exp *lnull = rel_unop_(v->sql, rel, le, "sys", "isnull", card_value);
					set_has_no_nil(lnull);
					sql_exp * le_copy = exp_copy(v->sql, le);
					sql_exp * re_copy = exp_copy(v->sql, re);
					le = exp_compare_func(v->sql, le_copy, re_copy, op, 0);
					sql_subfunc *f = sql_bind_func3(v->sql, "sys", (quantifier==1)?"any":"all", exp_subtype(le), exp_subtype(lnull), exp_subtype(rnull), F_FUNC, true);
					le = exp_op3(v->sql->sa, le, lnull, rnull, f);
					if (is_select(rel->op) && depth == 0) {
						le = exp_compare(v->sql->sa, le, exp_atom_bool(v->sql->sa, is_notequal_func(sf) ? 0 : 1), cmp_equal);
					} else if (is_notequal_func(sf)) {
						le = rel_unop_(v->sql, rel, le, "sys", "not", card_value);
					}
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
					sql_rel *rewrite = NULL;
					(void)rewrite_inner(v->sql, rel, lsq, op_join, &rewrite, NULL);
					exp_reset_props(rewrite, le, is_left(rewrite->op));
				}
				if (rsq) {
					sql_rel *rewrite = NULL;
					operator_type op = !is_tuple?op_join:is_anyequal(sf)?op_semi:op_anti;
					(void)rewrite_inner(v->sql, rel, rsq, op, &rewrite, NULL);
					exp_reset_props(rewrite, re, is_left(rewrite->op));
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
	if (mvc_highwater(v->sql))
		return sql_error(v->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (is_select(rel->op) && !list_empty(rel->exps)) {
		sql_rel *j = rel->l, *jl = j->l, *ojl = jl;
		int needed = 0, changed = 0;

		if (!j || (!is_join(j->op) && !is_semi(j->op)) || !list_empty(j->exps))
			return rel;
		/* if needed first push select exps down under the join */
		for (node *n = rel->exps->h; n;) {
			node *next = n->next;
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;

			if (is_func(e->type) && is_anyequal_func(sf)) {
				if (exp_card(e) > CARD_ATOM && rel_has_all_exps(jl, e->l, false)) {
					if (!is_select(jl->op) || rel_is_ref(jl))
						j->l = jl = rel_select(v->sql->sa, jl, NULL);
					rel_select_add_exp(v->sql->sa, jl, e);
					list_remove_node(rel->exps, NULL, n);
					changed = 1;
					v->changes++;
				} else {
					needed = 1;
				}
			}
			n = next;
		}
		if (ojl != jl)
			set_processed(jl);
		if (changed && !(j->l = rewrite_join2semi(v, j->l)))
			return NULL;
		if (!needed)
			return try_remove_empty_select(v, rel);
		if (!j->exps)
			j->exps = sa_list(v->sql->sa);
		list *sexps = sa_list(v->sql->sa);
		for (node *n = rel->exps->h; n; ) {
			node *next = n->next;
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;

			/* Any compare expression based only on the left side will be split into a
			 * select under the anti join.
			 */
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
							e = exp_compare(v->sql->sa, n->data, m->data, cmp_equal );
							append(j->exps, e);
						}
					} else {
						e = exp_compare(v->sql->sa, l, r, cmp_equal);
						if (e && j->op == op_anti)
							set_semantics(e);
						append(j->exps, e);
					}
				}
				list_remove_node(rel->exps, NULL, n);
			} else if (!rel_rebind_exp(v->sql, j->r, e) && j->op == op_anti) {
				append(sexps, e);
				list_remove_node(rel->exps, NULL, n);
			}
			n = next;
		}
		v->changes++;
		if (list_length(sexps)) {
			sql_rel *jl = j->l = rel_select(v->sql->sa, j->l, NULL);
			set_processed(jl);
			jl->exps = sexps;
		}
		rel = try_remove_empty_select(v, rel);
	}
	return rel;
}

/* exp visitor */
static sql_exp *
rewrite_exists(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	assert(e->type == e_func);

	sql_subfunc *sf = e->f;
	if (is_exists_func(sf) && !list_empty(e->l)) {
		list *l = e->l;

		if (list_length(l) == 1) { /* exp_values */
			sql_exp *ie = l->h->data, *le;
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
			/* number of expressions in set relations must match the children */
			if (!is_project(sq->op) || (is_set(sq->op) && list_length(sq->exps) > 1) || (is_simple_project(sq->op) && !list_empty(sq->r)))
				sq = rel_project(v->sql->sa, sq, rel_projections(v->sql, sq, NULL, 1, 1));
			if (!sq)
				return NULL;
			le = rel_reduce2one_exp(v->sql, sq);
			if (!le->alias.label)
				exp_label(v->sql->sa, le, ++v->sql->label);
			le = exp_ref(v->sql, le);

			if (depth >= 1 && is_ddl(rel->op)) { /* exists is at a ddl statement, it must be inside at least a relation */
				sq = rel_groupby(v->sql, sq, NULL);
				sql_subfunc *ea = sql_bind_func(v->sql, "sys", is_exists(sf)?"exist":"not_exist", exp_subtype(le), NULL, F_AGGR, true, true);
				le = rel_groupby_add_aggr(v->sql, sq, exp_aggr1(v->sql->sa, le, ea, 0, 0, CARD_AGGR, 0));
				return exp_rel(v->sql, sq);
			}
			if (is_project(rel->op) || depth > 0 || is_outerjoin(rel->op)) {
				sql_rel *rewrite = NULL;

				(void)rewrite_inner(v->sql, rel, sq, op_left, &rewrite, NULL);
				exp_reset_props(rewrite, le, is_left(rewrite->op));
				if (!rewrite)
					return NULL;
				if (rewrite && !rewrite->exps)
					rewrite->exps = sa_list(v->sql->sa);
				v->changes++;
				if (rewrite) {
					if (!rewrite->attr)
						rewrite->attr = sa_list(v->sql->sa);
					sql_exp *a = exp_atom_bool(v->sql->sa, is_exists(sf));
					set_no_nil(a);
					if (!e->alias.label)
						exp_label(v->sql->sa, a, ++v->sql->label);
					else
						exp_setalias(a, e->alias.label, exp_relname(e), exp_name(e));
					le = exp_ref(v->sql, a);
					le->card = CARD_MULTI; /* mark as multi value, the real attribute is introduced later */
					append(rewrite->attr, a);
					if ((is_project(rel->op) || depth))
						return le;
				}
				set_has_nil(le); /* outer joins could have introduced nils */
				return le;
			} else { /* rewrite into semi/anti join */
				(void)rewrite_inner(v->sql, rel, sq, is_exists(sf)?op_semi:op_anti, NULL, NULL);
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
	return e;
}

static list *
rewrite_compare_exps(visitor *v, sql_rel *rel, list *exps)
{
	if (mvc_highwater(v->sql))
		return sql_error(v->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
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
		if (is_compare(e->type) && e->flag == cmp_dis) {
			list *l = e->l;
			for (node *m = l->h; m; m = m->next) {
				sql_exp *ae = m->data;
				if (is_compare(ae->type) && !is_anti(ae) && ae->flag == cmp_con)
					if (!(ae->l = rewrite_compare_exps(v, rel, ae->l)))
						return NULL;
			}
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
	if (rel->op == op_join && list_empty(rel->exps) && !rel_is_ref(rel)) {
		sql_rel *r = rel->r;

		if (is_simple_project(r->op) && r->l && !project_unsafe(r, 1)) {
			sql_rel *rl = r->l;

			if (is_simple_project(rl->op) && !rl->l && list_length(rl->exps) == 1) {
				sql_exp *t = rl->exps->h->data;

				if (is_atom(t->type) && !exp_name(t)) { /* atom with out alias cannot be used later */
					sql_rel *nrel = rel->l;
					rel->l = NULL;
					rel_destroy(v->sql, rel);
					rel = rel_project(v->sql->sa, nrel, rel_projections(v->sql, nrel, NULL, 1, 1));
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
	if (rel->op == op_join && list_empty(rel->exps) && !rel_is_ref(rel)) {
		sql_rel *r = rel->r;

		if (is_simple_project(r->op) && !r->l && list_length(r->exps) == 1) {
			sql_exp *t = r->exps->h->data;

			if (is_atom(t->type) && !exp_name(t)) { /* atom with out alias cannot be used later */
				sql_rel *nrel = rel->l;
				rel->l = NULL;
				rel_destroy(v->sql, rel);
				rel = nrel;
				v->changes++;
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
			list *rexps = r->exps, *exps = NULL;

			if (!rexps)
				rexps = rel_projections(v->sql, r, NULL, 1, 1);
			for(node *n = rexps->h; n && !rel_changes; n=n->next) {
				sql_exp *e = n->data;

				if (exp_is_count(e, r))
					rel_changes = 1;
			}
			if (!rel_changes)
				return rel;

			exps = rel_projections(v->sql, r, NULL, 1, 1);
			for(node *n = rexps->h, *m = exps->h; n && m; n=n->next, m=m->next) {
				sql_exp *e = n->data, *ne = m->data;

				if (exp_is_count(e, r)) {
					/* rewrite count in subquery */
					list *args, *targs;
					sql_subfunc *isnil = sql_bind_func(v->sql, "sys", "isnull", exp_subtype(ne), NULL, F_FUNC, true, true), *ifthen;

					ne = exp_unop(v->sql->sa, ne, isnil);
					e = exp_ref(v->sql, e);
					set_has_no_nil(ne);
					targs = sa_list(v->sql->sa);
					append(targs, sql_fetch_localtype(TYPE_bit));
					append(targs, exp_subtype(e));
					append(targs, exp_subtype(e));
					ifthen = sql_bind_func_(v->sql, "sys", "ifthenelse", targs, F_FUNC, true, true);
					args = sa_list(v->sql->sa);
					append(args, ne);
					append(args, exp_atom(v->sql->sa, atom_zero_value(v->sql->sa, exp_subtype(e))));
					append(args, e);
					ne = exp_op(v->sql->sa, args, ifthen);
					if (exp_name(e))
						exp_prop_alias(v->sql->sa, ne, e);
					m->data = ne;
				}
			}
			exps = list_join(rel_projections(v->sql, rel->l, NULL, 1, 1), exps);
			if (!list_empty(rel->attr))
				exps = append(exps, exp_ref(v->sql, rel->attr->h->data));
			rel = rel_project(v->sql->sa, rel, exps);
			set_processed(rel);
			r->used |= rewrite_fix_count_used;
			v->changes++;
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
			list *sets = (list*) found->value.pval;
			list *grpr = sa_list(v->sql->sa);

			rel->p = prop_remove(v->sql->sa, rel->p, found); /* remove property */
			for (node *n = sets->h ; n ; n = n->next) {
				sql_rel *nrel;
				list *l = (list*) n->data, *exps = sa_list(v->sql->sa), *pexps = sa_list(v->sql->sa);

				l = list_flatten(l);
				nrel = rel_groupby(v->sql, rel_dup(rel->l), l);

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
						ne = exp_atom(v->sql->sa, atom_general(v->sql->sa, exp_subtype(e), NULL, 0));
						if (exp_name(e))
							exp_prop_alias(v->sql->sa, ne, e);
					} else {
						sql_exp *ec = exp_copy(v->sql, e);
						ne = exp_ref(v->sql, ec);
						append(exps, ec);
					}
					append(pexps, ne);
				}
				if (list_empty(exps)) {
					sql_exp *e = exp_atom_bool(v->sql->sa, 1);
					exp_label(v->sql->sa, e, ++v->sql->label); /* protection against empty projections */
					list_append(exps, e);
				}
				nrel->exps = exps;
				if (!list_empty(rel->r) && !list_empty(nrel->r)) { /* aliases on grouping columns, ugh */
					for (node *n = ((list*)nrel->r)->h; n; n = n->next) {
						sql_exp *e = n->data;
						sql_exp *ne = exps_bind_nid(rel->r, e->alias.label);
						if (ne)
							n->data = exp_copy(v->sql, ne);
					}
					list_hash_clear(nrel->r);
				}
				set_processed(nrel);
				if (list_empty(pexps)) {
					sql_exp *e = exp_atom_bool(v->sql->sa, 1);
					exp_label(v->sql->sa, e, ++v->sql->label); /* protection against empty projections */
					list_append(pexps, e);
				}
				nrel = rel_project(v->sql->sa, nrel, pexps);
				set_processed(nrel);
				grpr = append(grpr, nrel);
			}

			/* always do relation inplace, so it will be fine when the input group has more than 1 reference */
			assert(list_length(grpr) > 0);
			if (list_length(grpr) == 0) {
				return NULL;
			} else if (list_length(grpr) == 1) {
				sql_rel *grp = grpr->h->data;
				rel = rel_inplace_project(v->sql->sa, rel, grp, grp->exps);
			} else {
				rel = rel_inplace_setop_n_ary(v->sql, rel, grpr, op_munion, rel_projections(v->sql, rel, NULL, 1, 1));
			}

			v->changes++;
			return rel;
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
				sql_rel *nrel = rel_groupby(v->sql, rel_dup(rel->l), rel->r);
				list *exps = sa_list(v->sql->sa), *pexps = sa_list(v->sql->sa);
				sql_subtype *bt = sql_fetch_localtype(TYPE_bte);

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
				if (list_empty(exps)) {
					sql_exp *e = exp_atom_bool(v->sql->sa, 1);
					exp_label(v->sql->sa, e, ++v->sql->label); /* protection against empty projections */
					list_append(exps, e);
				}
				nrel->exps = exps;
				set_processed(nrel);
				if (list_empty(pexps)) {
					sql_exp *e = exp_atom_bool(v->sql->sa, 1);
					exp_label(v->sql->sa, e, ++v->sql->label); /* protection against empty projections */
					list_append(pexps, e);
				}
				/* always do relation inplace, so it will be fine when the input group has more than 1 reference */
				rel = rel_inplace_project(v->sql->sa, rel, nrel, pexps);
				rel->card = exps_card(pexps);
				v->changes++;
				return rel;
			}
		}
	}
	return rel;
}

static sql_rel *
rewrite_swap_fullouter(visitor *v, sql_rel *rel)
{
	if (is_full(rel->op) && rel_has_freevar(v->sql, rel->r)) { /* swap */
		sql_rel *s = rel->r;
		rel->r = rel->l;
		rel->l = s;
	}
	return rel;
}

static sql_exp *
rewrite_complex(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	if (e->type != e_func)
		return e;

	sql_exp *res = rewrite_anyequal(v, rel, e, depth);
	if (res == e)
		res = rewrite_exists(v, rel, e, depth);
	if (res == e)
		res = rewrite_compare(v, rel, e, depth);
	return res;
}

static sql_rel *
flatten_values(visitor *v, sql_rel *rel)
{
	list *exps = sa_list(v->sql->sa);
	sql_exp *e = rel->exps->h->data;
	sql_rel *nrel = NULL;
	list *vals = exp_get_values(e);
	if (vals) {
		list *urs = sa_list(v->sql->sa);
		for(int i = 0; i<list_length(vals); i++) {
			nrel = rel_project(v->sql->sa, NULL, sa_list(v->sql->sa));
			set_processed(nrel);
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				list *vals = exp_get_values(e);

				if (vals) {
					if (i == 0)
						append(exps, exp_ref(v->sql, e));
					sql_exp *val = list_fetch(vals, i);
					exp_setalias(val, e->alias.label, exp_relname(e), exp_name(e));
					sql_exp *e_copy = exp_copy(v->sql, val);
					append(nrel->exps, e_copy);
				}
			}
			rel_set_exps(nrel, nrel->exps);
			urs = append(urs, nrel);
		}

		if (list_length(urs) == 1) {
			if (is_single(rel))
				set_single(nrel);
			rel_destroy(v->sql, rel);
			rel = nrel;
		} else {
			nrel = rel_setop_n_ary(v->sql->sa, urs, op_munion);
			rel_setop_n_ary_set_exps(v->sql, nrel, exps, false);
			if (is_single(rel))
				set_single(nrel);
			rel_destroy(v->sql, rel);
			rel = nrel;
		}

		v->changes++;
	}
	return rel;
}

/* rewrite project [ [multi values], [multi values2] , .. [] ] -> union ) */
static inline sql_rel *
rewrite_values(visitor *v, sql_rel *rel)
{
	if (!is_simple_project(rel->op) || list_empty(rel->exps) || is_rewrite_values_used(rel->used))
		return rel;

	sql_exp *e = rel->exps->h->data;
	if (!is_values(e) || (!exps_have_rel_exp(rel->exps) && !exps_have_freevar(v->sql, rel->exps)))
		return rel;
	if (rel_is_ref(rel)) { /* need extra project */
		rel->l = rel_project(v->sql->sa, rel->l, rel->exps);
		rel->exps = rel_projections(v->sql, rel->l, NULL, 1, 1);
		((sql_rel*)rel->l)->r = rel->r; /* propagate order by exps */
		rel->r = NULL;
		rel->used |= rewrite_values_used;
		v->changes++;
		return rel;
	}
	return flatten_values(v, rel);
}

typedef struct sql_args {
	list *args;
	list *exps;
} sql_args;

static int
var_name_cmp(sql_arg *v, char *name)
{
	return strcmp(v->name, name);
}

static sql_exp *
exp_inline_arg(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void)rel;
	(void)depth;
	sql_args *args = v->data;
	if (e->type == e_atom && e->r) {
		sql_arg *a = e->r;
		int level = is_freevar(e);
		node *n = list_find(args->args, a->name, (fcmp)&var_name_cmp);
		if (n) {
			sql_exp *val = list_fetch(args->exps, list_position(args->args, n->data));
			val = exp_copy(v->sql, val);
			exp_prop_alias(v->sql->sa, val, e);
			if (level)
				set_freevar(val, level-1);
			return val;
		}
	}
	return e;
}

static sql_rel *
rel_inline_table_func(visitor *v, sql_rel *rel)
{
	if (!rel_is_ref(rel) && rel->op == op_table && !rel->l && rel->r) { /* TODO add input relation (rel->l) rewriting */
		sql_exp *opf = rel->r;
		if (opf->type == e_func) {
			sql_subfunc *f = opf->f;

			if (f->func->vararg || f->func->varres)
				return rel;

			if (f->func->lang == FUNC_LANG_SQL && f->func->type == F_UNION) {
				sql_rel *r = rel_parse(v->sql, f->func->s, f->func->query, m_instantiate);

				if (r && is_ddl(r->op) && list_length(r->exps) == 1) {
					sql_exp *psm = r->exps->h->data;
					if (psm && psm->type == e_psm && psm->flag == PSM_RETURN) {
						sql_exp *ret = psm->l;
						if (ret && ret->type == e_psm && ret->flag == PSM_REL) {
							r = ret->l;
							list *exps = r->exps;
							r = rel_project(v->sql->sa, r, sa_list(v->sql->sa));
							for(node *n = rel->exps->h, *m = exps->h; n && m; n = n->next, m = m->next) {
								sql_exp *e = m->data;
								sql_exp *pe = n->data;

								if (!e->alias.label)
									e = exp_label(v->sql->sa, e, ++v->sql->label);
								e = exp_ref(v->sql, e);
								exp_setalias(e, pe->alias.label, exp_relname(pe), exp_name(pe));
								if (is_freevar(pe))
									set_freevar(e, is_freevar(pe)-1);
								append(r->exps, e);
							}
							sql_args a;
							visitor vv = *v;
							if (f->func->ops) {
								a.args = f->func->ops;
								a.exps = opf->l;
								vv.data = &a;
								r = rel_exp_visitor_topdown(&vv, r, &exp_inline_arg, true);
								v->data = NULL;
							}
							r = rel_unnest(v->sql, r);
							return r;
						}
					}
				}
			}
		}
	}
	return rel;
}

/* add an dummy true projection column */
static sql_rel *
rel_unnest_simplify(visitor *v, sql_rel *rel)
{
	/* at rel_select.c explicit cross-products generate empty selects, if these are not used, they can be removed at rewrite_simplify */
	if (rel && v->sql->emode != m_deps)
		rel = rel_inline_table_func(v, rel);
	if (rel)
		rel = rewrite_basetable(v->sql, rel, true);	/* add proper exps lists */
	if (rel)
		rel = rewrite_empty_project(v, rel); /* remove empty project/groupby */
	if (rel)
		rel = rewrite_simplify(v, 0, false, rel);
	if (rel)
		rel = rewrite_split_select_exps(v, rel); /* has to run before rewrite_complex */
	if (rel)
		rel = rewrite_swap_fullouter(v, rel);
	if (rel)
		rel = rewrite_aggregates(v, rel);
	if (rel)
		rel = rewrite_values(v, rel);
	return rel;
}

static sql_rel *
rel_unnest_projects(visitor *v, sql_rel *rel)
{
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
		rel = rewrite_simplify(v, 0, false, rel);		/* as expressions got merged before, lets try to simplify again */
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

static sql_rel *
rel_collect_non_trivial_dependent_joins(visitor *v, sql_rel *rel)
{
	/* also keep lists of outer refs */
	list *refs = v->data;
	assert(!is_dependent(rel) || is_join(rel->op) || is_semi(rel->op));
	bool found = false;
	if (!list_empty(refs)) {
		for(node *n = refs->h; n; n = n->next->next) {
			if (n->data == rel) {
				found = true;
				break;
			}
		}
	}
	if (!found && is_dependent(rel) && (is_join(rel->op) || is_semi(rel->op))) {

		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		struct access_info *accessing = rel_accessing(v->sql, l, r);
		if (!accessing) {
			if (rel_has_freevar(v->sql, l) && is_innerjoin(rel->op) && !rel->exps) { /* flip left and right */
				rel->l = r;
				rel->r = l;
				l = rel->l;
				r = rel->r;
				accessing = rel_accessing(v->sql, l, r);
			}
		}
		if (accessing) {
			list_prepend(refs, v->parent);
			prop *ap = rel->p = prop_create(v->sql->sa, PROP_UNNESTING, rel->p);
			ap->value.pval = accessing;
			list_prepend(refs, rel);
			sql_rel *p = v->parent;
			if (p && is_project(p->op) && p->l != rel)
				assert(0);
		} else {
			reset_dependent(rel);
		}
	}
	return rel;
}

struct unnesting_info {
	sql_rel *join;
	list *outer_refs;
	sql_rel *d;
	list *refs;
};

struct unnesting {
	struct unnesting_info *info;
	list *cclasses;		/* map of equivalant columns (only use if reduction by join isn't selective) */
	list *repr; /* map outer 2 inner col names */
	struct unnesting *parent;
	bool right;
};

static bool
path_is_linear(sql_rel *p, sql_rel *a)
{
	/* lca == join or semi */
	if ((is_select(p->op) || (is_simple_project(p->op) && p->l && !exps_have_rank(p->exps))) && p == a)
		return true;
	if (is_simple_project(p->op) && p->l && !exps_have_rank(p->exps))
		return path_is_linear(p->l, a);
	return false;
}

static void
rel_update_subrel(sql_rel *parent, sql_rel *old, sql_rel *new)
{
	if (parent->l == old) {
		parent->l = new;
	} else if (parent->r == old) {
		parent->r = new;
	} else if (is_munion(parent->op)) {
		list *rels = parent->l;
		for(node *n = rels->h; n; n = n->next) {
			sql_rel *orel = n->data;
			if (orel == old) {
				n->data = new;
				break;
			}
		}
	}
}

static void
djoin_push_up_project(visitor *v, sql_rel *prel, sql_rel *dj, sql_rel *accessor, sql_rel **nprel, sql_rel **changed_maps)
{
	/* push project(A(l) U A(r)) */
	if (is_join(dj->op) && list_empty(dj->attr)) {
		sql_rel *project = dj->r;
		while (project != accessor) {
			list *nexps = rel_projections(v->sql, dj->l, NULL, 1, 1);
			list *rexps = project->exps;
			sql_exp *id = NULL;
			for (node *n = rexps->h; n; n = n->next) {
				sql_exp *e = n->data;
				bool free = false;
				if ((free=exp_has_freevar(v->sql, e)))
					rel_bind_var(v->sql, dj->l, e);
				if (is_left(dj->op) && ((project->l == accessor && free) || exp_is_atom(e))) { /* for left outer return NULL for most inner project on misses */
					/* if id is NULL then NULL else e */
					sql_subtype *tp = exp_subtype(e);
					if (!tp)
						continue;
					if (!id) {
						if (is_select(accessor->op) || is_join(accessor->op) ||
								is_topn(accessor->op) || is_sample(accessor->op)) {
							id = rel_bound_exp(v->sql, accessor);
						} else {
							id = exp_ref(v->sql, accessor->exps->h->data);
						}
						set_has_nil(id);
					}
					sql_exp *nid = rel_unop_(v->sql, NULL, exp_ref(v->sql, id), "sys", "isnull", card_value);
					set_has_no_nil(nid);
					sql_exp *ne = rel_nop_(v->sql, NULL, nid, exp_null(v->sql->sa, tp), e, NULL, "sys", "ifthenelse", card_value);
					exp_prop_alias(v->sql->sa, ne, e);
					n->data = ne;
				}
			}
			dj->exps = push_up_project_exps(v, project, dj->exps);
			nexps = list_join(nexps, rexps);
			sql_rel *next = project->l;
			if (!*changed_maps)
				*changed_maps = project;
			project->l = dj;
			project->exps = nexps;
			rel_update_subrel(prel, dj, project);
			*nprel = prel = project;
			project = next;
		}
		dj->r = project;
	} else { /* semijoin */
		sql_rel *project = dj->r;
		while (project != accessor) {
			dj->exps = push_up_project_exps(v, project, dj->exps);
			sql_rel *old = project;
			project = project->l;
			old->l = NULL;
			rel_destroy(v->sql, old);
		}
		dj->r = project;
	}
}

static void
djoin_push_up_select(visitor *v, sql_rel *prel, sql_rel *dj, sql_rel *accessor, sql_rel **nprel, sql_rel **changed_maps)
{
	list *nexps = sa_list(v->sql->sa);
	node *nn = NULL;
	for (node *n = accessor->exps->h; n; n = nn) {
		nn = n->next;
		sql_exp *e = n->data;
		if (exp_has_freevar(v->sql, e)) {
			rel_bind_var(v->sql, dj->l, e);
			nexps = append(nexps, e);
			list_remove_node(accessor->exps, NULL, n);
		}
	}
	if (dj->exps)
		dj->exps = list_join(dj->exps, nexps);
	else
		dj->exps = nexps;
	if (dj->r != accessor) { /* any projects in between join and select */
		/* push project(A(l) U A(r)) */
		djoin_push_up_project(v, prel, dj, accessor, nprel, changed_maps);
	}
}

static bool
rel_simple_djoin_elim(visitor *v, sql_rel *prel, sql_rel *rel, sql_rel **changed_maps)
{
	prop *p = find_prop(rel->p, PROP_UNNESTING);
	assert(p);
	struct access_info *ai = p?p->value.pval:NULL;
	if (list_empty(ai->accessed_by)) {
		assert(!is_dependent(rel));
		return false;
	}
	for (node *n = ai->accessed_by->h; n; ) {
		sql_rel *accessor = n->data;
		node *nxt = n->next;
		if (path_is_linear(rel->r, accessor)) {
			if (rel_is_ref(accessor))
				break;
			sql_rel *cmaps = NULL, *nprel = prel;
			if (is_select(accessor->op) && (is_innerjoin(rel->op) || is_left(rel->op) || is_semi(rel->op))) {
				djoin_push_up_select(v, prel, rel, accessor, &nprel, &cmaps);
				list_remove_node(ai->accessed_by, NULL, n);
			}
			if (is_simple_project(accessor->op) && (is_innerjoin(rel->op) || is_left(rel->op) || is_semi(rel->op))) {
				djoin_push_up_project(v, prel, rel, accessor->l, &nprel, &cmaps);
				list_remove_node(ai->accessed_by, NULL, n);
				if (nprel)
					prel = nprel;
			}
			if (!*changed_maps)
				*changed_maps = cmaps;
		} else {
			break;
		}
		n = nxt;
	}
	if (list_empty(ai->accessed_by)) {
		reset_dependent(rel);
		return true;
	}
	return false;
}

static sql_exp*
repr_find(visitor *v, list *repr_map, sql_exp *exp)
{
	if (!repr_map) {
	printf("not found\n");
		return exp;
	}
	for(node *n = repr_map->h; n; n = n->next->next) {
		sql_exp *outer = n->data;
		sql_exp *nexp = NULL;
		if (outer == exp || outer->alias.label == exp->nid || outer->alias.label == exp->alias.label) {
			nexp =  n->next->data;
			return exp_ref(v->sql, nexp);
		}
	}
	return exp;
}

static list *
repr_replace(visitor *v, list *repr_map, sql_exp *exp, sql_exp *nexp)
{
	bool found = false;
	if (!repr_map)
		repr_map = sa_list(v->sql->sa);
	for(node *n = repr_map->h; n && !found; n = n->next->next) {
		sql_exp *outer = n->data;
		if (outer == exp || outer->alias.label == exp->nid || outer->alias.label == exp->alias.label) {
			found = true;
			n->next->data = nexp;
		}
	}
	if (!found) {
		append(repr_map, exp);
		append(repr_map, nexp);
	}
	return repr_map;
}

static void
unnesting_merge(struct unnesting *res, struct unnesting *lun, struct unnesting *run)
{
	if (!res->repr) {
		res->repr = lun->repr;
		if (!res->repr)
			res->repr = run->repr;
	}
}

static list * rewrite_columns(visitor *v, list *exps, struct unnesting *info);

static sql_exp *
rewrite_column(visitor *v, sql_exp *e, struct unnesting *info)
{
	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_filter) {
			e->l = rewrite_columns(v, e->l, info);
			e->r = rewrite_columns(v, e->r, info);
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			e->l = rewrite_columns(v, e->l, info);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = rewrite_column(v, e->l, info);
			e->r = rewrite_columns(v, e->r, info);
		} else {
			e->l = rewrite_column(v, e->l, info);
			e->r = rewrite_column(v, e->r, info);
			if (e->f)
				e->f = rewrite_column(v, e->f, info);
		}
		break;
	case e_convert:
		e->l = rewrite_column(v, e->l, info);
		break;
	case e_func:
	case e_aggr:
		if (e->l)
			e->l = rewrite_columns(v, e->l, info);
		break;
	case e_column:
		{
			sql_exp *ne = repr_find(v, info->repr, e);
			if (ne && ne != e && ne->alias.label > 0) {
				if (e->freevar && e->alias.label == e->nid)
					e->alias = ne->alias;
				e->nid = ne->alias.label;
				e->freevar = 0;
			}
		}
		break;
	case e_atom:
		if (e->f)
			e->f = rewrite_columns(v, e->f, info);
		break;
	case e_psm:
		break;
	}
	return e;
}

static list *
rewrite_columns(visitor *v, list *exps, struct unnesting *info)
{
	if (list_empty(info->repr) || list_empty(exps))
		return exps;
	list *nexps = sa_list(v->sql->sa);
	/* for any access replace by info->repr[access] */
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = rewrite_column(v, n->data, info);
		append(nexps, e);
	}
	return nexps;
}

static list *
rewrite_columns_for_join(visitor *v, list *exps, struct unnesting *linfo, struct unnesting *rinfo)
{
	if (list_empty(linfo->repr) && list_empty(rinfo->repr))
		return exps;
	if (list_empty(exps))
		return exps;
	/* rewrite based on inner side (left outer use linfo->repr), (right use rinfo->repr) full use
	 * coalesce(linfo.repr/rinfo.repr) */
	return rewrite_columns(v, exps, rinfo);
}

static list *
add_outers(visitor *v, list *exps, struct unnesting *info, bool partition)
{
	list *outers = sa_list(v->sql->sa);
	for (node *n = info->info->outer_refs->h; n; n = n->next) {
		sql_exp *exp = repr_find(v, info->repr, n->data);
		if (partition)
			set_partitioning(exp);
		append(outers, exp);
	}
	return list_join(outers, exps);
}

static void
rewrite_columns_groupings(visitor *v, sql_rel *rel, struct unnesting *info)
{
	prop *found;

	if ((found = find_prop(rel->p, PROP_GROUPINGS))) {
		list *sets = (list*) found->value.pval;
		for(node *n = sets->h; n; n = n->next) {
			list *l = n->data;
			list *outers = add_outers(v, sa_list(v->sql->sa), info, false);
			l = list_prepend(l, outers);
			for (node *m = l->h->next; m; m = m ->next) {
				m->data = rewrite_columns(v, m->data, info);
			}
		}
	}
}

static bool rel_djoin_elim(visitor *v, sql_rel *prel, sql_rel *rel, struct unnesting *parent, list *parent_accessing, list *refs);

static bool
accessed(sql_rel *rel, sql_rel *accessed_by)
{
	if (!rel || is_basetable(rel->op))
		return false;
	if (rel == accessed_by)
		return true;
	if (rel->op == op_munion) {
		list *l = rel->l;
		for (node *n = l->h; n; n = n->next)
			if (accessed(n->data, accessed_by))
				return true;
	} else {
		if (accessed(rel->l, accessed_by))
				return true;
		if (is_join(rel->op) || is_semi(rel->op) || is_set(rel->op))
			if (accessed(rel->r, accessed_by))
				return true;
	}
	return false;
}

static list *
accessing(visitor *v, sql_rel *rel, list *acc)
{
	if (!acc || list_empty(acc))
		return NULL;
	list *res = sa_list(v->sql->sa);
	for(node *n = acc->h; n; n = n->next) {
		if (accessed(rel, n->data))
			append(res, n->data);
	}
	return res;
}

static void
add_outers_repr(visitor *v, sql_rel *d, struct unnesting *info, bool relabel)
{
	node *n = d->exps->h, *m = info->info->outer_refs->h;
	int nr_outers = d->nr_outers, nr_inner = list_length(info->info->outer_refs) - nr_outers;

	assert(nr_inner >= 0);

	for (; m && nr_inner && nr_outers; m = m->next, nr_inner--) {
		sql_exp *new = exp_copy(v->sql, m->data);
		sql_exp *old = m->data;
		info->repr = sa_list_append(v->sql->sa, info->repr, old);
		info->repr = sa_list_append(v->sql->sa, info->repr, new);
		if (relabel)
			exp_label(v->sql->sa, new, ++v->sql->label);
	}
	for (; n && m; n = n->next, m = m->next) {
		sql_exp *new = relabel ? n->data : exp_copy(v->sql, n->data);
		sql_exp *old = m->data;
		info->repr = sa_list_append(v->sql->sa, info->repr, old);
		info->repr = sa_list_append(v->sql->sa, info->repr, new);
		if (relabel)
			exp_label(v->sql->sa, new, ++v->sql->label);
	}
}

static void
add_outer_join_exps(visitor *v, sql_rel *rel, struct unnesting *linfo, struct unnesting *rinfo)
{
	/* add natural join predicates */
	for (node *n = rinfo->info->outer_refs->h; n; n = n->next) {
		sql_exp *exp = n->data;
		sql_exp *lexp = linfo ? repr_find(v, linfo->repr, exp) : exp_ref(v->sql, exp);
		sql_exp *rexp = repr_find(v, rinfo->repr, exp);
		sql_exp *cmp = exp_compare(v->sql->sa, lexp, rexp, cmp_equal);
		set_semantics(cmp);
		rel->exps = sa_list_append(v->sql->sa, rel->exps, cmp);
	}
}

static void
unnest(visitor *v, sql_rel *parent, sql_rel * rel, struct unnesting *info, list *acc)
{
	if (!rel)
		return ;

	if (list_empty(acc) && !rel_is_ref(rel) && (!is_simple_project(rel->op) || rel->l)) { /* unnest ends add join with d */
		sql_rel *d = rel_project(v->sql->sa, rel_dup(info->info->d), rel_projections(v->sql, info->info->d, NULL, 1, 1));
		add_outers_repr(v, d, info, true);
		sql_rel *nrel = rel_crossproduct(v->sql->sa, d, rel, is_right(info->info->join->op) ? info->info->join->op : op_join);
		rel_update_subrel(parent, rel, nrel);
		return;
	}
	if (rel_is_ref(rel) && is_unnest_used(rel->used)) {
		if (/*is_recursive(rel) &&*/ info->right) {
			sql_rel *nrel = rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 0, 1));
			nrel->nr_outers = rel->nr_outers;
			rel_update_subrel(parent, rel, nrel);
			rel = nrel;
		}
		add_outers_repr(v, rel, info, info->right);
		return ;
	}
	rel->used |= unnest_used;
	if (list_empty(acc) && rel_is_ref(rel)) { /* unnest of ref needs inplace join with d */
		sql_rel *d = rel_project(v->sql->sa, rel_dup(info->info->d), rel_projections(v->sql, info->info->d, NULL, 1, 1));
		add_outers_repr(v, d, info, true);
	   	list *exps = list_merge(rel_projections(v->sql, d, NULL, 0, 1), rel_projections(v->sql, rel, NULL, 0, 1), NULL);
	  	if (!rel_in_rel(info->info->d, rel)) {
			rel = rel_inplace_project(v->sql->sa, rel, NULL, exps);
			rel->nr_outers = list_length(info->info->outer_refs);
			rel->l = rel_crossproduct(v->sql->sa, d, rel->l,  is_right(info->info->join->op) ? info->info->join->op : op_join);
		} else {
			sql_rel *nrel = rel_crossproduct(v->sql->sa, d, rel,  is_right(info->info->join->op) ? info->info->join->op : op_join);
			nrel = rel_project(v->sql->sa, nrel, exps);
			nrel->nr_outers = list_length(info->info->outer_refs);
			rel_update_subrel(parent, rel, nrel);
			rel = nrel;
		}
		return;
	}

	/* handle projection of freevar without lower relation */
	if (is_simple_project(rel->op) && !rel->l) {
		if (is_right(info->info->join->op)) {
			sql_rel *d = rel_project(v->sql->sa, rel_dup(info->info->d), rel_projections(v->sql, info->info->d, NULL, 1, 1));
			add_outers_repr(v, d, info, true);
			sql_rel *nrel = rel_crossproduct(v->sql->sa, d, rel, is_right(info->info->join->op) ? info->info->join->op : op_join);
			rel_update_subrel(parent, rel, nrel);
			return;
		}
		sql_rel *d = rel->l = rel_project(v->sql->sa, rel_dup(info->info->d), rel_projections(v->sql, info->info->d, NULL, 1, 1));
		add_outers_repr(v, d, info, true);
		rel->exps = add_outers(v, rel->exps, info, false);
		rel->nr_outers = list_length(info->info->outer_refs);
		rel->exps = rewrite_columns(v, rel->exps, info);
		return;
	}
	node *n = list_find(acc, rel, NULL);
	if (n && !is_join(rel->op) && !is_semi(rel->op))
		list_remove_node(acc, NULL, n);
	if (is_select(rel->op)) {
		/* add equivalences from select predicates to info->cclasses */
		unnest(v, rel, rel->l, info, acc);
		rel->exps = rewrite_columns(v, rel->exps, info);
	} else if (is_simple_project(rel->op)) {
		unnest(v, rel, rel->l, info, acc);
		/* for window functions add the info->outerrefs too the partition by list */
		for(node *n = rel->exps->h; n; n = n->next) {
			sql_exp *aexp = n->data;
			n->data = aexp = exp_rewrite(v->sql, rel, aexp, info->info->outer_refs);
		}
		rel->exps = add_outers(v, rel->exps, info, false);
		rel->nr_outers = list_length(info->info->outer_refs);
		rel->exps = rewrite_columns(v, rel->exps, info);
		if (rel->r) {
			rel->r = add_outers(v, rel->r, info, true);
			rel->r = rewrite_columns(v, rel->r, info);
		}
	} else if (is_topn(rel->op) || is_sample(rel->op)) {
		unnest(v, rel, rel->l, info, acc);
		if (is_topn(rel->op)) {
			sql_rel *or = rel->l;
			assert(is_simple_project(or->op));
			if (!or->r)
				or->r = add_outers(v, or->r, info, true);
		}
		rel->grouped = 1;
	} else if (is_munion(rel->op) && is_recursive(rel)) {
		list *rels = rel->l;
		assert(list_length(rels) == 2);
		sql_rel *base = rels->h->data;
		sql_rel *iter = rels->h->next->data;
		struct unnesting base_unnest = { .info = info->info };
		list *base_acc = accessing(v, base, acc);
		bool add_base = (list_empty(base_acc));
		unnest(v, rel, base, &base_unnest, base_acc);
		set_recursive(base);
		list *iter_acc = accessing(v, iter, acc);
		if (add_base) /* make sure we unnest up till the base */
			append(iter_acc, base);
		unnest(v, rel, iter, info, iter_acc);
		rel->exps = add_outers(v, rel->exps, info, false);
		rel->nr_outers = list_length(info->info->outer_refs);
		rel->exps = rewrite_columns(v, rel->exps, info);
	} else if (is_munion(rel->op)) {
		list *rels = rel->l;
		for (node *n = rels->h; n; n = n->next) {
			sql_rel *inner = n->data;
			list *iacc = accessing(v, inner, acc);
			info->repr = NULL;
			unnest(v, rel, inner, info, iacc);
			inner = n->data;
			if (!is_project(inner->op)) {
				sql_rel *ninner = rel_project(v->sql->sa, inner, rel_projections(v->sql, inner, NULL, 1, 1));
				rel_update_subrel(rel, inner, ninner);
			}
		}
		rel->exps = add_outers(v, rel->exps, info, false);
		rel->nr_outers = list_length(info->info->outer_refs);
		rel->exps = rewrite_columns(v, rel->exps, info);
	} else if (is_set(rel->op)) {
		list *lacc = accessing(v, rel->l, acc);
		unnest(v, rel, rel->l, info, lacc);
			info->repr = NULL;
		list *racc = accessing(v, rel->r, acc);
		unnest(v, rel, rel->r, info, racc);
		rel->exps = add_outers(v, rel->exps, info, false);
		rel->nr_outers = list_length(info->info->outer_refs);
		rel->exps = rewrite_columns(v, rel->exps, info);
	} else if (is_groupby(rel->op)) {
		bool no_groups = !rel->r; /* or for cubes if groupingsets has empty case */
		unnest(v, rel, rel->l, info, acc);
		list *gexps = rel->r = add_outers(v, rel->r, info, false);
		gexps = rel->r = rewrite_columns(v, rel->r, info);
		rel->exps = add_outers(v, rel->exps, info, false);
		rel->nr_outers = list_length(info->info->outer_refs);
		rel->exps = rewrite_columns(v, rel->exps, info);
		if (rel->p)
			rewrite_columns_groupings(v, rel, info);
		if (no_groups) {
			/* change info->D left-outer-(group)join rel (on outerRefs) */
			/* for now first create left-outer-join(d,rel) */
			sql_rel *d = rel_project(v->sql->sa, rel_dup(info->info->d), rel_projections(v->sql, info->info->d, NULL, 1, 1));
			sql_rel *nrel = rel_crossproduct(v->sql->sa, d, rel,  op_left);
			rel_update_subrel(parent, rel, nrel);
			/* outer_join_exps  */
			for (node *n = d->exps->h, *m = gexps->h, *o = info->info->outer_refs->h;
				n && m && o; n = n->next, m = m->next, o = o->next) {
				sql_exp *exp = n->data;
				exp_label(v->sql->sa, exp, ++v->sql->label);
				info->repr = repr_replace(v, info->repr, o->data, exp);
				sql_exp *lexp = exp_ref(v->sql, exp);
				sql_exp *rexp = exp_ref(v->sql, m->data);
				sql_exp *cmp = exp_compare(v->sql->sa, lexp, rexp, cmp_equal);
				set_semantics(cmp);
				nrel->exps = sa_list_append(v->sql->sa, nrel->exps, cmp);
			}
		}
	} else if (is_join(rel->op) || is_semi(rel->op)) {
		prop *p = find_prop(rel->p, PROP_UNNESTING);
		struct access_info *ai = p?p->value.pval:NULL;
		if (ai && !list_empty(ai->accessed_by)) {
			list *nacc = list_dup(acc, NULL);
			(void)rel_djoin_elim( v, parent, rel, info, nacc, info->info->refs);
			return;
		}
		if (n)
			list_remove_node(acc, NULL, n);
		/* split accessing into accessingLeft and Right */
		list *accLeft = accessing(v, rel->l, acc);
		list *accRight = accessing(v, rel->r, acc);
		if (list_empty(accRight) && (rel->op != op_right && rel->op != op_full)) {
			unnest(v, rel, rel->l, info, accLeft);
			rel->exps = rewrite_columns(v, rel->exps, info);
			return;
		}
		if (list_empty(accLeft) && (rel->op != op_left && rel->op != op_full && rel->op != op_anti && rel->op != op_semi)) {
			unnest(v, rel, rel->r, info, accRight);
			rel->exps = rewrite_columns(v, rel->exps, info);
			return;
		}
		struct unnesting lunnesting = { .info = info->info };
		struct unnesting runnesting = { .info = info->info, .right=true };
		unnest(v, rel, rel->l, &lunnesting, accLeft);
		unnest(v, rel, rel->r, &runnesting, accRight);
		rel->exps = rewrite_columns_for_join(v, rel->exps, &lunnesting, &runnesting);
		add_outer_join_exps(v, rel, &lunnesting, &runnesting);
		/* merge cclases and repr */
		unnesting_merge(info, &lunnesting, &runnesting);
	} else if (is_base(rel->op) && rel->l && rel->flag == TABLE_FROM_RELATION) {
		unnest(v, rel, rel->l, info, acc);
		sql_exp *tfe = rel->r;
		list *ops = tfe->l;
		ops = add_outers(v, ops, info, false);
		tfe->l = ops = rewrite_columns(v, ops, info);
		rel->exps = add_outers(v, rel->exps, info, false);
		rel->nr_outers = list_length(info->info->outer_refs);
		rel->exps = rewrite_columns(v, rel->exps, info);
	}
}

static list *
accessing_merge(list *acc, list *nacc)
{
	if (!list_empty(nacc)) {
		for (node *n = nacc->h; n; n = n->next) {
			sql_rel *r = n->data;
			if (!list_find(acc, r, NULL))
				append(acc, r);
		}
	}
	return acc;
}

static sql_rel *
rel_smallest(sql_rel *r, list *outer_refs)
{
	if (r->op != op_join /*|| !list_empty(r->exps)*/)
		return r;
	sql_rel *rl = r->l;
	sql_rel *rr = r->r;
	int llen = list_length(outer_refs), rlen = llen;
	for(node *n = outer_refs->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (rel_find_exp(rl, e)) {
			llen--;
		} else {
			rlen--;
		}
	}
	if (llen && rlen)
		return r;
	if (llen)
		return rel_smallest(rr, outer_refs);
	return rel_smallest(rl, outer_refs);
}

static bool
rel_djoin_elim(visitor *v, sql_rel *prel, sql_rel *rel, struct unnesting *parent, list *parent_accessing, list *refs)
{
	sql_rel *changed_maps = NULL;
	if (rel_simple_djoin_elim(v, prel, rel, &changed_maps)) {
		if (parent) {
			if (changed_maps)
				unnest(v, prel, changed_maps, parent, parent_accessing);
			else
				unnest(v, prel, rel, parent, parent_accessing);
		}
		node *n = list_find(refs, rel, NULL);
		if (!n && rel_is_ref(prel))
			n = list_find(refs, prel, NULL);
		assert(n);
		list_remove_node(refs, NULL, n->next);
		list_remove_node(refs, NULL, n);
		return true;
	}
	if (parent) {
		assert(parent_accessing && !list_empty(parent_accessing));
		list *acc_left = accessing(v, rel->l, parent_accessing);
		unnest(v, rel, rel->l, parent, acc_left);
		rel->exps = rewrite_columns(v, rel->exps, parent);
	}
	assert(is_dependent(rel));
	prop *p = find_prop(rel->p, PROP_UNNESTING);
	assert(p);
	struct access_info *ai = p?p->value.pval:NULL;
	assert(ai && ai->outer_refs);
	sql_rel *l = rel_smallest(rel->l, ai->outer_refs);
	sql_rel *d = rel_project(v->sql->sa, rel_dup(l), exps_copy(v->sql, ai->outer_refs));

	list *outer_refs = ai->outer_refs;
	if (parent) {
		sql_exp *e = parent->info->outer_refs->h->data;
		outer_refs = list_dup(outer_refs, NULL);
		if (!refs_find(outer_refs, e)) {
			d = rel_crossproduct(v->sql->sa, d, rel_dup(parent->info->d), op_join);
			outer_refs = list_merge(outer_refs, parent->info->outer_refs, NULL);
			d = rel_project(v->sql->sa, rel_dup(d), rel_projections(v->sql, d, NULL, 0, 1));
		}
	}
	if (!exps_unique(v->sql, d->l, d->exps, true))
		rel_distinct(d);

	struct unnesting_info info = { .join = rel, .outer_refs = outer_refs, .d = d, .refs = refs };
	struct unnesting unnesting = { .info = &info, .parent = parent };
	list *acc = ai?ai->accessed_by:NULL;

	if (parent) { /* add all parent_accessing too acc */
		acc = list_dup(acc, NULL);
		list *acc_left = accessing(v, rel->r, parent_accessing);
		acc = accessing_merge(acc, acc_left);
	}
	unnest(v, rel, rel->r, &unnesting, acc);
	add_outer_join_exps(v, rel, parent, &unnesting);
	reset_dependent(rel);
	node *n = list_find(refs, rel, NULL);
	if (!n && rel_is_ref(prel))
		n = list_find(refs, prel, NULL);
	assert(n);
	list_remove_node(refs, NULL, n->next);
	list_remove_node(refs, NULL, n);
	rel_destroy(v->sql, info.d);
	if (changed_maps && parent) {
		sql_rel *p = changed_maps;
		while(p && p->op == op_project) {
			p->exps = add_outers(v, p->exps, parent, false);
			p->nr_outers = list_length(parent->info->outer_refs);
			p->exps = rewrite_columns(v, p->exps, parent);
			assert(!p->r);
			if (p->l != rel)
				p = p->l;
			else
				p = NULL;
		}
	}
	return true;
}

static inline sql_rel *
run_exp_rewriter(visitor *v, sql_rel *rel, exp_rewrite_fptr rewriter, bool direction, const char *name, bool continue_on_changes)
{
	(void)name;
	v->changes = 0;
	/*
#ifndef NDEBUG
	int changes = v->changes;
	lng clk = GDKusec();
	rel = rel_exp_visitor_bottomup(v, rel, rewriter, direction);
	printf("%s %d %d %d " LLFMT "\n", name, (int)v->sql->sa->usedmem, (int)v->sql->sa->inuse, (v->changes - changes), (GDKusec() - clk));
	return rel;
#else
*/
	rel = rel_exp_visitor_bottomup(v, rel, rewriter, direction);
	if (continue_on_changes) {
		v->changes = 0;
		rel = rel_exp_visitor_bottomup(v, rel, rewriter, direction);
	}
//#endif
	return rel;
}

static inline sql_rel *
run_rel_rewriter(visitor *v, sql_rel *rel, rel_rewrite_fptr rewriter, const char *name)
{
	(void)name;
	v->changes = 0;
	/*
#ifndef NDEBUG
	int changes = v->changes;
	lng clk = GDKusec();
	rel = rel_visitor_bottomup(v, rel, rewriter);
	printf("%s %d " LLFMT "\n", name, (v->changes - changes), (GDKusec() - clk));
	return rel;
#else
*/
	return rel_visitor_bottomup(v, rel, rewriter);
//#endif
}


sql_rel *
rel_unnest(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql };

	rel = run_exp_rewriter(&v, rel, &rel_simplify_exp_and_rank, false, "simplify_exp_and_rank", false);
	rel = run_rel_rewriter(&v, rel, &rel_unnest_simplify, "unnest_simplify");
	rel = run_exp_rewriter(&v, rel, &rewrite_complex, true, "rewrite_complex", false);
	rel = run_exp_rewriter(&v, rel, &rewrite_ifthenelse, false, "rewrite_ifthenelse", false); /* add isnull handling */
	rel = run_exp_rewriter(&v, rel, &rewrite_exp_rel, true, "rewrite_exp_rel", false);

	rel = run_rel_rewriter(&v, rel, &rel_unnest_comparison_rewriters, "unnest_comparison_rewriters");
	rel = run_rel_rewriter(&v, rel, &rel_unnest_simplify, "unnest_simplify");
	v.data = sa_list(sql->sa);
	v.opt++;
	rel = run_rel_rewriter(&v, rel, &rel_collect_non_trivial_dependent_joins, "collect_non_trivial_dependent_joins");
	list *refs = v.data;
	sql_rel *op = NULL;
	while (list_length(refs) > 0) {
		node *n = refs->h;
		sql_rel *r = n->data;
		sql_rel *p = n->next->data;

		if (!rel_is_ref(r) && p->l != r && p->r != r && (!is_munion(p->op) || !list_find(p->l, r, NULL))) {
			/* lower dependend join moved down, ie p isn't direct parent anymore */
			if (!p->l && op)
				p = op;
			sql_rel *l = p->l;
			if (!is_munion(p->op)) {
				if (l->l == r)
					p = l;
				if (l->r == r)
					p = l;
			}
			assert(p->l == r || p->r == r || (is_munion(p->op) && list_find(p->l, r, NULL)));
		}
		if (rel_is_ref(r)) {
			r = rel_inplace_project(v.sql->sa, r, NULL, rel_projections(v.sql, r, NULL, 0, 1));
			p = r;
			r = r->l;
		}
		op = r;
		(void)rel_djoin_elim(&v, p, r, NULL, NULL, refs );
	}
	v.data = NULL;

	rel = run_rel_rewriter(&v, rel, &rewrite_fix_count, "fix_count");	/* fix count inside a left join (adds a project (if (cnt IS null) then (0) else (cnt)) */
	rel = run_rel_rewriter(&v, rel, &rel_unnest_projects, "unnest_projects");
	rel = run_exp_rewriter(&v, rel, &exp_reset_card_and_freevar_set_physical_type, false, "exp_reset_card_and_freevar_set_physical_type", false);
	rel = run_exp_rewriter(&v, rel, &exp_set_type, false, "exp_set_type", true);
	rel = rel_visitor_topdown(&v, rel, &rel_set_type);
	return rel;
}
