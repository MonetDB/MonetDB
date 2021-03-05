/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_rewriter.h"
#include "rel_prop.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "mal_errors.h" /* for SQLSTATE() */

/* simplify expressions, such as not(not(x)) */
/* exp visitor */

#define is_not_anyequal(sf) (strcmp((sf)->func->base.name, "sql_not_anyequal") == 0)

static list *
exps_simplify_exp(visitor *v, list *exps)
{
	if (list_empty(exps))
		return exps;

	int needed = 0;
	for (node *n=exps->h; n && !needed; n = n->next) {
		sql_exp *e = n->data;

		needed = (exp_is_true(e) || exp_is_false(e) || (is_compare(e->type) && e->flag == cmp_or));
	}
	/* if there's only one expression and it is false, we have to keep it */
	if (list_length(exps) == 1 && exp_is_false(exps->h->data))
		return exps;
	if (needed) {
		list *nexps = sa_list(v->sql->sa);
		for (node *n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			/* TRUE or X -> TRUE
		 	* FALSE or X -> X */
			if (is_compare(e->type) && e->flag == cmp_or) {
				list *l = e->l = exps_simplify_exp(v, e->l);
				list *r = e->r = exps_simplify_exp(v, e->r);

				if (list_length(l) == 1) {
					sql_exp *ie = l->h->data;

					if (exp_is_true(ie)) {
						v->changes++;
						continue;
					} else if (exp_is_false(ie)) {
						v->changes++;
						nexps = list_merge(nexps, r, (fdup)NULL);
						continue;
					}
				} else if (list_length(l) == 0) { /* left is true */
					v->changes++;
					continue;
				}
				if (list_length(r) == 1) {
					sql_exp *ie = r->h->data;

					if (exp_is_true(ie)) {
						v->changes++;
						continue;
					} else if (exp_is_false(ie)) {
						nexps = list_merge(nexps, l, (fdup)NULL);
						v->changes++;
						continue;
					}
				} else if (list_length(r) == 0) { /* right is true */
					v->changes++;
					continue;
				}
			}
			/* TRUE and X -> X */
			if (exp_is_true(e)) {
				v->changes++;
				continue;
			/* FALSE and X -> FALSE */
			} else if (exp_is_false(e)) {
				v->changes++;
				return append(sa_list(v->sql->sa), e);
			} else {
				append(nexps, e);
			}
		}
		return nexps;
	}
	return exps;
}

sql_exp *
rewrite_simplify_exp(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	if (!e)
		return e;

	v->changes = 0;
	(void)rel; (void)depth;

	sql_subfunc *sf = e->f;
	if (is_func(e->type) && list_length(e->l) == 1 && is_not_func(sf)) {
		list *args = e->l;
		sql_exp *ie = args->h->data;

		if (!ie)
			return e;

		sql_subfunc *sf = ie->f;
		if (is_func(ie->type) && list_length(ie->l) == 1 && is_not_func(sf)) {
			args = ie->l;

			ie = args->h->data;
			if (exp_name(e))
				exp_prop_alias(v->sql->sa, ie, e);
			v->changes++;
			return ie;
		}
		if (is_func(ie->type) && list_length(ie->l) == 2 && is_not_anyequal(sf)) {
			args = ie->l;

			sql_exp *l = args->h->data;
			sql_exp *vals = args->h->next->data;

			ie = exp_in_func(v->sql, l, vals, 1, 0);
			if (exp_name(e))
				exp_prop_alias(v->sql->sa, ie, e);
			v->changes++;
			return ie;
		}
		/* TRUE or X -> TRUE
		 * FALSE or X -> X */
		if (is_compare(e->type) && e->flag == cmp_or) {
			list *l = e->l = exps_simplify_exp(v, e->l);
			list *r = e->r = exps_simplify_exp(v, e->r);

			if (list_length(l) == 1) {
				sql_exp *ie = l->h->data;

				if (exp_is_true(ie)) {
					v->changes++;
					return ie;
				} else if (exp_is_false(ie) && list_length(r) == 1) {
					v->changes++;
					return r->h->data;
				}
			} else if (list_length(l) == 0) { /* left is true */
				v->changes++;
				return exp_atom_bool(v->sql->sa, 1);
			}
			if (list_length(r) == 1) {
				sql_exp *ie = r->h->data;

				if (exp_is_true(ie)) {
					v->changes++;
					return ie;
				} else if (exp_is_false(ie) && list_length(l) == 1) {
					v->changes++;
					return l->h->data;
				}
			} else if (list_length(r) == 0) { /* right is true */
				v->changes++;
				return exp_atom_bool(v->sql->sa, 1);
			}
		}
	}
	return e;
}

sql_rel *
rewrite_simplify(visitor *v, sql_rel *rel)
{
	if (!rel)
		return rel;

	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps)) {
		rel->exps = exps_simplify_exp(v, rel->exps);
		/* At a select or inner join relation if the single expression is false, eliminate the inner relations with a dummy projection */
		if (v->value_based_opt && list_length(rel->exps) == 1 && exp_is_false(rel->exps->h->data)) {
			if ((is_select(rel->op) || (is_innerjoin(rel->op) && !rel_is_ref(rel->r))) && rel->card > CARD_ATOM && !rel_is_ref(rel->l)) {
				list *nexps = sa_list(v->sql->sa), *toconvert = rel_projections(v->sql, rel->l, NULL, 1, 1);
				if (is_innerjoin(rel->op))
					toconvert = list_merge(toconvert, rel_projections(v->sql, rel->r, NULL, 1, 1), NULL);

				for (node *n = toconvert->h ; n ; n = n->next) {
					sql_exp *e = n->data, *a = exp_atom(v->sql->sa, atom_general(v->sql->sa, exp_subtype(e), NULL));
					exp_prop_alias(v->sql->sa, a, e);
					list_append(nexps, a);
				}
				rel_destroy(rel->l);
				if (is_innerjoin(rel->op)) {
					rel_destroy(rel->r);
					rel->r = NULL;
					rel->op = op_select;
				}
				rel->l = rel_project(v->sql->sa, NULL, nexps);
				rel->card = CARD_ATOM;
				v->changes++;
			}
		}
	}
	return rel;
}

sql_rel *
rel_remove_empty_select(visitor *v, sql_rel *rel)
{
	if ((is_join(rel->op) || is_semi(rel->op) || is_select(rel->op) || is_project(rel->op) || is_topn(rel->op) || is_sample(rel->op)) && rel->l)
		rel->l = try_remove_empty_select(v, rel->l);
	if ((is_join(rel->op) || is_semi(rel->op) || is_set(rel->op)) && rel->r)
		rel->r = try_remove_empty_select(v, rel->r);
	if (is_join(rel->op) && list_empty(rel->exps))
		rel->exps = NULL; /* crossproduct */
	return rel;
}

/* push the expression down, ie translate colum references
	from relation f into expression of relation t
*/

static sql_exp * _exp_push_down(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t);

static list *
exps_push_down(mvc *sql, list *exps, sql_rel *f, sql_rel *t)
{
	if (list_empty(exps))
		return exps;
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *arg = n->data, *narg = NULL;

		narg = _exp_push_down(sql, arg, f, t);
		if (!narg)
			return NULL;
		narg = exp_propagate(sql->sa, narg, arg);
		n->data = narg;
	}
	return exps;
}

static sql_exp *
_exp_push_down(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t)
{
	sql_exp *oe = e;
	sql_exp *ne = NULL, *l, *r, *r2;

	switch(e->type) {
	case e_column:
		if (e->l) {
			ne = rel_bind_column2(sql, f, e->l, e->r, 0);
			/* if relation name matches expressions relation name, find column based on column name alone */
		}
		if (!ne && !e->l)
			ne = rel_bind_column(sql, f, e->r, 0, 1);
		if (!ne || ne->type != e_column)
			return NULL;
		e = NULL;
		if (ne->l && ne->r)
			e = rel_bind_column2(sql, t, ne->l, ne->r, 0);
		if (!e && ne->r && !ne->l)
			e = rel_bind_column(sql, t, ne->r, 0, 1);
		sql->session->status = 0;
		sql->errstr[0] = 0;
		if (e && oe)
			e = exp_propagate(sql->sa, e, oe);
		/* if the upper exp was an alias, keep this */
		if (e && exp_relname(ne))
			exp_prop_alias(sql->sa, e, ne);
		return e;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			list *l, *r;

			l = exps_push_down(sql, e->l, f, t);
			if (!l)
				return NULL;
			r = exps_push_down(sql, e->r, f, t);
			if (!r)
				return NULL;
			if (e->flag == cmp_filter)
				return exp_filter(sql->sa, l, r, e->f, is_anti(e));
			return exp_or(sql->sa, l, r, is_anti(e));
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			list *r;

			l = _exp_push_down(sql, e->l, f, t);
			if (!l)
				return NULL;
			r = exps_push_down(sql, e->r, f, t);
			if (!r)
				return NULL;
			return exp_in(sql->sa, l, r, e->flag);
		} else {
			l = _exp_push_down(sql, e->l, f, t);
			if (!l)
				return NULL;
			r = _exp_push_down(sql, e->r, f, t);
			if (!r)
				return NULL;
			if (e->f) {
				r2 = _exp_push_down(sql, e->f, f, t);
				if (l && r && r2)
					ne = exp_compare2(sql->sa, l, r, r2, e->flag);
			} else if (l && r) {
				if (l->card < r->card)
					ne = exp_compare(sql->sa, r, l, swap_compare((comp_type)e->flag));
				else
					ne = exp_compare(sql->sa, l, r, e->flag);
			}
		}
		if (!ne)
			return NULL;
		return exp_propagate(sql->sa, ne, e);
	case e_convert:
		l = _exp_push_down(sql, e->l, f, t);
		if (l)
			return exp_convert(sql->sa, l, exp_fromtype(e), exp_totype(e));
		return NULL;
	case e_aggr:
	case e_func: {
		list *l = e->l, *nl = NULL;

		if (!l) {
			return e;
		} else {
			nl = exps_push_down(sql, l, f, t);
			if (!nl)
				return NULL;
		}
		if (e->type == e_func)
			return exp_op(sql->sa, nl, e->f);
		else
			return exp_aggr(sql->sa, nl, e->f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
	}
	case e_atom:
	case e_psm:
		return e;
	}
	return NULL;
}

sql_exp *
exp_push_down(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t)
{
	return _exp_push_down(sql, e, f, t);
}

sql_rel *
rewrite_reset_used(visitor *v, sql_rel *rel)
{
	(void) v;
	rel->used = 0;
	return rel;
}
